package events

import (
	"fmt"
	"sync"

	"github.com/sirupsen/logrus"
)

// Broadcaster sends events to multiple, reliable Sinks. The goal of this
// component is to dispatch events to configured endpoints. Reliability can be
// provided by wrapping incoming sinks.
type Broadcaster struct {

	// Sink accepts and sends events.
	sinks   []Sink

	// 事件管道，长度为 1
	events  chan Event

	// 新增 sink
	adds    chan configureRequest

	// 移除 sink
	removes chan configureRequest

	// 关闭控制
	shutdown chan struct{}
	closed   chan struct{}
	once     sync.Once
}


// NewBroadcaster appends one or more sinks to the list of sinks.
//
// The broadcaster behavior will be affected by the properties of the sink.
//
// Generally, the sink should accept all messages and deal with reliability on its own.
//
// Use of EventQueue and RetryingSink should be used here.
//
func NewBroadcaster(sinks ...Sink) *Broadcaster {

	b := Broadcaster{
		sinks:    sinks,
		events:   make(chan Event),
		adds:     make(chan configureRequest),
		removes:  make(chan configureRequest),
		shutdown: make(chan struct{}),
		closed:   make(chan struct{}),
	}


	// Start the broadcaster
	go b.run()


	return &b
}

// Write accepts an event to be dispatched to all sinks.
// This method will never fail and should never block (hopefully!).
// The caller cedes the memory to the broadcaster and should not modify it after calling write.
func (b *Broadcaster) Write(event Event) error {
	select {
	// 写管道
	case b.events <- event:
	case <-b.closed:
		return ErrSinkClosed
	}
	return nil
}


// Add the sink to the broadcaster.
//
// The provided sink must be comparable with equality.
// Typically, this just works with a regular pointer type.
func (b *Broadcaster) Add(sink Sink) error {
	return b.configure(b.adds, sink)
}

// Remove the provided sink.
func (b *Broadcaster) Remove(sink Sink) error {
	return b.configure(b.removes, sink)
}

// add/remove sink 的 request
type configureRequest struct {
	sink     Sink
	response chan error // 获取 config 的错误信息
}


// 参数 ch 可能是 b.adds 或者 b.removes
func (b *Broadcaster) configure(ch chan configureRequest, sink Sink) error {

	rspChan := make(chan error, 1)

	// 构造配置请求
	confReq := configureRequest{
		sink: sink,
		response: rspChan,
	}

	// 发送请求到 ch 管道中、等待响应
	for {
		select {
		case ch <- confReq:
			ch = nil
		case err := <-rspChan:
			return err
		case <-b.closed:
			return ErrSinkClosed
		}
	}
}

// Close the broadcaster, ensuring that all messages are flushed to the
// underlying sink before returning.
func (b *Broadcaster) Close() error {

	// 触发关闭
	b.once.Do(func() {
		close(b.shutdown)
	})

	// 等待关闭完成
	<-b.closed

	return nil
}

// run is the main broadcast loop, started when the broadcaster is created.
// Under normal conditions, it waits for events on the event channel. After
// Close is called, this goroutine will exit.
func (b *Broadcaster) run() {
	defer close(b.closed)

	// 从 b.sinks 中删除 target
	remove := func(target Sink) {
		// 遍历 b.sinks，从中查找 target 并删除它
		for i, sink := range b.sinks {
			if sink == target {
				b.sinks = append(b.sinks[:i], b.sinks[i+1:]...)
				break
			}
		}
	}

	for {
		select {
		// 监听 event
		case event := <-b.events:

			// 遍历 b.sinks
			for _, sink := range b.sinks {

				// 发送 event 到 sink
				if err := sink.Write(event); err != nil {

					// 如果 sink 被关闭了就从 b.sinks 中移除它
					if err == ErrSinkClosed {
						// remove closed sinks
						remove(sink)
						continue
					}

					logrus.WithField("event", event).WithField("events.sink", sink).WithError(err).Errorf("broadcaster: dropping event")
				}
			}

		// 新增 sink
		case request := <-b.adds:

			// while we have to iterate for add/remove,
			// common iteration for send is faster against slice.


			// 从 b.sinks 中查找 request.sink
			var found bool
			for _, sink := range b.sinks {
				if request.sink == sink {
					found = true
					break
				}
			}

			// 如果未找到，就插入进去
			if !found {
				b.sinks = append(b.sinks, request.sink)
			}

			// b.sinks[request.sink] = struct{}{}
			// ???
			request.response <- nil

		// 移除 sink
		case request := <-b.removes:
			remove(request.sink)
			request.response <- nil

		// 监听关闭信号
		case <-b.shutdown:

			// close all the underlying sinks

			// 逐个 sink 进行关闭
			for _, sink := range b.sinks {
				if err := sink.Close(); err != nil && err != ErrSinkClosed {
					logrus.WithField("events.sink", sink).WithError(err).Errorf("broadcaster: closing sink failed")
				}
			}

			return
		}
	}
}

func (b *Broadcaster) String() string {
	// Serialize copy of this broadcaster without the sync.Once, to avoid
	// a data race.

	b2 := map[string]interface{}{
		"sinks":   b.sinks,
		"events":  b.events,
		"adds":    b.adds,
		"removes": b.removes,

		"shutdown": b.shutdown,
		"closed":   b.closed,
	}

	return fmt.Sprint(b2)
}
