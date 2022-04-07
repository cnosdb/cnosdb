package tcp

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

const (
	// DefaultTimeout Default Header timeout
	DefaultTimeout = 30 * time.Second
)

// Mux Reusable connections
type Mux struct {
	mu sync.RWMutex
	ln net.Listener
	m  map[byte]*listener

	defaultListener *listener

	wg sync.WaitGroup

	// Receive Header timeout
	Timeout time.Duration

	// Out-of-band error logger
	Logger *log.Logger
}

type replayConn struct {
	net.Conn
	firstByte     byte
	readFirstByte bool
}

func (rc *replayConn) Read(b []byte) (int, error) {
	if rc.readFirstByte {
		return rc.Conn.Read(b)
	}

	if len(b) == 0 {
		return 0, nil
	}

	b[0] = rc.firstByte
	rc.readFirstByte = true
	return 1, nil
}

// NewMux 返回可复用的网络连接对象 Mux 的实例
func NewMux() *Mux {
	return &Mux{
		m:       make(map[byte]*listener),
		Timeout: DefaultTimeout,
		Logger:  log.New(os.Stderr, "[tcp] ", log.LstdFlags),
	}
}

// Serve 开始维护通过 ln 建立的连接。
// ln 可通过注册基于 Header 字节 的监听器实现复用
func (mux *Mux) Serve(ln net.Listener) error {
	mux.mu.Lock()
	mux.ln = ln
	mux.mu.Unlock()
	for {
		// 等待建立新的连接
		// 若错误类型为 temporary ,继续等待连接建立
		// 对于其他错误，则立即关闭并退出 Mux
		conn, err := ln.Accept()
		if err, ok := err.(interface {
			Temporary() bool
		}); ok && err.Temporary() {
			continue
		}
		if err != nil {
			// 等待
			mux.wg.Wait()

			// 同步关闭所有已注册的 listener
			// mux.m 的键类型为 byte ，所以最坏情况会产生 256 个 goroutine
			var wg sync.WaitGroup
			mux.mu.RLock()
			for _, ln := range mux.m {
				wg.Add(1)
				go func(ln *listener) {
					defer wg.Done()
					ln.Close()
				}(ln)
			}
			mux.mu.RUnlock()
			wg.Wait()

			mux.mu.RLock()
			dl := mux.defaultListener
			mux.mu.RUnlock()
			if dl != nil {
				dl.Close()
			}

			return err
		}

		// 启动新 goroutine 进行解复用
		mux.wg.Add(1)
		go mux.handleConn(conn)
	}
}

func (mux *Mux) handleConn(conn net.Conn) {
	defer mux.wg.Done()
	// Set a read deadline so connections with no data don't timeout.
	// 启动 conn 的读取超时配置
	if err := conn.SetReadDeadline(time.Now().Add(mux.Timeout)); err != nil {
		conn.Close()
		mux.Logger.Printf("tcp.Mux: cannot set read deadline: %s", err)
		return
	}

	// 读取初始字节 Header ，该字节用于决定该 conn 的监听器
	var typ [1]byte
	if _, err := io.ReadFull(conn, typ[:]); err != nil {
		conn.Close()
		mux.Logger.Printf("tcp.Mux: cannot read header byte: %s", err)
		return
	}

	// 取消 conn 的读取超时配置
	if err := conn.SetReadDeadline(time.Time{}); err != nil {
		conn.Close()
		mux.Logger.Printf("tcp.Mux: cannot reset set read deadline: %s", err)
		return
	}

	// 根据 Header 分配 conn 的监听器
	mux.mu.RLock()
	handler := mux.m[typ[0]]
	mux.mu.RUnlock()

	if handler == nil {
		if mux.defaultListener == nil {
			conn.Close()
			mux.Logger.Printf("tcp.Mux: handler not registered: %d. Connection from %s closed", typ[0], conn.RemoteAddr())
			return
		}

		conn = &replayConn{
			Conn:      conn,
			firstByte: typ[0],
		}
		handler = mux.defaultListener
	}

	handler.HandleConn(conn, typ[0])
}

// Listen 在 mux 中，创建唯一基于 Header 字节的监听器实例
func (mux *Mux) Listen(header byte) net.Listener {
	mux.mu.Lock()
	defer mux.mu.Unlock()

	if _, ok := mux.m[header]; ok {
		panic(fmt.Sprintf("listener already registered under header byte: %d", header))
	}

	ln := &listener{
		header: header,
		c:      make(chan net.Conn),
		done:   make(chan struct{}),
		mux:    mux,
	}
	mux.m[header] = ln
	return ln
}

// release 从 mux.m 中移除监听器
func (mux *Mux) release(ln *listener) bool {
	mux.mu.Lock()
	defer mux.mu.Unlock()

	for b, l := range mux.m {
		if l == ln {
			delete(mux.m, b)
			return true
		}
	}
	return false
}

// DefaultListener 创建默认的 net.Listener 监听器实例，用于处理未注册 Header 字节的连接。
// 读取数据时，之前已被读取的 Header 字节将会被重新读取。
//
// 注册该默认监听器实例时，需要确保请求报文的起始字节与其他已注册 Header 字节的监听器之间没有冲突，
// 如 GET 请求中的 71 ('G')
func (mux *Mux) DefaultListener() net.Listener {
	mux.mu.Lock()
	defer mux.mu.Unlock()
	if mux.defaultListener == nil {
		mux.defaultListener = &listener{
			c:    make(chan net.Conn),
			done: make(chan struct{}),
			mux:  mux,
		}
	}

	return mux.defaultListener
}

// listener 用于处理 Mux 中建立的网络连接的监听器
type listener struct {
	mux    *Mux
	header byte

	// 当关闭 listener 时，通道 done 将在持有 mu 的互斥锁并关闭 通道 c 之前被关闭，此时其他所有
	// 持有 mu 的读写锁的线程应能够收到信号，释放 mu 并退出
	done chan struct{}

	mu sync.RWMutex
	c  chan net.Conn
}

// Accept 等待并返回下一个连接到该监听器的连接
func (ln *listener) Accept() (net.Conn, error) {
	ln.mu.RLock()
	defer ln.mu.RUnlock()

	select {
	case <-ln.done:
		return nil, errors.New("network connection closed")
	case conn := <-ln.c:
		return conn, nil
	}
}

// Close 将监听器从所属 mux 中移除并关闭内部通道，任何阻塞的 Accept 操作都将不再阻塞并返回错误。
func (ln *listener) Close() error {
	if ok := ln.mux.release(ln); ok {
		// 关闭 ln.done ，使读锁的持有者释放锁
		close(ln.done)

		// 在持有互斥锁的情况下，将 ln.c 置为 nil
		// 以阻塞接下来的发送和接收动作
		ln.mu.Lock()
		ln.c = nil
		ln.mu.Unlock()
	}
	return nil
}

// HandleConn 处理 conn ，到 listener 被关闭为止
func (ln *listener) HandleConn(conn net.Conn, handlerID byte) {
	ln.mu.RLock()
	defer ln.mu.RUnlock()

	timer := time.NewTimer(ln.mux.Timeout)
	defer timer.Stop()

	// 将 conn 发送至申请者。申请者将负责关闭该 conn
	select {
	case <-ln.done:
		// Receive will return immediately if ln.Close has been called.
		// ln 被关闭时，所有基于 ln 的接收过程将立刻退出
		conn.Close()
	case ln.c <- conn:
		// ln 关闭时， ln.c 被置为 nil ，此时 ln.c 上所有 I/O 被阻塞
	case <-timer.C:
		conn.Close()
		ln.mux.Logger.Printf("tcp.Mux: handler not ready: %d. Connection from %s closed", handlerID, conn.RemoteAddr())
		return
	}
}

// Addr 返回所属 mux 中被复用的监听器的网络地址
func (ln *listener) Addr() net.Addr {
	if ln.mux == nil {
		return nil
	}

	ln.mux.mu.RLock()
	defer ln.mux.mu.RUnlock()

	if ln.mux.ln == nil {
		return nil
	}

	return ln.mux.ln.Addr()
}

// Dial 基于初始的 Header 字节与远程的 mux 监听器建立连接
func Dial(network, address string, header byte) (net.Conn, error) {
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}

	if _, err := conn.Write([]byte{header}); err != nil {
		return nil, fmt.Errorf("write mux header: %s", err)
	}
	return conn, nil
}
