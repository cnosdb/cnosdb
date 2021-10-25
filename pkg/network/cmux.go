package network

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/soheilhy/cmux"
)

type comparer struct {
	bytes []byte
	bl    int
}

func (c *comparer) matchPrefix(r io.Reader) bool {
	buf := make([]byte, c.bl)
	n, _ := io.ReadFull(r, buf)
	return bytes.Equal(c.bytes, buf[:n])
}

func ByteMatcher(b byte) cmux.Matcher {
	c := &comparer{
		bytes: []byte{b},
		bl:    1,
	}
	return c.matchPrefix
}

func StringMatcher(str string) cmux.Matcher {
	b := []byte(str)
	c := &comparer{
		bytes: []byte(str),
		bl:    len(b),
	}
	return c.matchPrefix
}

func ListenByte(mux cmux.CMux, header byte) net.Listener {
	h := string(header)
	ln := &Listener{
		header: h,
		ln:     mux.Match(ByteMatcher(header)),
	}
	return ln
}

func ListenString(mux cmux.CMux, header string) net.Listener {
	ln := &Listener{
		header: header,
		ln:     mux.Match(StringMatcher(header)),
	}
	return ln
}

// Listener 用于处理 Mux 中建立的网络连接的监听器
type Listener struct {
	header string
	ln     net.Listener

	mu sync.RWMutex
}

// Accept 等待并返回下一个连接到该监听器的连接
func (ln *Listener) Accept() (net.Conn, error) {
	ln.mu.RLock()
	defer ln.mu.RUnlock()

	conn, err := ln.ln.Accept()
	if err != nil {
		return nil, err
	}
	h := []byte(ln.header)
	if _, err = io.ReadFull(conn, h[:]); err != nil {
		return nil, errors.New(fmt.Sprintf("mux.Listener: cannot read header: %s", err))
	}

	return conn, nil
}

// Close 将监听器从所属 mux 中移除并关闭内部通道，任何阻塞的 Accept 操作都将不再阻塞并返回错误。
func (ln *Listener) Close() error {
	return ln.ln.Close()
}

// Addr 返回所属 mux 中被复用的监听器的网络地址
func (ln *Listener) Addr() net.Addr {
	ln.mu.RLock()
	defer ln.mu.RUnlock()

	if ln.ln == nil {
		return nil
	}

	return ln.ln.Addr()
}

func Dial(network, address string, header string) (net.Conn, error) {
	conn, err := net.Dial(network, address)
	if err != nil {
		return nil, err
	}

	if _, err := conn.Write([]byte(header)); err != nil {
		return nil, fmt.Errorf("write mux header: %s", err)
	}
	return conn, nil
}

func DialTimeout(network, address string, header string, timeout time.Duration) (net.Conn, error) {
	conn, err := net.DialTimeout(network, address, timeout)
	if err != nil {
		return nil, err
	}

	if _, err := conn.Write([]byte(header)); err != nil {
		return nil, fmt.Errorf("write mux header: %s", err)
	}
	return conn, nil
}
