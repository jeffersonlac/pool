package pool

import (
	"errors"
	"fmt"
	"net"
	"sync"
	"time"
)

// CloseConnReason describes why does the pool close a connection
type CloseConnReason string

const (
	ConnUnusable CloseConnReason = "unusable"
	ConnTimeout  CloseConnReason = "timeout"
	PoolFull     CloseConnReason = "poolFull"
	PoolClosed   CloseConnReason = "poolClosed"
)

// channelPool implements the Pool interface based on buffered channels.
type channelPool struct {
	// storage for our net.Conn connections
	mu    sync.RWMutex
	conns chan *connInfo
	// every connection's timeout
	timeout time.Duration

	callbacks Callbacks
}

type connInfo struct {
	conn       net.Conn
	createTime time.Time
}

// Callbacks contains all callbacks to the channel pool user
type Callbacks struct {
	// net.Conn generator
	Factory    Factory
	ConnClosed ConnClosedCallback
}

// Factory is a function to create new connections.
type Factory func() (net.Conn, error)

// ConnClosedCallback is a function to notify there is a connection has been closed.
type ConnClosedCallback func(remoteAddr string, reason CloseConnReason, err error)

// NewChannelPool returns a new pool based on buffered channels with an initial
// capacity and maximum capacity. Factory is used when initial capacity is
// greater than zero to fill the pool. A zero initialCap doesn't fill the Pool
// until a new Get() is called. During a Get(), If there is no new connection
// available in the pool, a new connection will be created via the Factory()
// method.
func NewChannelPool(initialCap, maxCap int, callbacks Callbacks, timeout time.Duration) (Pool, error) {
	if initialCap < 0 || maxCap <= 0 || initialCap > maxCap {
		return nil, errors.New("invalid capacity settings")
	}

	c := &channelPool{
		conns:     make(chan *connInfo, maxCap),
		callbacks: callbacks,
		timeout:   timeout,
	}

	// create initial connections, if something goes wrong,
	// just close the pool error out.
	for i := 0; i < initialCap; i++ {
		conn, err := callbacks.Factory()
		if err != nil {
			c.Close()
			return nil, fmt.Errorf("factory is not able to fill the pool: %s", err)
		}
		c.conns <- &connInfo{conn: conn, createTime: time.Now()}
	}

	return c, nil
}

func (c *channelPool) getConnsAndCallbacks() (chan *connInfo, Callbacks) {
	c.mu.RLock()
	conns := c.conns
	callbacks := c.callbacks
	c.mu.RUnlock()
	return conns, callbacks
}

// Get implements the Pool interfaces Get() method. If there is no new
// connection available in the pool, a new connection will be created via the
// Factory() method.
func (c *channelPool) Get() (net.Conn, error) {
	conns, callbacks := c.getConnsAndCallbacks()
	if conns == nil {
		return nil, ErrClosed
	}

	// wrap our connections with out custom net.Conn implementation (wrapConn
	// method) that puts the connection back to the pool if it's closed.
	for {
		select {
		case connInfo := <-conns:
			if connInfo == nil {
				return nil, ErrClosed
			}

			if timeout := c.timeout; timeout > 0 {
				if connInfo.createTime.Add(timeout).Before(time.Now()) {
					if connInfo.conn != nil {
						c.closeConn(connInfo.conn, ConnTimeout, callbacks.ConnClosed)

					}
					continue
				}
			}

			return c.wrapConn(connInfo.conn), nil
		default:
			conn, err := callbacks.Factory()
			if err != nil {
				return nil, err
			}

			return c.wrapConn(conn), nil
		}
	}
}

// put puts the connection back to the pool. If the pool is full or closed,
// conn is simply closed. A nil conn will be rejected.
func (c *channelPool) put(conn net.Conn) error {
	if conn == nil {
		return errors.New("connection is nil. rejecting")
	}

	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.conns == nil {
		// pool is closed, close passed connection
		return c.closeConn(conn, PoolClosed, c.callbacks.ConnClosed)
	}

	// put the resource back into the pool. If the pool is full, this will
	// block and the default case will be executed.
	select {
	case c.conns <- &connInfo{conn: conn, createTime: time.Now()}:
		return nil
	default:
		// pool is full, close passed connection
		return c.closeConn(conn, PoolFull, c.callbacks.ConnClosed)
	}
}

func (c *channelPool) Close() {
	c.mu.Lock()
	conns := c.conns
	c.conns = nil
	connClosedCallback := c.callbacks.ConnClosed
	c.callbacks = Callbacks{}
	c.mu.Unlock()

	if conns == nil {
		return
	}

	close(conns)
	for connInfo := range conns {
		c.closeConn(connInfo.conn, PoolClosed, connClosedCallback)
	}
}

func (c *channelPool) Len() int {
	conns, _ := c.getConnsAndCallbacks()
	return len(conns)
}

func (c *channelPool) closeConn(conn net.Conn, reason CloseConnReason, cb ConnClosedCallback) error {
	var remoteAddr string
	if conn.RemoteAddr() != nil {
		remoteAddr = conn.RemoteAddr().String()
	}
	err := conn.Close()
	if cb != nil {
		cb(remoteAddr, reason, err)
	}
	return err
}
