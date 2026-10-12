package ws

import "sync"

type Client struct {
	Send chan any
	done chan struct{}
	once sync.Once
}

func NewClient(n int) *Client           { return &Client{Send: make(chan any, n), done: make(chan struct{})} }
func (c *Client) Close()                { c.once.Do(func() { close(c.done) }) }
func (c *Client) Done() <-chan struct{} { return c.done }

type Hub struct {
	mu  sync.RWMutex
	m   map[string]map[*Client]struct{}
	Max int
}

func NewHub(max int) *Hub { return &Hub{m: map[string]map[*Client]struct{}{}, Max: max} }
func (h *Hub) Register(id string, c *Client) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.m[id] == nil {
		h.m[id] = map[*Client]struct{}{}
	}
	if h.Max > 0 && len(h.m[id]) >= h.Max {
		return false
	}
	h.m[id][c] = struct{}{}
	return true
}
func (h *Hub) Unregister(id string, c *Client) {
	h.mu.Lock()
	defer h.mu.Unlock()
	delete(h.m[id], c)
	c.Close()
}
func (h *Hub) Broadcast(id string, v any) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for c := range h.m[id] {
		select {
		case c.Send <- v:
		default:
			delete(h.m[id], c)
			c.Close()
		}
	}
}
