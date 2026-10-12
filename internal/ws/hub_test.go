package ws

import (
	"testing"
	"time"
)

func TestLimitAndSlowClientEviction(t *testing.T) {
	h := NewHub(1)
	a := NewClient(1)
	b := NewClient(1)
	if !h.Register("s", a) {
		t.Fatal("register")
	}
	if h.Register("s", b) {
		t.Fatal("limit not enforced")
	}
	h.Broadcast("s", 1)
	h.Broadcast("s", 2)
	select {
	case <-a.Done():
	case <-time.After(time.Second):
		t.Fatal("slow client not evicted")
	}
}
