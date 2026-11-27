package urltest

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	M "github.com/sagernet/sing/common/metadata"
)

type directDialer struct {
	dialer net.Dialer
}

func (d *directDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return d.dialer.DialContext(ctx, network, destination.String())
}

func (d *directDialer) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	var listenConfig net.ListenConfig
	return listenConfig.ListenPacket(ctx, "udp", destination.String())
}

func TestMedianDelay(t *testing.T) {
	cases := []struct {
		input    []uint16
		expected uint16
	}{
		{nil, 0},
		{[]uint16{100}, 100},
		{[]uint16{300, 100, 200}, 200},
		{[]uint16{100, 200, 300, 400}, 250},
		{[]uint16{5008, 170, 180}, 180},
	}
	for _, c := range cases {
		if got := MedianDelay(c.input); got != c.expected {
			t.Errorf("MedianDelay(%v) = %d, want %d", c.input, got, c.expected)
		}
	}
}

func TestURLTestMultiReusesConnection(t *testing.T) {
	var requests atomic.Int32
	var newConns atomic.Int32
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	server.Config.ConnState = func(conn net.Conn, state http.ConnState) {
		if state == http.StateNew {
			newConns.Add(1)
		}
	}
	server.Start()
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	delay, err := URLTestMulti(ctx, server.URL, &directDialer{}, 3, true)
	if err != nil {
		t.Fatalf("URLTestMulti: %v", err)
	}
	// 1 warm-up + 3 measured samples.
	if got := requests.Load(); got != 4 {
		t.Errorf("server saw %d requests, want 4", got)
	}
	// All 4 requests must share the single pooled connection established
	// by the warm-up; a fresh connection per request is exactly the cold
	// behavior this function exists to avoid.
	if got := newConns.Load(); got != 1 {
		t.Errorf("server saw %d new connections, want 1 (connection reuse)", got)
	}
	t.Logf("median delay: %d ms", delay)
}

type flakyDialer struct {
	directDialer
	dials  atomic.Int32
	failOn map[int32]bool
}

func (d *flakyDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	n := d.dials.Add(1)
	if d.failOn[n] {
		return nil, errors.New("simulated dial failure")
	}
	return d.directDialer.DialContext(ctx, network, destination)
}

func TestURLTestMultiPartialFailure(t *testing.T) {
	// Connection: close on every response forces each sample to dial, so
	// dial failures map 1:1 to sample failures.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Connection", "close")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// 1 of 3 samples succeeds: below the majority threshold, must error.
	_, err := URLTestMulti(ctx, server.URL, &flakyDialer{failOn: map[int32]bool{2: true, 3: true}}, 3, false)
	if err == nil {
		t.Fatal("expected error when only 1 of 3 samples succeeds")
	}
	// 2 of 3 samples succeed: median of the successes is reported.
	delay, err := URLTestMulti(ctx, server.URL, &flakyDialer{failOn: map[int32]bool{2: true}}, 3, false)
	if err != nil {
		t.Fatalf("expected success with 2 of 3 samples, got: %v", err)
	}
	t.Logf("partial-failure median delay: %d ms", delay)
}

func TestURLTestMultiFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	// Nothing listens on this port; all samples must fail.
	_, err := URLTestMulti(ctx, "http://127.0.0.1:1/generate_204", &directDialer{}, 2, false)
	if err == nil {
		t.Fatal("expected error for unreachable target")
	}
}
