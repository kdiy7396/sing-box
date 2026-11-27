package urltest

// This file is intentionally self-contained: it adds multi-sample latency
// testing without modifying any upstream function in urltest.go, so the
// patch can be carried across sing-box releases by copying this file (and
// the test file) and re-applying the small Clash API handler change in
// experimental/clashapi/proxies.go.

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"sort"
	"time"

	"github.com/sagernet/sing-anytls"
	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-mux"
	"github.com/sagernet/sing-snell"
	"github.com/sagernet/sing/common"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/ntp"
)

// URLTestMulti is a v2rayNG-style latency test: unlike URLTest, which times a
// single cold request (fresh connection, fresh TLS session, no reuse) and
// reports that one sample, URLTestMulti shares one HTTP client/transport
// across all requests so connection pooling behaves like a real client,
// optionally discards one warm-up request (which pays the dial, DNS and TLS
// costs), then measures samples requests and reports the median delay.
//
// The measured samples still go through the detour on a pooled connection,
// so the result approximates the latency a real application sees on an
// already-established proxy connection instead of full connection setup.
//
// Timeout semantics: the caller's ctx is the total budget for the warm-up
// plus all samples (they run serially). Each individual request is
// additionally capped by a per-sample timeout of C.TCPTimeout derived from
// that ctx, so one hung request cannot starve the remaining samples of
// their chance to run, but callers should still size the total budget for
// 1+samples requests (e.g. a 2s budget cannot fit 10 samples of a 300ms
// path). Like urlTest, requests use HEAD and the response status is not
// checked, so endpoints that answer HEAD differently (e.g. 405) still
// produce a measurement, with the same semantics as the single test.
//
// Result semantics: at least half of the measured samples (rounded up)
// must succeed, otherwise the run is reported as failed — a median built
// from a small minority of successes would hide a mostly-broken path.
func URLTestMulti(ctx context.Context, link string, detour N.Dialer, samples int, warmup bool) (uint16, error) {
	if samples <= 0 {
		samples = 3
	}
	if samples > 10 {
		samples = 10
	}
	if link == "" {
		link = "https://www.gstatic.com/generate_204"
	}
	transport := &http.Transport{
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(addr)
			if err != nil {
				return nil, err
			}
			return detour.DialContext(ctx, network, M.ParseSocksaddrHostPortStr(host, port))
		},
		TLSClientConfig: &tls.Config{
			Time:    ntp.TimeFuncFromContext(ctx),
			RootCAs: adapter.RootPoolFromContext(ctx),
		},
		MaxIdleConnsPerHost: 1,
	}
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},
		Timeout: C.TCPTimeout,
	}
	// For multiplex detours, keep-session applies to every request, not
	// just the warm-up: the warm-up establishes the kept session and the
	// measured samples must dial under the same session semantics to reuse
	// it, instead of switching context flavors mid-run.
	requestCtx := ctx
	if multiplexOutbound, isMultiplexOutbound := common.Cast[adapter.OutboundWithMultiplex](detour); isMultiplexOutbound && multiplexOutbound.MultiplexEnabled() {
		requestCtx = adapter.ContextWithKeepSession(requestCtx)
		requestCtx = mux.ContextWithKeepSession(requestCtx)
		requestCtx = anytls.ContextWithKeepSession(requestCtx)
		requestCtx = contextWithQUICKeepSession(requestCtx)
		requestCtx = snell.ContextWithKeepSession(requestCtx)
	}
	doSample := func() (uint16, error) {
		// Per-sample timeout derived from the shared budget: one hung
		// request is cut at C.TCPTimeout instead of consuming the whole
		// run, leaving the remaining samples their share of the budget.
		sampleCtx, cancel := context.WithTimeout(requestCtx, C.TCPTimeout)
		defer cancel()
		req, err := http.NewRequestWithContext(sampleCtx, http.MethodHead, link, nil)
		if err != nil {
			return 0, err
		}
		start := time.Now()
		resp, err := client.Do(req)
		if err != nil {
			return 0, err
		}
		resp.Body.Close()
		delay := time.Since(start) / time.Millisecond
		if delay > 65535 {
			delay = 65535
		}
		return uint16(delay), nil
	}
	if warmup {
		// The warm-up result is intentionally discarded: it establishes the
		// pooled connection (and any DNS/TLS/session caches along the chain)
		// that the measured samples then reuse. Its error is not fatal; the
		// measured samples below report failures on their own.
		_, _ = doSample()
	}
	delays := make([]uint16, 0, samples)
	var lastErr error
	for i := 0; i < samples; i++ {
		delay, err := doSample()
		if err != nil {
			lastErr = err
			if ctx.Err() != nil {
				return 0, err
			}
			continue
		}
		delays = append(delays, delay)
	}
	if len(delays) < (samples+1)/2 {
		if lastErr != nil {
			return 0, fmt.Errorf("urltest: only %d of %d samples succeeded: %w", len(delays), samples, lastErr)
		}
		return 0, fmt.Errorf("urltest: only %d of %d samples succeeded", len(delays), samples)
	}
	return MedianDelay(delays), nil
}

// MedianDelay returns the median of the given delay samples.
func MedianDelay(delays []uint16) uint16 {
	if len(delays) == 0 {
		return 0
	}
	sorted := make([]uint16, len(delays))
	copy(sorted, delays)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	middle := len(sorted) / 2
	if len(sorted)%2 == 1 {
		return sorted[middle]
	}
	return uint16((uint32(sorted[middle-1]) + uint32(sorted[middle])) / 2)
}
