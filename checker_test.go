package main

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestReadURLs(t *testing.T) {
	urls, err := readURLs(strings.NewReader(
		"https://example.com/docs#one\n\nhttps://Example.COM/docs#two\nhttps://example.com/docs?v=2\n",
	))
	want := []string{"https://example.com/docs", "https://example.com/docs?v=2"}
	if err != nil || !reflect.DeepEqual(urls, want) {
		t.Fatalf("urls=%v err=%v, want %v", urls, err, want)
	}
	if _, err := readURLs(strings.NewReader("file:///etc/passwd\n")); err == nil {
		t.Fatal("expected a rejected URL")
	}
}

func TestCheckURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/ok":
			w.WriteHeader(http.StatusOK)
		case "/moved":
			http.Redirect(w, r, "/ok", http.StatusMovedPermanently)
		case "/loop":
			http.Redirect(w, r, "/loop", http.StatusFound)
		case "/limited":
			w.WriteHeader(http.StatusTooManyRequests)
		case "/unavailable":
			w.WriteHeader(http.StatusServiceUnavailable)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	client := newClient(time.Second)
	defer client.CloseIdleConnections()

	for _, test := range []struct {
		path    string
		outcome Outcome
		status  int
	}{
		{"/ok", "ok", 200},
		{"/moved", "ok", 200},
		{"/missing", "broken", 404},
		{"/limited", "review", 429},
		{"/unavailable", "review", 503},
		{"/loop", "review", 0},
	} {
		t.Run(test.path, func(t *testing.T) {
			result := checkURL(context.Background(), client, server.URL+test.path)
			if result.Outcome != test.outcome || result.Status != test.status {
				t.Fatalf("unexpected result: %+v", result)
			}
			if test.path == "/moved" && result.FinalURL != server.URL+"/ok" {
				t.Fatalf("unexpected redirect destination: %s", result.FinalURL)
			}
			if test.path == "/loop" && result.Error == "" {
				t.Fatal("expected a redirect error")
			}
		})
	}
}

func TestCheckURLTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()
	client := newClient(100 * time.Millisecond)
	defer client.CloseIdleConnections()
	result := checkURL(context.Background(), client, server.URL)
	if result.Outcome != "review" || result.Error == "" {
		t.Fatalf("expected an uncertain timeout result: %+v", result)
	}
}

func TestCheckAllBoundsConcurrency(t *testing.T) {
	var active, peak atomic.Int32
	started := make(chan struct{}, 12)
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		current := active.Add(1)
		defer active.Add(-1)
		for previous := peak.Load(); current > previous; previous = peak.Load() {
			if peak.CompareAndSwap(previous, current) {
				break
			}
		}
		started <- struct{}{}
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client := newClient(time.Second)
	defer client.CloseIdleConnections()
	urls := make([]string, 12)
	for i := range urls {
		urls[i] = server.URL
	}
	results := checkAll(ctx, client, urls, 3, time.Millisecond)
	for range 3 {
		select {
		case <-started:
		case <-ctx.Done():
			t.Fatal("workers did not start")
		}
	}
	close(release)
	count := 0
	for result := range results {
		if result.Outcome != "ok" {
			t.Fatalf("unexpected result: %+v", result)
		}
		count++
	}
	if count != len(urls) || peak.Load() != 3 {
		t.Fatalf("results=%d peak=%d", count, peak.Load())
	}
}

func TestCheckAllCancellation(t *testing.T) {
	started := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		started <- struct{}{}
		<-r.Context().Done()
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := newClient(5 * time.Second)
	defer client.CloseIdleConnections()
	results := checkAll(ctx, client, []string{server.URL, server.URL}, 1, time.Millisecond)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("request did not start")
	}
	cancel()
	deadline := time.After(2 * time.Second)
	for {
		select {
		case _, open := <-results:
			if !open {
				return
			}
		case <-deadline:
			t.Fatal("pipeline did not stop after cancellation")
		}
	}
}
