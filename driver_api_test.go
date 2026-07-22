package triflestats

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type recordingHTTPClient struct {
	requests []*http.Request
	status   int
	headers  http.Header
	err      error
}

func (c *recordingHTTPClient) Do(request *http.Request) (*http.Response, error) {
	c.requests = append(c.requests, request)
	if c.err != nil {
		return nil, c.err
	}
	return &http.Response{
		StatusCode: c.status,
		Header:     c.headers,
		Body:       io.NopCloser(strings.NewReader("busy")),
	}, nil
}

func TestAPIDriverWritesImmediatelyAsGzipJSON(t *testing.T) {
	client := &recordingHTTPClient{status: http.StatusCreated, headers: http.Header{}}
	driver, err := NewAPIDriver("secret", "project-1", WithAPIHTTPClient(client))
	if err != nil {
		t.Fatal(err)
	}
	cfg := DefaultConfig()
	cfg.Driver = driver
	cfg.BufferEnabled = true
	at := time.Date(2026, 7, 22, 12, 0, 0, 0, time.UTC)

	if err := Track(context.Background(), cfg, "orders", at, map[string]any{"count": 1}, Untracked()); err != nil {
		t.Fatal(err)
	}
	if err := Assert(context.Background(), cfg, "state::orders", at, map[string]any{"pending": 4}); err != nil {
		t.Fatal(err)
	}
	if cfg.Storage() != driver {
		t.Fatal("expected API driver to bypass buffer")
	}
	if err := cfg.FlushBuffer(); err != nil {
		t.Fatal(err)
	}
	if len(client.requests) != 2 {
		t.Fatalf("expected two immediate requests, got %d", len(client.requests))
	}

	reader, err := gzip.NewReader(client.requests[0].Body)
	if err != nil {
		t.Fatal(err)
	}
	var payload map[string]any
	if err := json.NewDecoder(reader).Decode(&payload); err != nil {
		t.Fatal(err)
	}
	if payload["operation"] != "track" || payload["untracked"] != true {
		t.Fatalf("unexpected payload: %#v", payload)
	}
	if client.requests[0].URL.String() != apiEndpoint {
		t.Fatalf("unexpected endpoint: %s", client.requests[0].URL)
	}
	if client.requests[0].Header.Get("Content-Encoding") != "gzip" {
		t.Fatal("expected gzip content encoding")
	}
	if client.requests[0].Header.Get("Authorization") != "Bearer secret" || client.requests[0].Header.Get("X-Trifle-Source-Id") != "project-1" {
		t.Fatal("expected project authentication headers")
	}
}

func TestAPIDriverSurfacesOverloadWithoutRetry(t *testing.T) {
	client := &recordingHTTPClient{
		status:  http.StatusTooManyRequests,
		headers: http.Header{"Retry-After": []string{"3"}},
	}
	driver, err := NewAPIDriver("secret", "project-1", WithAPIHTTPClient(client))
	if err != nil {
		t.Fatal(err)
	}

	err = driver.DirectWrite(context.Background(), "track", "orders", time.Now(), map[string]any{"count": 1}, false)
	apiErr, ok := err.(*APIError)
	if !ok || apiErr.StatusCode != 429 || apiErr.RetryAfter != "3" {
		t.Fatalf("unexpected error: %#v", err)
	}
	if len(client.requests) != 1 {
		t.Fatalf("expected no retry, got %d requests", len(client.requests))
	}
}

func TestAPIDriverValidatesCredentialsAndRejectsReads(t *testing.T) {
	if _, err := NewAPIDriver("", "project-1"); err == nil {
		t.Fatal("expected token validation error")
	}
	driver, err := NewAPIDriver("secret", "project-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := driver.Get(context.Background(), nil); err == nil {
		t.Fatal("expected Values to be unsupported")
	}
}

func TestAPIDriverMarksTransportFailuresAsUnknownWithoutRetry(t *testing.T) {
	client := &recordingHTTPClient{err: context.DeadlineExceeded}
	driver, err := NewAPIDriver("secret", "project-1", WithAPIHTTPClient(client))
	if err != nil {
		t.Fatal(err)
	}

	err = driver.DirectWrite(context.Background(), "track", "orders", time.Now(), map[string]any{"count": 1}, false)
	apiErr, ok := err.(*APIError)
	if !ok || !apiErr.DeliveryUnknown {
		t.Fatalf("unexpected error: %#v", err)
	}
	if len(client.requests) != 1 {
		t.Fatalf("expected no retry, got %d requests", len(client.requests))
	}
}
