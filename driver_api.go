package triflestats

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const (
	apiEndpoint       = "https://app.trifle.io/api/v1/metrics"
	apiRequestTimeout = 10 * time.Second
	apiErrorBodyLimit = 1024
)

// HTTPDoer is the minimal transport contract used by APIDriver.
type HTTPDoer interface {
	Do(*http.Request) (*http.Response, error)
}

// APIDriverOption customizes the API transport without changing the fixed endpoint.
type APIDriverOption func(*APIDriver)

// WithAPIHTTPClient injects an HTTP client, primarily for tests and future delivery policies.
func WithAPIHTTPClient(client HTTPDoer) APIDriverOption {
	return func(driver *APIDriver) {
		if client != nil {
			driver.client = client
		}
	}
}

// APIDriver synchronously writes Track and Assert calls to a Trifle Cloud project.
type APIDriver struct {
	token     string
	projectID string
	client    HTTPDoer
}

// APIError describes a failed or ambiguous API delivery. Writes are never retried automatically.
type APIError struct {
	StatusCode      int
	ResponseBody    string
	RetryAfter      string
	DeliveryUnknown bool
	Cause           error
}

func (e *APIError) Error() string {
	if e.StatusCode != 0 {
		return fmt.Sprintf("Trifle API returned HTTP %d", e.StatusCode)
	}
	return fmt.Sprintf("Trifle API request failed: %v", e.Cause)
}

func (e *APIError) Unwrap() error { return e.Cause }

// UnsupportedAPIOperationError reports operations not available in the write-only v1 driver.
type UnsupportedAPIOperationError struct{ Operation string }

func (e *UnsupportedAPIOperationError) Error() string {
	return fmt.Sprintf("%s is not supported by the API driver", e.Operation)
}

// NewAPIDriver creates a synchronous Trifle Cloud Projects driver.
func NewAPIDriver(token, projectID string, opts ...APIDriverOption) (*APIDriver, error) {
	if strings.TrimSpace(token) == "" {
		return nil, fmt.Errorf("token must not be empty")
	}
	if strings.TrimSpace(projectID) == "" {
		return nil, fmt.Errorf("projectID must not be empty")
	}

	driver := &APIDriver{
		token:     token,
		projectID: projectID,
		client:    &http.Client{Timeout: apiRequestTimeout},
	}
	for _, opt := range opts {
		if opt != nil {
			opt(driver)
		}
	}
	return driver, nil
}

func (d *APIDriver) Description() string { return "Trifle Stats API" }
func (d *APIDriver) BypassBuffer() bool  { return true }

func (d *APIDriver) DirectWrite(ctx context.Context, operation, key string, at time.Time, values map[string]any, untracked bool) error {
	payload, err := json.Marshal(map[string]any{
		"operation": operation,
		"key":       key,
		"at":        at.Format(time.RFC3339Nano),
		"values":    values,
		"untracked": untracked,
	})
	if err != nil {
		return fmt.Errorf("encode Trifle API request: %w", err)
	}

	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(payload); err != nil {
		return fmt.Errorf("compress Trifle API request: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("compress Trifle API request: %w", err)
	}

	request, err := http.NewRequestWithContext(ctx, http.MethodPost, apiEndpoint, &compressed)
	if err != nil {
		return fmt.Errorf("build Trifle API request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+d.token)
	request.Header.Set("X-Trifle-Source-Id", d.projectID)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Encoding", "gzip")
	request.Header.Set("User-Agent", "trifle-stats-go/v2")

	response, err := d.client.Do(request)
	if err != nil {
		return &APIError{DeliveryUnknown: true, Cause: err}
	}
	defer response.Body.Close()

	if response.StatusCode >= 200 && response.StatusCode <= 299 {
		_, _ = io.Copy(io.Discard, response.Body)
		return nil
	}

	body, readErr := io.ReadAll(io.LimitReader(response.Body, apiErrorBodyLimit))
	return &APIError{
		StatusCode:   response.StatusCode,
		ResponseBody: string(body),
		RetryAfter:   response.Header.Get("Retry-After"),
		Cause:        readErr,
	}
}

func (d *APIDriver) Inc(context.Context, []Key, map[string]any) error {
	return &UnsupportedAPIOperationError{Operation: "direct Inc"}
}

func (d *APIDriver) Set(context.Context, []Key, map[string]any) error {
	return &UnsupportedAPIOperationError{Operation: "direct Set"}
}

func (d *APIDriver) Get(context.Context, []Key) ([]map[string]any, error) {
	return nil, &UnsupportedAPIOperationError{Operation: "Values"}
}

func (d *APIDriver) Ping(context.Context, Key, map[string]any) error {
	return &UnsupportedAPIOperationError{Operation: "Beam"}
}

func (d *APIDriver) Scan(context.Context, Key) (time.Time, map[string]any, bool, error) {
	return time.Time{}, nil, false, &UnsupportedAPIOperationError{Operation: "Scan"}
}

var _ Driver = (*APIDriver)(nil)
var _ DirectWriter = (*APIDriver)(nil)
var _ BufferBypassDriver = (*APIDriver)(nil)
