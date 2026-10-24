package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

type Outcome string

const (
	OutcomeOK     Outcome = "ok"
	OutcomeBroken Outcome = "broken"
	OutcomeReview Outcome = "review"
)

type Result struct {
	URL      string  `json:"url"`
	FinalURL string  `json:"final_url,omitempty"`
	Status   int     `json:"status,omitempty"`
	Outcome  Outcome `json:"outcome"`
	Error    string  `json:"error,omitempty"`
}

func readURLs(input io.Reader) ([]string, error) {
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 4096), 64*1024)
	seen := make(map[string]bool)
	var urls []string
	line := 0

	for scanner.Scan() {
		line++
		raw := strings.TrimSpace(scanner.Text())
		if raw == "" {
			continue
		}

		parsed, err := url.Parse(raw)
		if err != nil {
			return nil, fmt.Errorf("line %d: invalid URL: %w", line, err)
		}
		if (parsed.Scheme != "http" && parsed.Scheme != "https") ||
			parsed.Hostname() == "" || parsed.User != nil {
			return nil, fmt.Errorf("line %d: expected an absolute HTTP(S) URL without credentials", line)
		}

		parsed.Host = strings.ToLower(parsed.Host)
		if port := parsed.Port(); (parsed.Scheme == "http" && port == "80") ||
			(parsed.Scheme == "https" && port == "443") {
			parsed.Host = strings.TrimSuffix(parsed.Host, ":"+port)
		}
		if parsed.Path == "" {
			parsed.Path = "/"
		}
		parsed.Fragment = ""
		parsed.RawFragment = ""
		normalised := parsed.String()
		if seen[normalised] {
			continue
		}

		seen[normalised] = true
		urls = append(urls, normalised)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read URLs: %w", err)
	}
	if len(urls) == 0 {
		return nil, fmt.Errorf("input contains no URLs")
	}

	return urls, nil
}

func newClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return fmt.Errorf("redirect limit reached")
			}
			if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
				return fmt.Errorf("redirect uses an unsupported scheme")
			}
			if req.URL.User != nil {
				return fmt.Errorf("redirect contains credentials")
			}
			return nil
		},
	}
}

func checkURL(ctx context.Context, client *http.Client, target string) Result {
	result := Result{URL: target, Outcome: OutcomeReview}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	req.Header.Set("User-Agent", "article-linkcheck/1.0")

	response, err := client.Do(req)
	if err != nil {
		result.Error = err.Error()
		return result
	}
	defer func() {
		// Drain a bounded amount so the connection can return to the pool;
		// bodies larger than this are cut off and the connection is dropped.
		io.Copy(io.Discard, io.LimitReader(response.Body, 64*1024))
		response.Body.Close()
	}()

	result.Status = response.StatusCode
	result.FinalURL = response.Request.URL.String()

	switch {
	case response.StatusCode >= 200 && response.StatusCode < 300:
		result.Outcome = OutcomeOK
	case response.StatusCode == http.StatusNotFound ||
		response.StatusCode == http.StatusGone:
		result.Outcome = OutcomeBroken
	}

	return result
}

// checkAll checks urls with at most `workers` requests in flight, handing out
// jobs at least `interval` apart. Results arrive in completion order; the
// channel closes once every worker has stopped, either because all URLs were
// checked or because ctx was cancelled.
func checkAll(
	ctx context.Context,
	client *http.Client,
	urls []string,
	workers int,
	interval time.Duration,
) <-chan Result {
	jobs := make(chan string)
	results := make(chan Result)

	go func() {
		defer close(jobs)

		for i, target := range urls {
			if i > 0 {
				select {
				case <-ctx.Done():
					return
				case <-time.After(interval):
				}
			}

			select {
			case <-ctx.Done():
				return
			case jobs <- target:
			}
		}
	}()

	var running sync.WaitGroup
	for range workers {
		running.Go(func() {
			for target := range jobs {
				if ctx.Err() != nil {
					return
				}
				result := checkURL(ctx, client, target)
				select {
				case <-ctx.Done():
					return
				case results <- result:
				}
			}
		})
	}

	go func() {
		running.Wait()
		close(results)
	}()

	return results
}
