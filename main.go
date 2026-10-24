package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	os.Exit(run())
}

func run() int {
	workers := flag.Int("workers", 4, "maximum concurrent checks")
	interval := flag.Duration("interval", 250*time.Millisecond, "minimum wait between job handoffs")
	timeout := flag.Duration("timeout", 10*time.Second, "timeout for one check")
	budget := flag.Duration("budget", 5*time.Minute, "time budget for the network scan")
	flag.Parse()

	if flag.NArg() != 1 || *workers < 1 || *interval <= 0 || *timeout <= 0 || *budget <= 0 {
		fmt.Fprintln(os.Stderr, "usage: linkcheck [positive limits] urls.txt")
		return 2
	}

	file, err := os.Open(flag.Arg(0))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	urls, err := readURLs(file)
	file.Close()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}

	signals, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(signals, *budget)
	defer cancel()
	client := newClient(*timeout)
	defer client.CloseIdleConnections()

	encoder := json.NewEncoder(os.Stdout)
	results := checkAll(ctx, client, urls, *workers, *interval)
	checked, problems := 0, 0
	for result := range results {
		if err := encoder.Encode(result); err != nil {
			cancel()
			for range results {
			}
			fmt.Fprintln(os.Stderr, err)
			return 2
		}
		checked++
		if result.Outcome != OutcomeOK {
			problems++
		}
	}

	fmt.Fprintf(os.Stderr, "checked=%d total=%d needs_attention=%d\n", checked, len(urls), problems)
	if ctx.Err() != nil {
		fmt.Fprintln(os.Stderr, "scan incomplete:", ctx.Err())
		return 2
	}
	if problems > 0 {
		return 1
	}
	return 0
}
