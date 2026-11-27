package main

import (
	"context"
	"net/http"
	"sync"
	"time"
)

func gatewayHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{Addr: addr, Handler: handler, ReadHeaderTimeout: 15 * time.Second, MaxHeaderBytes: 1 << 20, IdleTimeout: 120 * time.Second}
}

func drainHTTPServers(ctx context.Context, servers ...*http.Server) error {
	var wg sync.WaitGroup
	errs := make(chan error, len(servers))
	for _, server := range servers {
		wg.Add(1)
		go func(s *http.Server) {
			defer wg.Done()
			if err := s.Shutdown(ctx); err != nil {
				_ = s.Close()
				errs <- err
			}
		}(server)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		return err
	}
	return nil
}
