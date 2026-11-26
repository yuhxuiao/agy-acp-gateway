package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"syscall"
	"time"

	gwcfg "agy-acp-gateway-go/internal/config"
)

func main() {
	gwcfg.InitProcessGCDefaults()
	// container first-run: make sure the default workspace and log dir exist
	_ = os.MkdirAll(DefaultCwd, 0755)
	_ = os.MkdirAll(filepath.Dir(GatewayLogFile), 0700)
	if authConfigErr != nil {
		LogEvent("AUTH_CONFIG_ERR", authConfigErr.Error(), nil)
	}
	GlobalAccountManager.Init()
	syncPrimaryPromptSemBeforeServe()
	gw := NewGatewayServer()
	if !isChildAccountWorker() {
		GlobalAccountManager.SetPrimaryOccupiedSlotsFn(gw.CountOccupiedWorkerSlots)
		GlobalAccountManager.SetPrimaryCancelParkedFn(gw.CancelParkedConversation)
	}

	// Pre-warm SQLite index asynchronously so the first dashboard poll is instant, then release temp buffers to OS
	go func() {
		gwSids := getKnownGatewaySids()
		convDir := ConversationsDir()
		appConvDir := AppConversationsDir()
		if GlobalStore.IsReady() {
			if convDir != "" {
				GlobalStore.SyncUsageStepsFromDir(convDir, gwSids)
			}
			if appConvDir != "" {
				GlobalStore.SyncUsageStepsFromSourceDir(appConvDir, "APP", "AGY_APP", "Antigravity App", gwSids)
			}
		} else {
			_ = readFallbackStepsFromDir(convDir, "HOST", "AGY_CLI", "AGY CLI", gwSids)
			if appConvDir != "" {
				_ = readFallbackStepsFromDir(appConvDir, "APP", "AGY_APP", "Antigravity App", gwSids)
			}
		}
		runtime.GC()
		debug.FreeOSMemory()
	}()

	srv18890 := gatewayHTTPServer(fmt.Sprintf("%s:%d", ListenHost, GatewayPort), gw.GatewayOnlyHandler())
	srv18891 := gatewayHTTPServer(fmt.Sprintf("%s:%d", ListenHost, DashboardPort), gw.DashboardOnlyHandler())

	go func() {
		var modelIDs []string
		for _, m := range AgyModels {
			modelIDs = append(modelIDs, m.ID)
		}
		LogEvent("GATEWAY_READY", fmt.Sprintf("Go gateway listening on http://%s:%d & :%d", ListenHost, GatewayPort, DashboardPort), map[string]any{
			"engine":               "Go native",
			"defaultModel":         DefaultModelID,
			"models":               modelIDs,
			"authEnabled":          authEnabled(),
			"maxLiveWorkers":       effectiveMaxLiveWorkers(),
			"maxConcurrentPrompts": effectiveMaxConcurrentPrompts(),
			"pureApiHome":          PureApiHome,
			"logFile":              GatewayLogFile,
		})
		if err := srv18890.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "18890 listen error: %v\n", err)
			os.Exit(1)
		}
	}()

	go func() {
		if err := srv18891.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(os.Stderr, "18891 listen error: %v\n", err)
			os.Exit(1)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	drainTimeout := 6 * time.Second
	if isChildAccountWorker() {
		drainTimeout = 4 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), drainTimeout)
	defer cancel()
	if err := drainHTTPServers(ctx, srv18890, srv18891); err != nil {
		LogEvent("HTTP_DRAIN_TIMEOUT", "Grace period expired; remaining connections closed", nil)
	}
	gw.Close()
	GlobalAccountManager.StopAll()
	GlobalEngine.StopEngine()
	GlobalStore.Close()
}
