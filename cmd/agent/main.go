package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"go.uber.org/zap"

	"github.com/hive-sre/hive-agent/internal/auth"
	"github.com/hive-sre/hive-agent/internal/clustermanager"
	"github.com/hive-sre/hive-agent/internal/commands"
	"github.com/hive-sre/hive-agent/internal/config"
	"github.com/hive-sre/hive-agent/internal/executor"
	"github.com/hive-sre/hive-agent/internal/heartbeat"
	"github.com/hive-sre/hive-agent/internal/inventory"
	hivekube "github.com/hive-sre/hive-agent/internal/kubernetes"
	"github.com/hive-sre/hive-agent/internal/registration"
	"github.com/hive-sre/hive-agent/internal/state"
	"github.com/hive-sre/hive-agent/internal/telemetry"
	"github.com/hive-sre/hive-agent/internal/websocket"
	"k8s.io/client-go/rest"
)

const version = "1.0.0"

func main() {
	root := &cobra.Command{Use: "hive-agent", RunE: run}
	root.Flags().String("config", "", "config file path")
	root.AddCommand(&cobra.Command{Use: "healthz", Run: func(cmd *cobra.Command, args []string) { fmt.Fprintln(cmd.OutOrStdout(), "healthy") }})
	root.AddCommand(&cobra.Command{Use: "readyz", Run: func(cmd *cobra.Command, args []string) { fmt.Fprintln(cmd.OutOrStdout(), "ready") }})
	if err := root.Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "hive-agent failed: %v\n", err)
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, _ []string) error {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	cfg, err := config.Load(cmd.Flags().Lookup("config").Value.String())
	if err != nil {
		return err
	}
	log, err := telemetry.NewLogger(cfg.LogLevel)
	if err != nil {
		return err
	}
	defer log.Sync()
	shutdownTelemetry, err := telemetry.InitOTel(ctx, cfg)
	if err != nil {
		log.Warn("otel disabled", zap.Error(err))
	} else {
		defer shutdownTelemetry(context.Background())
	}
	metrics := telemetry.NewMetrics()
	machine := state.New(func(s state.Snapshot) { metrics.SetState(string(s.State)) })
	metrics.SetState(string(machine.Snapshot().State))
	// Build in-cluster REST config so we can pass it to ClusterManager
	restCfg, err := rest.InClusterConfig()
	if err != nil {
		// Fallback: if not running in-cluster (e.g. local dev), continue without restCfg
		log.Warn("could not obtain in-cluster REST config; ClusterAwareConfig will be unavailable", zap.Error(err))
		restCfg = nil
	}
	kube, err := hivekube.NewInCluster(ctx, log)
	if err != nil {
		return err
	}
	meta, err := kube.ClusterMetadata(ctx)
	if err != nil {
		return err
	}
	registrar := registration.NewClient(cfg, auth.NewTokenStore(), log)
	agentCapabilities := commands.Capabilities()
	req := registration.Request{
		ClusterName:       cfg.ClusterName,
		Provider:          cfg.Provider,
		AgentVersion:      version,
		KubernetesVersion: meta.KubernetesVersion,
		ClusterUID:        meta.ClusterUID,
		NodeCount:         meta.NodeCount,
		APIKey:            cfg.APIKey,
		Capabilities:      agentCapabilities,
		State:             string(machine.Snapshot().State),
	}
	reg, err := registrar.Register(ctx, req)
	if err != nil {
		return err
	}
	registrar.StoreRequest(req)
	_ = machine.Transition(state.Registered, "cluster registered")
	// Register initial capabilities.  If the server rejects the JWT (stale
	// cache on first connect), use the singleflight Reregister path which also
	// opens the token gate so every subsequent call gets the fresh JWT.
	if capErr := registrar.RegisterCapabilities(ctx, reg.ClusterID, agentCapabilities); capErr != nil {
		if errors.Is(capErr, registration.ErrUnauthorized) {
			log.Warn("capabilities rejected (JWT stale); re-registering", zap.Error(capErr))
			if reErr := registrar.Reregister(ctx); reErr != nil {
				return fmt.Errorf("re-register after capabilities 401: %w", reErr)
			}
			// Update reg with the new cluster ID that Reregister may have received.
			// RegisterCapabilities will use the fresh JWT from TokenStore automatically.
			if capErr2 := registrar.RegisterCapabilities(ctx, reg.ClusterID, agentCapabilities); capErr2 != nil {
				return fmt.Errorf("capabilities failed after re-register: %w", capErr2)
			}
		} else {
			return fmt.Errorf("register capabilities: %w", capErr)
		}
	}
	log.Info("registered hive agent", zap.String("clusterId", reg.ClusterID))

	// Register cluster in the ClusterManager so callers can use ClusterAwareConfig.
	cm := clustermanager.New(log)
	if restCfg != nil {
		cm.Register(reg.ClusterID, kube, restCfg)
	}

	commandExecutor := commands.NewExecutor(reg.ClusterID, kube)
	exec := executor.New(reg.ClusterID, commandExecutor, log, metrics)
	inv := inventory.New(kube, registrar, reg.ClusterID, metrics, log)
	hb := heartbeat.New(kube, registrar, reg.ClusterID, machine, metrics, log)
	tunnel := websocket.NewManager(cfg, registrar.TokenStore(), reg, exec, registrar, machine, metrics, log, cm)
	server := healthServer(cfg, tunnel, metrics)
	go func() { _ = server.ListenAndServe() }()
	go hb.Run(ctx, 30*time.Second)
	go inv.Run(ctx, 5*time.Minute)
	go tunnel.Run(ctx)
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = server.Shutdown(shutdown)
	return nil
}

func healthServer(cfg config.Config, tunnel *websocket.Manager, _ *telemetry.Metrics) *http.Server {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"healthy"}`))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		if !tunnel.Connected() {
			http.Error(w, `{"status":"not_ready"}`, http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ready"}`))
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) { http.Error(w, "not found", http.StatusNotFound) })
	return &http.Server{Addr: cfg.ListenAddress, Handler: mux, ReadHeaderTimeout: 5 * time.Second, BaseContext: func(_ net.Listener) context.Context { return context.Background() }}
}
