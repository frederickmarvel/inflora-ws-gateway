package main

import (
	"context"
	"github.com/frederickmarvel/inflora-shared/db"
	"github.com/frederickmarvel/inflora-shared/events"
	"github.com/frederickmarvel/inflora-shared/observability"
	"github.com/frederickmarvel/inflora-ws-gateway/internal/auth"
	"github.com/frederickmarvel/inflora-ws-gateway/internal/config"
	"github.com/frederickmarvel/inflora-ws-gateway/internal/handler"
	"github.com/frederickmarvel/inflora-ws-gateway/internal/seq"
	"github.com/frederickmarvel/inflora-ws-gateway/internal/subscriber"
	"github.com/frederickmarvel/inflora-ws-gateway/internal/ws"
	"go.uber.org/zap"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		panic(err)
	}
	log, err := observability.NewLogger(cfg.ServiceName, cfg.LogLevel, cfg.LogFormat)
	if err != nil {
		panic(err)
	}
	defer observability.Sync(log)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	database, err := db.Open(ctx, db.Config{DSN: cfg.DBDSN, MaxOpenConns: cfg.DBMaxOpen, MaxIdleConns: cfg.DBMaxIdle, PingTimeout: 5 * time.Second})
	if err != nil {
		log.Fatal("database", zap.Error(err))
	}
	defer database.Close()
	validator := auth.NewValidator(cfg.TolkienURL, cfg.InternalKey, cfg.AuthCacheTTL)
	store := &seq.Store{DB: database, Retention: cfg.ReplayWindow}
	hub := ws.NewHub(cfg.MaxConnections)
	natsConfig := events.NATSConfig{URL: cfg.NATSURL, Name: cfg.ServiceName, Stream: cfg.NATSStream, Subjects: []string{events.DonationCharged + ".>", events.OverlayTokenRotated + ".>"}}
	donationNATS, err := events.NewNATSSubscriber(natsConfig)
	if err != nil {
		log.Fatal("nats donation subscriber", zap.Error(err))
	}
	defer donationNATS.Close()
	rotationNATS, err := events.NewNATSSubscriber(natsConfig)
	if err != nil {
		log.Fatal("nats rotation subscriber", zap.Error(err))
	}
	defer rotationNATS.Close()
	runCtx, stop := context.WithCancel(context.Background())
	defer stop()
	go func() {
		if err := (subscriber.Donation{N: donationNATS, Store: store, Hub: hub}).Run(runCtx); err != nil && runCtx.Err() == nil {
			log.Error("donation consumer stopped", zap.Error(err))
		}
	}()
	go func() {
		if err := (subscriber.Rotation{N: rotationNATS, Auth: validator}).Run(runCtx); err != nil && runCtx.Err() == nil {
			log.Error("rotation consumer stopped", zap.Error(err))
		}
	}()
	srv := &http.Server{Addr: cfg.HTTPAddr, Handler: handler.New(handler.Server{DB: database, Auth: validator, Store: store, Hub: hub, Heartbeat: cfg.HeartbeatInterval, Timeout: cfg.HeartbeatTimeout, Window: cfg.ReplayWindow, Version: "1.0.0", AllowedOrigins: cfg.AllowedOrigins, DevOpenOrigin: cfg.DevOpenOrigin, OverlayDir: handler.ResolveOverlayDir(cfg.OverlayDir)})}
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatal("http", zap.Error(err))
		}
	}()
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	<-ch
	stop()
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancelShutdown()
	_ = srv.Shutdown(shutdown)
}
