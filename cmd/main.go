package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/kl09/crypto-addresses-parser/internal/parser/eth"
	"github.com/kl09/crypto-addresses-parser/internal/repository"
	"github.com/kl09/crypto-addresses-parser/internal/writer"
)

const (
	ethWatcherGoRoutines = 5
	bnbWatcherGoRoutines = 5
	postgresDSN          = "postgres://127.0.0.1:5432/wallet"
	dbWriterBatch        = 10000
	walletRepoChanSize   = dbWriterBatch * 10
)

var ethAPIs = []string{
	"https://ethereum-rpc.publicnode.com",
	"https://eth.blockrazor.xyz",
}

var bnbAPIs = []string{
	"https://bsc-dataseed1.defibit.io",
	"https://bsc.blockrazor.xyz",
	"https://bsc-rpc.publicnode.com",
}

func main() {
	// TODO: several API,
	f, err := os.OpenFile("logs.txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatalf("error opening file: %v", err)
		return
	}

	w := io.MultiWriter(os.Stdout, f)
	slog.SetDefault(slog.New(slog.NewJSONHandler(w, nil)))

	ctx, cancelFn := context.WithCancelCause(context.Background())

	db, err := dialPG(ctx, postgresDSN)
	if err != nil {
		slog.Error(fmt.Sprintf("dial PG: %s", err))
		os.Exit(1)
	}

	walletRepoCh := make(chan string, walletRepoChanSize)
	walletsRepository := repository.NewBalancesRepository(db)
	blockRepository := repository.NewBlocksRepository(db)

	walletsWriter := writer.NewWalletWriter(walletsRepository, walletRepoCh, dbWriterBatch)
	writerDone := make(chan struct{})
	go func() {
		walletsWriter.WriteWallets(ctx)
		writerDone <- struct{}{}
	}()

	ethWatcher := eth.NewParser(ethAPIs, "eth", ethWatcherGoRoutines, walletRepoCh, blockRepository)
	bnbWatcher := eth.NewParser(bnbAPIs, "bnb", bnbWatcherGoRoutines, walletRepoCh, blockRepository)

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-signalChan
		slog.Info("signal received, shutting down")
		signal.Reset()
		cancelFn(errors.New("shutdown"))
	}()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err = ethWatcher.ParsePast(ctx)
		if err != nil {
			slog.Error("parse ETH addresses from block to start", "error", err)
		}
		slog.Info("ETH ParsePast done")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		err = bnbWatcher.ParsePast(ctx)
		if err != nil {
			slog.Error("parse BNB addresses from block to start", "error", err)
		}
		slog.Info("BNB ParsePast done")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		err = ethWatcher.ParseNew(ctx)
		if err != nil {
			slog.Error("parse ETH addresses from new block", "error", err)
		}
		slog.Info("ETH ParseNew done")
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		err = bnbWatcher.ParseNew(ctx)
		if err != nil {
			slog.Error("parse BNB addresses from new block", "error", err)
		}
		slog.Info("BNB ParseNew done")
	}()

	wg.Wait()
	close(walletRepoCh)

	select {
	case <-writerDone:
	case <-time.After(time.Minute):
	}
}

func dialPG(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("create pool: %w", err)
	}

	return pool, nil
}
