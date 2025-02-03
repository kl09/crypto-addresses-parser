package eth

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/kl09/crypto-addresses-parser/internal/domain"
	"github.com/onrik/ethrpc"
)

var errBlockIsNil = errors.New("block is nil")

const (
	newBlockName  = "new"
	pastBlockName = "past"
)

type Client struct {
	*ethrpc.EthRPC
	name string
}

type Parser struct {
	rpcClients         []*Client
	walletsChan        chan string
	blocksRepository   domain.BlocksRepository
	network            string
	startedAt          time.Time
	statsEvery         time.Duration
	delayBetweenErrors time.Duration

	maxGoroutinesNumber int
}

func NewParser(
	api []string,
	network string,
	maxGoroutinesNumber int,
	walletsChan chan string,
	blocksRepository domain.BlocksRepository,
) *Parser {
	rpcClients := make([]*Client, 0, len(api))
	for _, a := range api {
		rpcClients = append(rpcClients, &Client{
			EthRPC: ethrpc.NewEthRPC(
				a,
				ethrpc.WithHttpClient(
					&http.Client{
						Transport: &http.Transport{
							DialContext: (&net.Dialer{
								Timeout: 20 * time.Second,
							}).DialContext,
							MaxIdleConns:    10,
							MaxConnsPerHost: 10,
							IdleConnTimeout: 90 * time.Second,
						},
						Timeout: 60 * time.Second,
					},
				),
			),
			name: a,
		})
	}

	return &Parser{
		rpcClients:          rpcClients,
		network:             network,
		walletsChan:         walletsChan,
		maxGoroutinesNumber: maxGoroutinesNumber,
		blocksRepository:    blocksRepository,
		startedAt:           time.Now(),
		statsEvery:          30 * time.Second,
		delayBetweenErrors:  5 * time.Second,
	}
}

func (w *Parser) randomClient() *Client {
	return w.rpcClients[rand.Intn(len(w.rpcClients))]
}

func (w *Parser) ParseNew(ctx context.Context) error {
	var (
		counter     int64
		statsMx     = sync.Mutex{}
		lastStatsAt = time.Now()
		err         error
	)

	logger := slog.With("network", w.network, "operation", newBlockName)
	logger.Info("start parsing new blocks")
	blockNumber, err := w.getBlockNumber(ctx, w.network, newBlockName)
	if err != nil {
		return fmt.Errorf("get block number: %w", err)
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			addresses, err := w.addressesFromBlock(blockNumber)
			if err != nil {
				if !errors.Is(err, errBlockIsNil) {
					logger.Error(fmt.Sprintf("addresses from block %d: %s", blockNumber, err))
				}

				time.Sleep(w.delayBetweenErrors)
				continue
			}
			for address := range addresses {
				w.walletsChan <- address
			}

			atomic.AddInt64(&counter, 1)
			if time.Now().Add(-w.statsEvery).After(lastStatsAt) && statsMx.TryLock() {
				w.writeStats(ctx, atomic.LoadInt64(&counter), blockNumber, newBlockName, logger)
				statsMx.Unlock()
				lastStatsAt = time.Now()
			}
			blockNumber++
		}
	}
}

func (w *Parser) ParsePast(ctx context.Context) error {
	var (
		counter     int64
		statsMx     = sync.Mutex{}
		lastStatsAt = time.Now()
		err         error
	)

	logger := slog.With("network", w.network, "operation", pastBlockName)
	logger.Info("start parsing old blocks")
	blockNumber, err := w.getBlockNumber(ctx, w.network, pastBlockName)
	if err != nil {
		return fmt.Errorf("get block number: %w", err)
	}

	blockCurrentChan := make(chan int, w.maxGoroutinesNumber*2)
	// if there are problems with RPC, we will return the block to the queue.
	blockDeadQueueChan := make(chan int, w.maxGoroutinesNumber*100)
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()

		for b := blockNumber; b >= 1; b-- {
			select {
			case <-ctx.Done():
				return
			case blockCurrentChan <- b:
			}
		}
	}()

	for g := 0; g < w.maxGoroutinesNumber; g++ {
		wg.Add(1)
		go func(logger *slog.Logger) {
			defer wg.Done()
			logger = logger.With("worker", g)

			parseAddresses := func(lastBlock int) {
				addresses, err := w.addressesFromBlock(lastBlock)
				if err != nil {
					if !errors.Is(err, errBlockIsNil) {
						logger.Error(fmt.Sprintf("addresses from block %d: %s", lastBlock, err))
						return
					}

					select {
					case blockDeadQueueChan <- lastBlock:
					default:
						logger.Error("blockDeadQueueChan is full")
					}
					time.Sleep(w.delayBetweenErrors)
					return
				}

				for address := range addresses {
					w.walletsChan <- address
				}

				atomic.AddInt64(&counter, 1)
				if time.Now().Add(-w.statsEvery).After(lastStatsAt) && statsMx.TryLock() {
					w.writeStats(ctx, atomic.LoadInt64(&counter), lastBlock, pastBlockName, logger)
					statsMx.Unlock()
					lastStatsAt = time.Now()
				}
			}

			for {
				select {
				case <-ctx.Done():
					return
				// priority for this queue.
				case b, ok := <-blockDeadQueueChan:
					if !ok {
						return
					}
					parseAddresses(b)
				default:
					select {
					case <-ctx.Done():
						return
					case b, ok := <-blockCurrentChan:
						if !ok {
							return
						}
						parseAddresses(b)
					}
				}
			}
		}(logger)
	}

	wg.Wait()

	return nil
}

func (w *Parser) addressesFromBlock(block int) (map[string]struct{}, error) {
	addresses := map[string]struct{}{}
	c := w.randomClient()
	b, err := c.EthGetBlockByNumber(block, true)
	if err != nil {
		return nil, fmt.Errorf("block by number %d %s: %w", block, c.name, err)
	}
	if b == nil {
		return nil, errBlockIsNil
	}

	for _, tx := range b.Transactions {
		addresses[tx.From] = struct{}{}
		addresses[tx.To] = struct{}{}
	}

	return addresses, nil
}

func (w *Parser) getBlockNumber(ctx context.Context, network, operation string) (int, error) {
	blockNumber, err := w.blocksRepository.LastBlock(ctx, network, operation)
	if err != nil {
		c := w.randomClient()
		blockNumber, err = c.EthBlockNumber()
		if err != nil {
			return 0, fmt.Errorf("current block number %s: %w", c.name, err)
		}
	}
	return blockNumber, nil
}

func (w *Parser) writeStats(
	ctx context.Context,
	counter int64,
	lastBlock int,
	operation string,
	logger *slog.Logger,
) {
	logger.Info(
		"blocks added",
		"counter", counter,
		"blocks/sec", counter/int64(time.Since(w.startedAt).Seconds()),
		"last_block", lastBlock,
	)
	err := w.blocksRepository.AddLastBlock(ctx, lastBlock, w.network, operation)
	if err != nil {
		logger.Error(fmt.Sprintf("add last block %d for %s_%s: %s", lastBlock, w.network, operation, err))
	}
}
