package writer

import (
	"context"
	"fmt"
	"log/slog"
	"strings"

	"github.com/kl09/crypto-addresses-parser/internal/domain"
)

type WalletWriter struct {
	walletsRepo domain.WalletRepository
	walletsChan chan string
	batchSize   int
}

func NewWalletWriter(walletsRepo domain.WalletRepository, walletsChan chan string, batchSize int) *WalletWriter {
	return &WalletWriter{
		walletsRepo: walletsRepo,
		walletsChan: walletsChan,
		batchSize:   batchSize,
	}
}

func (w *WalletWriter) WriteWallets(ctx context.Context) {
	lastSavedBuffMaxSize := 1000000
	wallets := make(map[string]struct{}, w.batchSize)
	lastSaved := make(map[string]struct{}, 10000000)

	saveWallets := func(wallets map[string]struct{}) {
		err := w.writeWallets(context.Background(), wallets)
		if err != nil {
			slog.Error(fmt.Sprintf("add addresses: %s", err))
		}
	}

	for {
		select {
		// there is no need to check if ctx is done, because the channel is closed when the context is done.
		case wallet, ok := <-w.walletsChan:
			// channel is closed
			if !ok {
				saveWallets(wallets)
				return
			}

			_, cached := lastSaved[wallet]
			if cached {
				continue
			}
			wallets[wallet] = struct{}{}
			if len(lastSaved) <= lastSavedBuffMaxSize {
				lastSaved[wallet] = struct{}{}
			}

			if !ok || len(wallets) >= w.batchSize {
				if len(w.walletsChan)*100/cap(w.walletsChan) > 50 {
					slog.Warn("chan is half full", "chan size", len(w.walletsChan))
				}

				saveWallets(wallets)

				clear(wallets)

				if !ok {
					return
				}
			}
		}
	}
}

func (w *WalletWriter) writeWallets(ctx context.Context, wallets map[string]struct{}) error {
	walletsSl := make([]string, 0, len(wallets))
	for wallet := range wallets {
		if len(wallet) > 2 {
			// delete prefix 0x.
			walletsSl = append(walletsSl, strings.ToLower(wallet)[2:])
		}
	}

	return w.walletsRepo.AddAddresses(ctx, walletsSl)
}
