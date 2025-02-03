package domain

import "context"

type WalletRepository interface {
	AddAddresses(ctx context.Context, addresses []string) error
}

type BlocksRepository interface {
	AddLastBlock(ctx context.Context, blockNumber int, network, operation string) error
	LastBlock(ctx context.Context, network, operation string) (int, error)
}
