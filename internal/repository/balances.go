package repository

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type BalancesRepository struct {
	db *pgxpool.Pool
}

func NewBalancesRepository(
	db *pgxpool.Pool,
) *BalancesRepository {
	return &BalancesRepository{
		db: db,
	}
}

func (r *BalancesRepository) AddAddresses(ctx context.Context, addresses []string) error {
	b := &pgx.Batch{}
	for _, address := range addresses {
		b.Queue(
			`INSERT INTO wallets (address) VALUES ($1) ON CONFLICT (address) DO NOTHING`,
			address,
		)
	}

	results := r.db.SendBatch(ctx, b)
	defer results.Close()

	return nil
}
