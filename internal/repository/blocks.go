package repository

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type BlocksRepository struct {
	db *pgxpool.Pool
}

func NewBlocksRepository(
	db *pgxpool.Pool,
) *BlocksRepository {
	return &BlocksRepository{
		db: db,
	}
}

func (r *BlocksRepository) AddLastBlock(ctx context.Context, blockNumber int, network, operation string) error {
	query := `
	INSERT INTO last_block (block_number, network) VALUES ($1, $2) 
	ON CONFLICT (network) DO UPDATE
    SET block_number = $1
	`

	_, err := r.db.Exec(ctx, query, blockNumber, network+"_"+operation)
	if err != nil {
		return fmt.Errorf("add last block: %w", err)
	}
	return nil
}

func (r *BlocksRepository) LastBlock(ctx context.Context, network, operation string) (int, error) {
	var blockNumber int
	query := `SELECT block_number FROM last_block WHERE network = $1`
	err := r.db.QueryRow(ctx, query, network+"_"+operation).Scan(&blockNumber)
	if err != nil {
		return 0, fmt.Errorf("last block: %w", err)
	}
	return blockNumber, nil
}
