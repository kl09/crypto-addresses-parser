CREATE TABLE wallets_with_balance
(
    address bytea PRIMARY KEY
);

CREATE INDEX wallets_with_balance_address_hash ON wallets_with_balance USING hash (address);

CREATE TABLE last_block
(
    block_number bigint,
    network text,
    PRIMARY KEY (network)
);
