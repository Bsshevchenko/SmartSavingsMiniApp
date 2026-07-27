CREATE TABLE users (
	id BIGINT NOT NULL, 
	username VARCHAR(64), 
	first_seen DATETIME NOT NULL, 
	last_seen DATETIME NOT NULL, 
	PRIMARY KEY (id)
);
CREATE TABLE currencies (
	id INTEGER NOT NULL, 
	code VARCHAR(32) NOT NULL, 
	user_id BIGINT, 
	created_at DATETIME NOT NULL, last_used_at DATETIME, 
	PRIMARY KEY (id), 
	CONSTRAINT uq_currency_user_code UNIQUE (user_id, code), 
	FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE CASCADE
);
CREATE INDEX ix_currency_user_code ON currencies (user_id, code);
CREATE TABLE categories (
	id INTEGER NOT NULL, 
	mode VARCHAR(16) NOT NULL, 
	name VARCHAR(64) NOT NULL, 
	user_id BIGINT NOT NULL, 
	created_at DATETIME NOT NULL, last_used_at DATETIME, 
	PRIMARY KEY (id), 
	CONSTRAINT ck_category_mode CHECK (mode in ('income','expense','asset')), 
	CONSTRAINT uq_category_user_mode_name UNIQUE (user_id, mode, name), 
	FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE CASCADE
);
CREATE INDEX ix_category_user_mode_name ON categories (user_id, mode, name);
CREATE TABLE entries (
	id INTEGER NOT NULL, 
	user_id BIGINT NOT NULL, 
	mode VARCHAR(16) NOT NULL, 
	amount NUMERIC(28, 10) NOT NULL, 
	currency_id INTEGER, 
	category_id INTEGER, 
	note VARCHAR(512), 
	created_at DATETIME NOT NULL, 
	updated_at DATETIME NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT ck_entry_mode CHECK (mode in ('income','expense','asset')), 
	FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE CASCADE, 
	FOREIGN KEY(currency_id) REFERENCES currencies (id) ON DELETE SET NULL, 
	FOREIGN KEY(category_id) REFERENCES categories (id) ON DELETE SET NULL
);
CREATE INDEX ix_entry_user_created_at ON entries (user_id, created_at);
CREATE TABLE capital_snapshots (
	id INTEGER NOT NULL, 
	user_id BIGINT NOT NULL, 
	snapshot_date DATE NOT NULL, 
	total_usd NUMERIC(28, 10) NOT NULL, 
	total_rub NUMERIC(28, 10) NOT NULL, 
	created_at DATETIME NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT uq_capital_snapshot_user_date UNIQUE (user_id, snapshot_date), 
	FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE CASCADE
);
CREATE INDEX ix_capital_snapshot_user_date ON capital_snapshots (user_id, snapshot_date);
CREATE TABLE currency_rates (
	id INTEGER NOT NULL, 
	currency_code VARCHAR(10) NOT NULL, 
	rate_date DATE NOT NULL, 
	rate_to_usd NUMERIC(20, 10) NOT NULL, 
	source VARCHAR(50) NOT NULL, 
	created_at DATETIME NOT NULL, 
	PRIMARY KEY (id), 
	CONSTRAINT uq_currency_rate_code_date UNIQUE (currency_code, rate_date)
);
CREATE INDEX ix_currency_rate_code_date ON currency_rates (currency_code, rate_date);
CREATE TABLE asset_latest_values (
	id INTEGER NOT NULL, 
	user_id BIGINT NOT NULL, 
	currency_code VARCHAR(10) NOT NULL, 
	category_name VARCHAR(64), 
	amount NUMERIC(28, 10) NOT NULL, 
	last_updated DATETIME NOT NULL, 
	entry_id INTEGER, 
	PRIMARY KEY (id), 
	CONSTRAINT uq_asset_latest_user_currency_category UNIQUE (user_id, currency_code, category_name), 
	FOREIGN KEY(user_id) REFERENCES users (id) ON DELETE CASCADE, 
	FOREIGN KEY(entry_id) REFERENCES entries (id) ON DELETE SET NULL
);
CREATE INDEX ix_asset_latest_user_currency_category ON asset_latest_values (user_id, currency_code, category_name);
CREATE TABLE realty (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    address TEXT NOT NULL,
    area_m2 REAL NOT NULL,
    currency TEXT NOT NULL DEFAULT 'RUB'
);
CREATE INDEX ix_realty_user ON realty(user_id);
CREATE TABLE realty_rates (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    realty_id   INTEGER NOT NULL REFERENCES realty(id) ON DELETE CASCADE,
    price_per_m2 REAL NOT NULL,
    rate_date   TEXT NOT NULL,
    source      TEXT,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX ix_realty_rates_realty ON realty_rates(realty_id);
