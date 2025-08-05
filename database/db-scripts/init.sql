-- Create table to store lists all available products
CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR NOT NULL,
    product_name VARCHAR NOT NULL,
    price DECIMAL DEFAULT 0,
    PRIMARY KEY (product_id)
);

-- Create table to store branch-related attributes and details
CREATE TABLE IF NOT EXISTS branches (
    branch_id VARCHAR NOT NULL,
    branch_name VARCHAR NOT NULL,
    PRIMARY KEY (branch_id)
);

-- Create table containing the different payment methods
CREATE TABLE IF NOT EXISTS payment_types (
    payment_type_id VARCHAR NOT NULL,
    payment_type VARCHAR NOT NULL,
    PRIMARY KEY (payment_type_id)
);

-- Create table to store the complete transactions history for all orders placed
CREATE TABLE IF NOT EXISTS transactions (
    order_id VARCHAR NOT NULL,
    order_datetime TIMESTAMP,
    branch_id VARCHAR,
    total_money_spent DECIMAL,
    payment_type_id VARCHAR,
    PRIMARY KEY (order_id),
    FOREIGN KEY (branch_id) REFERENCES branches(branch_id),
    FOREIGN KEY (payment_type_id) REFERENCES payment_types(payment_type_id)
);

-- Create table to captures the contents of customer baskets at the time of ordering
CREATE TABLE IF NOT EXISTS order_snapshots (
    order_snapshot_id VARCHAR NOT NULL,
    order_id VARCHAR,
    product_id VARCHAR,
    quantity INT DEFAULT 1,
    PRIMARY KEY (order_snapshot_id),
    FOREIGN KEY (order_id) REFERENCES transactions(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);