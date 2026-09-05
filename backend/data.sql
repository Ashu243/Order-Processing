CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INT NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING',
    total_amount NUMERIC(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (user_id)
        REFERENCES users(id)
);

CREATE TABLE outbox_orders (
    id SERIAL PRIMARY KEY,
    order_id INT NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'TO_BE_PUBLISHED',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    event_type VARCHAR(50) NOT NULL DEFAULT 'ORDER_CREATED',

    FOREIGN KEY (order_id)
    REFERENCES orders(id)
);


CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    price NUMERIC(10,2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE inventory (
    product_id INT PRIMARY KEY,
    quantity INT NOT NULL DEFAULT 0,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (product_id)
        REFERENCES products(id)
);

CREATE TABLE order_items (
    id SERIAL PRIMARY KEY,
    order_id INT NOT NULL,
    product_id INT NOT NULL,
    quantity INT NOT NULL,
    price NUMERIC(10,2) NOT NULL,

    FOREIGN KEY (order_id)
        REFERENCES orders(id),

    FOREIGN KEY (product_id)
        REFERENCES products(id)
);


INSERT INTO users (name, email) VALUES
('Ashu Verma', 'ashu@example.com'),
('Rahul Sharma', 'rahul@example.com'),
('Priya Singh', 'priya@example.com'),
('Aman Gupta', 'aman@example.com'),
('Neha Kapoor', 'neha@example.com');

INSERT INTO products (name, price) VALUES
('Mechanical Keyboard', 2499.00),
('Wireless Mouse', 999.00),
('USB-C Hub', 1499.00),
('27-inch Monitor', 12999.00),
('Laptop Stand', 1899.00),
('Webcam', 3499.00),
('Headphones', 2999.00),
('USB-C Cable', 499.00),
('Gaming Mouse Pad', 799.00),
('Bluetooth Speaker', 2499.00);

INSERT INTO inventory (product_id, quantity) VALUES
(1, 50),
(2, 100),
(3, 40),
(4, 15),
(5, 30),
(6, 25),
(7, 60),
(8, 200),
(9, 75),
(10, 35);

INSERT INTO orders (user_id, status, total_amount)
VALUES (1, 'PENDING', 4497.00)
RETURNING *;

INSERT INTO order_items
(order_id, product_id, quantity, price)
VALUES
(1, 1, 1, 2499.00),
(1, 2, 2, 999.00);

INSERT INTO orders (user_id, status, total_amount)
VALUES (2, 'CONFIRMED', 16397.00)
RETURNING *;

INSERT INTO order_items
(order_id, product_id, quantity, price)
VALUES
(3, 4, 1, 12999.00),
(3, 3, 1, 1499.00),
(3, 5, 1, 1899.00);

CREATE TABLE processed_events (
    event_id VARCHAR(100) PRIMARY KEY,
    processed_at TIMESTAMP DEFAULT NOW()
);