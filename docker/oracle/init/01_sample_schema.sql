-- gvenzl/oracle-xe runs .sql init scripts as SYS in the CDB root.
-- Switch into XEPDB1, grant SchemaCrawler-required privileges to dmf_user,
-- then set the current schema so all objects land in the right place.
ALTER SESSION SET CONTAINER = XEPDB1;

-- SELECT_CATALOG_ROLE lets SchemaCrawler read ALL_TABLES, ALL_COLUMNS, etc.
GRANT SELECT_CATALOG_ROLE TO dmf_user;

ALTER SESSION SET CURRENT_SCHEMA = DMF_USER;

CREATE TABLE customers (
    customer_id   NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    first_name    VARCHAR2(100)  NOT NULL,
    last_name     VARCHAR2(100)  NOT NULL,
    email         VARCHAR2(255)  NOT NULL UNIQUE,
    phone         VARCHAR2(20),
    created_at    TIMESTAMP      DEFAULT SYSTIMESTAMP NOT NULL
);

CREATE TABLE products (
    product_id    NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name          VARCHAR2(200)  NOT NULL,
    description   CLOB,
    price         NUMBER(12, 2)  NOT NULL,
    stock_qty     NUMBER(10)     DEFAULT 0 NOT NULL,
    created_at    TIMESTAMP      DEFAULT SYSTIMESTAMP NOT NULL
);

CREATE TABLE orders (
    order_id      NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id   NUMBER         NOT NULL,
    order_date    TIMESTAMP      DEFAULT SYSTIMESTAMP NOT NULL,
    status        VARCHAR2(20)   DEFAULT 'PENDING' NOT NULL
                  CHECK (status IN ('PENDING', 'PROCESSING', 'SHIPPED', 'DELIVERED', 'CANCELLED')),
    total_amount  NUMBER(12, 2)  NOT NULL,
    CONSTRAINT fk_orders_customer FOREIGN KEY (customer_id) REFERENCES customers (customer_id)
);

CREATE TABLE order_items (
    order_item_id NUMBER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    order_id      NUMBER         NOT NULL,
    product_id    NUMBER         NOT NULL,
    quantity      NUMBER(10)     NOT NULL CHECK (quantity > 0),
    unit_price    NUMBER(12, 2)  NOT NULL,
    CONSTRAINT fk_items_order   FOREIGN KEY (order_id)   REFERENCES orders   (order_id),
    CONSTRAINT fk_items_product FOREIGN KEY (product_id) REFERENCES products (product_id)
);

INSERT INTO customers (first_name, last_name, email, phone) VALUES ('Jane', 'Doe', 'jane.doe@example.com', '555-0101');
INSERT INTO customers (first_name, last_name, email, phone) VALUES ('John', 'Smith', 'john.smith@example.com', '555-0102');
INSERT INTO products (name, description, price, stock_qty) VALUES ('Widget A', 'Standard widget model A', 9.99, 500);
INSERT INTO products (name, description, price, stock_qty) VALUES ('Widget B', 'Premium widget model B', 24.99, 150);
INSERT INTO orders (customer_id, status, total_amount) VALUES (1, 'DELIVERED', 9.99);
INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (1, 1, 1, 9.99);
COMMIT;
