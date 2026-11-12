USE ecommerce_dw;

DROP TABLE IF EXISTS stg_orders;
DROP TABLE IF EXISTS customers;
DROP TABLE IF EXISTS products;

CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    name        VARCHAR(100),
    city        VARCHAR(100)
);

CREATE TABLE products (
    product_id   INT PRIMARY KEY,
    product_name VARCHAR(100),
    category     VARCHAR(50)
);

-- Tabla de paso (staging): sin llaves ni restricciones,
-- porque recibe los datos crudos, incluidos los defectuosos.
CREATE TABLE stg_orders (
    order_id    INT,
    customer_id INT NULL,
    product_id  INT,
    amount      DECIMAL(10,2),
    order_date  VARCHAR(20)
);