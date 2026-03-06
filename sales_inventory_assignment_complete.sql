-- CREATE schema assignment
CREATE SCHEMA assignment;

-- CREATE Customers table in the assignment schema
CREATE TABLE assignment.customers (
    customer_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone_number VARCHAR(50),
    registration_date DATE,
    membership_status VARCHAR(10)
);
-- CREATE Products table in the assignment schema
CREATE TABLE assignment.products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10, 2),
    supplier VARCHAR(100),
    stock_quantity INT
);
-- CREATE Sales table in the assignment schema
CREATE TABLE assignment.sales (
    sale_id INT PRIMARY KEY,
    customer_id INT,
    product_id INT,
    quantity_sold INT,
    sale_date DATE,
    total_amount DECIMAL(10, 2),
    FOREIGN KEY (customer_id) REFERENCES assignment.customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES assignment.products(product_id)
);

-- CREATE Inventory table in the assignment schema
CREATE TABLE assignment.inventory (
    product_id INT PRIMARY KEY,
    stock_quantity INT,
    FOREIGN KEY (product_id) REFERENCES assignment.products(product_id)
);

-- Inserting data into assignment.Customers table
INSERT INTO assignment.Customers 
(customer_id, first_name, last_name, email, phone_number, registration_date, membership_status) 
VALUES
(1, 'Karen', 'Molina', 'gonzalezkimberly@glass.com', '(728)697-1206', '2020-08-27', 'Bronze'),
(2, 'Elizabeth', 'Archer', 'tramirez@gmail.com', '778.104.6553', '2023-08-28', 'Silver'),
(3, 'Roberta', 'Massey', 'davislori@gmail.com', '+1-365-606-7458x399', '2024-06-12', 'Bronze'),
(4, 'Jacob', 'Adams', 'andrew72@hotmail.com', '246-459-1425x462', '2023-02-10', 'Gold'),
(5, 'Cynthia', 'Lowery', 'suarezkiara@ramsey.com', '001-279-688-8177x4015', '2020-11-13', 'Silver'),
(6, 'Emily', 'King', 'igoodwin@howard.com', '(931)575-5422x5900', '2021-05-01', 'Silver'),
(7, 'Linda', 'Larsen', 'pware@yahoo.com', '289-050-2028x7673', '2021-08-20', 'Silver'),
(8, 'Angela', 'Hanson', 'zanderson@gmail.com', '+1-403-917-3585', '2023-03-17', 'Bronze'),
(9, 'Whitney', 'Wilson', 'norma70@yahoo.com', '001-594-317-6656', '2024-01-27', 'Bronze'),
(10, 'Angela', 'Atkins', 'burnsjorge@medina.org', '344.217.5788', '2025-02-05', 'Silver'),
(11, 'Gary', 'Lucero', 'ssnyder@hotmail.com', '001-842-595-7853', '2024-10-08', 'Silver'),
(12, 'Matthew', 'Romero', 'jennifer22@gmail.com', '556.328.91896', '2022-04-07', 'Bronze'),
(13, 'Ronald', 'Thompson', 'hramos@hayes.biz', '298-487-2483', '2023-07-31', 'Bronze'),
(14, 'Suzanne', 'Anderson', 'michaelcole@ruiz-ware.com', '+1-018-029-7257', '2023-11-02', 'Bronze'),
(15, 'Mary', 'Kelly', 'matthewmurphy@gmail.com', '(845)934-9x286', '2021-01-20', 'Bronze'),
(16, 'John', 'George', 'burnettlauren@gmail.com', '+1-708-200-4286', '2022-05-17', 'Bronze'),
(17, 'James', 'Rodriguez', 'brownbrian@blair-sanford.com', '8826047658', '2022-11-25', 'Gold'),
(18, 'Steven', 'Burnett', 'zblackburn@yahoo.com', '(055)912-6726x1246', '2020-01-28', 'Gold'),
(19, 'Jonathan', 'White', 'millsseth@choi-kelly.org', '755-979-1934x772', '2022-02-06', 'Bronze'),
(20, 'Christopher', 'Santiago', 'heidimaddox@hotmail.com', '118-589-6973x058', '2021-10-16', 'Silver'),
(21, 'John', 'Diaz', 'gsmith@hotmail.com', '369.915.4337', '2022-09-17', 'Gold'),
(22, 'Curtis', 'Rose', 'ryanmartinez@moore.com', '(921)461-2128', '2021-12-14', 'Bronze'),
(23, 'Charles', 'Hughes', 'jonesangela@frank-lynn.com', '(152)603-5387x8994', '2024-07-29', 'Silver'),
(24, 'Sarah', 'Cooke', 'whitedennis@tucker.org', '(641)830-6756x56741', '2024-12-15', 'Bronze'),
(25, 'Luis', 'Harrison', 'melvin70@gmail.com', '516.509.9493', '2021-08-19', 'Silver'),
(26, 'Annette', 'Greene', 'aaron68@hall.com', '(733)734-1847x1078', '2025-04-12', 'Bronze'),
(27, 'Melissa', 'Jacobson', 'becklarry@gmail.com', '562-245-7784x4729', '2023-04-28', 'Bronze'),
(28, 'Julie', 'Gardner', 'adamsrodney@hall.com', '+1-014-029-3206x188', '2024-03-31', 'Gold'),
(29, 'Margaret', 'Taylor', 'lfuller@hotmail.com', '(299)340-8900x297', '2021-09-06', 'Bronze'),
(30, 'Erika', 'Mckee', 'wsmith@gmail.com', '(160)040-7321', '2021-05-25', 'Silver'),
(31, 'Donna', 'Whitney', 'justinnicholson@gmail.com', '7086491657', '2022-08-07', 'Gold'),
(32, 'Kristina', 'Wade', 'ashley30@richards-young.com', '603-604-2831x303', '2024-03-16', 'Silver'),
(33, 'Joshua', 'Green', 'ihartman@yahoo.com', '988-232-8285x00933', '2024-05-14', 'Silver'),
(34, 'John', 'Leblanc', 'herickson@green.info', '229.016.2527x20209', '2022-12-24', 'Silver'),
(35, 'Nicholas', 'Campbell', 'ghernandez@hotmail.com', '(982)215-6626', '2022-06-06', 'Gold'),
(36, 'Christopher', 'Hicks', 'ryan48@gmail.com', '884.881.7758', '2021-04-03', 'Silver'),
(37, 'Craig', 'Miller', 'scampbell@johnson.net', '390-328-7286x021', '2024-04-30', 'Silver'),
(38, 'Jennifer', 'Bailey', 'dwright@hotmail.com', '001-992-011-9250', '2022-09-07', 'Silver'),
(39, 'Emma', 'Davis', 'lisalester@hotmail.com', '911.706.3025', '2021-06-04', 'Gold'),
(40, 'Michael', 'Wilson', 'lmerritt@wallace-wang.com', '462.021.3233', '2025-01-14', 'Bronze'),
(41, 'Sarah', 'Church', 'deniseramos@gmail.com', '(840)285-3653x61868', '2021-03-14', 'Silver'),
(42, 'Carolyn', 'Stevenson', 'george62@garrison.net', '040.179.1155', '2024-07-26', 'Silver'),
(43, 'Sarah', 'Cole', 'amandamartin@hotmail.com', '481-651-5206x4800', '2024-07-27', 'Silver'),
(44, 'Jeremiah', 'Lozano', 'bethany38@lopez.net', '846-327-7426', '2023-01-02', 'Bronze'),
(45, 'Leslie', 'Boyd', 'cartermorgan@scott-franco.com', '+1-583-786-3525', '2022-10-22', 'Silver'),
(46, 'Carrie', 'Anderson', 'stevenlivingston@yahoo.com', '+1-086-709-5530x6149', '2024-08-23', 'Gold'),
(47, 'Jared', 'Davis', 'mooretodd@cook.com', '001-069-544-8807x2397', '2022-08-29', 'Bronze'),
(48, 'James', 'Soto', 'patriciaburns@yahoo.com', '129.857.8193x421', '2023-01-27', 'Gold'),
(49, 'Cody', 'Kline', 'bradfordleslie@hotmail.com', '+1-710-706-3703x7998', '2022-06-28', 'Bronze'),
(50, 'Jennifer', 'Perkins', 'austinowens@hill.info', '762.009.1882', '2020-10-19', 'Silver');

select * from assignment.customers;

-- Inserting data into assignment.Products table
INSERT INTO assignment.Products 
(product_id, product_name, category, price, supplier, stock_quantity) 
VALUES
(1, 'Laptop', 'Electronics', 999.99, 'Dell', 50),
(2, 'Smartphone', 'Electronics', 799.99, 'Samsung', 150),
(3, 'Washing Machine', 'Appliances', 499.99, 'LG', 30),
(4, 'Headphones', 'Accessories', 199.99, 'Sony', 100),
(5, 'Refrigerator', 'Appliances', 1200.00, 'Whirlpool', 40),
(6, 'Smart TV', 'Electronics', 1500.00, 'Samsung', 20),
(7, 'Microwave', 'Appliances', 180.00, 'Panasonic', 75),
(8, 'Blender', 'Appliances', 50.00, 'Ninja', 200),
(9, 'Gaming Console', 'Electronics', 350.00, 'Sony', 60),
(10, 'Wireless Mouse', 'Accessories', 25.00, 'Logitech', 300),
(11, 'Keyboard', 'Accessories', 49.99, 'Logitech', 250),
(12, 'Monitor', 'Electronics', 250.00, 'Acer', 120),
(13, 'External Hard Drive', 'Electronics', 80.00, 'Seagate', 90),
(14, 'Tablet', 'Electronics', 400.00, 'Apple', 70),
(15, 'Smartwatch', 'Electronics', 199.99, 'Apple', 120);

select * from assignment.products;

-- Inserting data into assignment.Sales table
INSERT INTO assignment.Sales 
(sale_id, customer_id, product_id, quantity_sold, sale_date, total_amount) 
VALUES
(1, 1, 1, 1, '2023-07-15', 999.99),
(2, 2, 2, 2, '2023-08-20', 1599.98),
(3, 3, 3, 1, '2023-09-10', 499.99),
(4, 4, 4, 3, '2023-07-25', 599.97),
(5, 5, 5, 1, '2023-06-18', 1200.00),
(6, 6, 6, 1, '2023-10-05', 1500.00),
(7, 7, 7, 1, '2023-08-01', 180.00),
(8, 8, 8, 2, '2023-09-02', 100.00),
(9, 9, 9, 1, '2023-10-10', 350.00),
(10, 10, 10, 3, '2023-11-12', 75.00),
(11, 11, 11, 2, '2023-12-01', 100.00),
(12, 12, 12, 1, '2023-12-07', 250.00),
(13, 13, 13, 1, '2024-01-15', 80.00),
(14, 14, 14, 1, '2024-02-05', 400.00),
(15, 15, 15, 1, '2024-01-05', 199.99);

-- Inserting data into assignment.Inventory table
INSERT INTO assignment.inventory 
(product_id, stock_quantity) 
VALUES
(1, 50),
(2, 150),
(3, 30),
(4, 100),
(5, 40),
(6, 20),
(7, 75),
(8, 200),
(9, 60),
(10, 300),
(11, 250),
(12, 120),
(13, 90),
(14, 70),
(15, 120);

-- Select all data from assignment.Customers table
SELECT * FROM assignment.Customers;

-- Select all data from assignment.Products table
SELECT * FROM assignment.Products;

-- Select all data from assignment.Sales table
SELECT * FROM assignment.Sales;

-- Select all data from assignment.Inventory table
SELECT * FROM assignment.Inventory;


SET search_path TO assignment;

-- 1. Write a query to select all data from the `Customers` table.
SELECT * FROM customers;

-- 2. Write a query to select the total number of products from the `Products` table.
SELECT COUNT(*) AS total_products
FROM products;

-- 3. Write a query to select the product name and its price from the `Products` table where the price is greater than 500.
SELECT product_name, price
FROM products
WHERE price > 500;

-- 4. Write a query to find the average price of all products from the `Products` table.
SELECT AVG(price) AS average_price
FROM products;

-- 5. Write a query to find the total sales amount from the `Sales` table.
SELECT SUM(total_amount) AS total_sales_amount
FROM sales;

-- 6. Write a query to select distinct membership statuses from the `Customers` table.
SELECT DISTINCT membership_status
FROM customers;

-- 7. Write a query to concatenate first and last names of all customers and show the result as `full_name`.
SELECT CONCAT(first_name, ' ', last_name) AS full_name
FROM customers;

-- 8. Write a query to find all products in the `Products` table where the category is 'Electronics'.
SELECT *
FROM products
WHERE category = 'Electronics';

-- 9. Write a query to find the highest price from the `Products` table.
SELECT MAX(price) AS highest_price
FROM products;

-- 10. Write a query to count the number of sales for each product from the `Sales` table.
SELECT product_id, COUNT(*) AS number_of_sales
FROM sales
GROUP BY product_id;

-- 11. Write a query to find the total quantity sold for each product from the `Sales` table.
SELECT product_id, SUM(quantity_sold) AS total_quantity_sold
FROM sales
GROUP BY product_id;

-- 12. Write a query to find the lowest price of products in the `Products` table.
SELECT MIN(price) AS lowest_price
FROM products;

-- 13. Write a query to find customers who have purchased products with a price greater than 1000.
SELECT DISTINCT c.customer_id, c.first_name, c.last_name
FROM customers c
JOIN sales s ON c.customer_id = s.customer_id
JOIN products p ON s.product_id = p.product_id
WHERE p.price > 1000;

-- 14. Write a query to join the `Sales` and `Products` tables on product_id, and select the product name and total sales amount.
SELECT p.product_name, SUM(s.total_amount) AS total_sales_amount
FROM sales s
JOIN products p ON s.product_id = p.product_id
GROUP BY p.product_name;

-- 15. Write a query to join the `Customers` and `Sales` tables and find the total amount spent by each customer.
SELECT c.customer_id, c.first_name, c.last_name,
       SUM(s.total_amount) AS total_spent
FROM customers c
JOIN sales s ON c.customer_id = s.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name;

-- 16. Write a query to join the `Customers`, `Sales`, and `Products` tables, and show each customer's first and last name, product name, and quantity sold.
SELECT c.first_name, c.last_name, p.product_name, s.quantity_sold
FROM customers c
JOIN sales s ON c.customer_id = s.customer_id
JOIN products p ON s.product_id = p.product_id;

-- 17. Write a query to perform a self-join on the `Customers` table and find all pairs of customers who have the same membership status.
SELECT a.customer_id AS customer_1_id, a.first_name AS customer_1_first,
       b.customer_id AS customer_2_id, b.first_name AS customer_2_first,
       a.membership_status
FROM customers a
JOIN customers b
ON a.membership_status = b.membership_status
AND a.customer_id < b.customer_id;

-- 18. Write a query to join the `Sales` and `Products` tables, and calculate the total number of sales for each product.
SELECT p.product_name, COUNT(s.sale_id) AS total_sales_count
FROM sales s
JOIN products p ON s.product_id = p.product_id
GROUP BY p.product_name;

-- 19. Write a query to find the products in the `Products` table where the stock quantity is less than 10.
SELECT *
FROM products
WHERE stock_quantity < 10;

-- 20. Write a query to join the `Sales` table and the `Products` table, and find products with sales greater than 5.
SELECT p.product_name, SUM(s.quantity_sold) AS total_sold
FROM sales s
JOIN products p ON s.product_id = p.product_id
GROUP BY p.product_name
HAVING SUM(s.quantity_sold) > 5;

-- 21. Write a query to select customers who have purchased products that are either in the 'Electronics' or 'Appliances' category.
SELECT DISTINCT c.customer_id, c.first_name
FROM customers c
JOIN sales s ON c.customer_id = s.customer_id
JOIN products p ON s.product_id = p.product_id
WHERE p.category IN ('Electronics', 'Appliances');

-- 22. Write a query to calculate the total sales amount per product and group the result by product name.
SELECT p.product_name, SUM(s.total_amount) AS total_sales_amount
FROM sales s
JOIN products p ON s.product_id = p.product_id
GROUP BY p.product_name;

-- 23. Write a query to join the `Sales` table with the `Customers` table and select customers who made a purchase in the year 2023.
SELECT c.customer_id, c.first_name
FROM customers c
JOIN sales s ON c.customer_id = s.customer_id
WHERE EXTRACT(YEAR FROM s.sale_date) = 2023;

-- 24. Write a query to find the customers with the highest total sales in 2023.
SELECT c.customer_id, c.first_name,
       SUM(s.total_amount) AS total_sales
FROM customers c
JOIN sales s ON c.customer_id = s.customer_id
WHERE EXTRACT(YEAR FROM s.sale_date) = 2023
GROUP BY c.customer_id, c.first_name
ORDER BY total_sales DESC
LIMIT 1;

-- 25. Write a query to join the `Products` and `Sales` tables and select the most expensive product sold.
SELECT p.product_name, p.price
FROM products p
JOIN sales s ON p.product_id = s.product_id
ORDER BY p.price DESC
LIMIT 1;

-- 26. Write a query to find the total number of customers who have purchased products worth more than 500.
SELECT COUNT(c.customer_id) AS customer_count
FROM customers c
JOIN sales s ON c.customer_id = s.customer_id
JOIN products p ON s.product_id = p.product_id
WHERE p.price > 500;

-- 27. Write a query to join the `Products`, `Sales`, and `Customers` tables and find the total number of sales made by customers who are in the 'Gold' membership tier.
SELECT COUNT(s.sale_id) AS total_sales_gold_members
FROM customers c
JOIN sales s ON c.customer_id = s.customer_id
JOIN products p ON s.product_id = p.product_id
WHERE c.membership_status = 'Gold';

-- 28. Write a query to join the `Products` and `Inventory` tables and find all products that have low stock (less than 10).
SELECT p.product_name, i.stock_quantity
FROM products p
JOIN inventory i ON p.product_id = i.product_id
WHERE i.stock_quantity < 10;

-- 29. Write a query to find customers who have purchased more than 5 products and show the total quantity of products they have bought.
SELECT c.customer_id, c.first_name, c.last_name,
       SUM(s.quantity_sold) AS total_quantity
FROM customers c
JOIN sales s ON c.customer_id = s.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name
HAVING SUM(s.quantity_sold) > 5;

-- 30. Write a query to find the average quantity sold per product.
SELECT product_id, AVG(quantity_sold) AS avg_quantity_sold
FROM sales
GROUP BY product_id;

-- 31. Write a query to find the number of sales made in the month of December 2023.
SELECT COUNT(*) AS sales_in_dec_2023
FROM sales
WHERE EXTRACT(YEAR FROM sale_date) = 2023
  AND EXTRACT(MONTH FROM sale_date) = 12;

-- 32. Write a query to find the total amount spent by each customer in 2023 and list the customers in descending order.
SELECT c.customer_id, c.first_name,
       SUM(s.total_amount) AS total_spent
FROM customers c
JOIN sales s ON c.customer_id = s.customer_id
WHERE EXTRACT(YEAR FROM s.sale_date) = 2023
GROUP BY c.customer_id, c.first_name
ORDER BY total_spent DESC;

-- 33. Write a query to find all products that have been sold but have less than 5 units left in stock.
SELECT p.product_name, i.stock_quantity
FROM products p
JOIN inventory i ON p.product_id = i.product_id
WHERE i.stock_quantity < 5
  AND p.product_id IN (SELECT DISTINCT product_id FROM sales);

-- 34. Write a query to find the total sales for each product and order the result by the highest sales.
SELECT p.product_name, SUM(s.total_amount) AS total_sales
FROM sales s
JOIN products p ON s.product_id = p.product_id
GROUP BY p.product_name
ORDER BY total_sales DESC;

-- 35. Write a query to find all customers who bought products within 7 days of their registration date.
SELECT DISTINCT c.customer_id, c.first_name,
       c.registration_date, s.sale_date
FROM customers c
JOIN sales s ON c.customer_id = s.customer_id
WHERE s.sale_date BETWEEN c.registration_date
  AND c.registration_date + INTERVAL '7 days';

-- 36. Write a query to join the `Sales` table with the `Products` table and filter the results by products priced between 100 and 500.
SELECT p.product_name, p.price, s.quantity_sold, s.total_amount
FROM sales s
JOIN products p ON s.product_id = p.product_id
WHERE p.price BETWEEN 100 AND 500;

-- 37. Write a query to find the most frequent customer who made purchases from the `Sales` table.
SELECT c.customer_id, c.first_name,
       COUNT(s.sale_id) AS purchase_count
FROM customers c
JOIN sales s ON c.customer_id = s.customer_id
GROUP BY c.customer_id, c.first_name
ORDER BY purchase_count DESC
LIMIT 1;

-- 38. Write a query to find the total quantity of products sold per customer.
SELECT c.customer_id, c.first_name,
       SUM(s.quantity_sold) AS total_quantity_sold
FROM customers c
JOIN sales s ON c.customer_id = s.customer_id
GROUP BY c.customer_id, c.first_name;

-- 39. Write a query to find the products with the highest stock and lowest stock, and display them together in a single result set.

-- 40. Write a query to find products whose names contain the word 'Phone' and their total sales.
SELECT p.product_name, SUM(s.total_amount) AS total_sales
FROM products p
LEFT JOIN sales s ON p.product_id = s.product_id
WHERE p.product_name LIKE '%Phone%'
GROUP BY p.product_name;

-- 41. Write a query to perform an `INNER JOIN` between `Customers` and `Sales`, then display the total sales amount and the product names for customers in the 'Gold' membership status.
SELECT c.first_name, c.membership_status,
       p.product_name, SUM(s.total_amount) AS total_sales_amount
FROM customers c
INNER JOIN sales s ON c.customer_id = s.customer_id
INNER JOIN products p ON s.product_id = p.product_id
WHERE c.membership_status = 'Gold'
GROUP BY c.customer_id, c.first_name, c.membership_status;

-- 42. Write a query to find the total sales of products by category.
SELECT p.category, SUM(s.total_amount) AS total_sales
FROM products p
JOIN sales s ON p.product_id = s.product_id
GROUP BY p.category;

-- 43. Write a query to join the `Products` table with the `Sales` table, and calculate the total sales for each product, grouped by month and year.
SELECT p.product_name,
       EXTRACT(YEAR FROM s.sale_date) AS sale_year,
       EXTRACT(MONTH FROM s.sale_date) AS sale_month,
       SUM(s.total_amount) AS total_sales
FROM sales s
JOIN products p ON s.product_id = p.product_id
GROUP BY p.product_name, EXTRACT(YEAR FROM s.sale_date), EXTRACT(MONTH FROM s.sale_date)
ORDER BY sale_year, sale_month;

-- 44. Write a query to join the `Sales` and `Inventory` tables and find products that have been sold but still have stock remaining.
SELECT p.product_name, i.stock_quantity AS remaining_stock
FROM sales s
JOIN inventory i ON s.product_id = i.product_id
JOIN products p ON p.product_id = i.product_id
WHERE i.stock_quantity > 0
GROUP BY p.product_name, i.stock_quantity;

-- 45. Write a query to find the top 5 customers who have made the highest purchases.
SELECT c.customer_id, c.first_name,
       SUM(s.total_amount) AS total_purchases
FROM customers c
JOIN sales s ON c.customer_id = s.customer_id
GROUP BY c.customer_id, c.first_name
ORDER BY total_purchases DESC
LIMIT 5;

-- 46. Write a query to calculate the total number of unique products sold in 2023.
SELECT COUNT(DISTINCT product_id) AS unique_products_sold_2023
FROM sales
WHERE EXTRACT(YEAR FROM sale_date) = 2023;

-- 47. Write a query to find the products that have not been sold in the last 6 months.
SELECT p.product_name
FROM products p
WHERE p.product_id NOT IN (
    SELECT DISTINCT product_id
    FROM sales
    WHERE sale_date >= CURRENT_DATE - INTERVAL '6 months'
);

-- 48. Write a query to select the products with a price range between $200 and $800, and find the total quantity sold for each.
SELECT p.product_name, p.price, SUM(s.quantity_sold) AS total_quantity_sold
FROM products p
JOIN sales s ON p.product_id = s.product_id
WHERE p.price BETWEEN 200 AND 800
GROUP BY p.product_name, p.price;

-- 49. Write a query to find the customers who spent the most money in the year 2023.
SELECT c.customer_id, c.first_name,
       SUM(s.total_amount) AS total_spent
FROM customers c
JOIN sales s ON c.customer_id = s.customer_id
WHERE EXTRACT(YEAR FROM s.sale_date) = 2023
GROUP BY c.customer_id, c.first_name
ORDER BY total_spent DESC
LIMIT 1;

-- 50. Write a query to select the products that have been sold more than 100 times and have a price greater than 200.
SELECT p.product_name, p.price, SUM(s.quantity_sold) AS total_sold_products
FROM products p
JOIN sales s ON p.product_id = s.product_id
WHERE p.price > 200
GROUP BY p.product_name, p.price
HAVING SUM(s.quantity_sold) > 100;
