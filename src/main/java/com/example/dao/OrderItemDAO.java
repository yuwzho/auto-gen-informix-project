package com.example.dao;

import com.example.model.OrderItem;
import com.example.util.ConnectionManager;

import java.sql.*;

/**
 * Data Access Object for OrderItem entity
 * SQL strings 1801-2200
 */
public class OrderItemDAO {
    
    // Basic CRUD - SQL strings 1801-1810
    private static final String INSERT_ORDER_ITEM = "INSERT INTO order_items (order_item_id, order_id, product_id, quantity, unit_price, discount, total_price, created_date, modified_date) VALUES (?, ?, ?, ?, ?, ?, ?, CURRENT, CURRENT)";
    private static final String UPDATE_ORDER_ITEM = "UPDATE order_items SET order_id = ?, product_id = ?, quantity = ?, unit_price = ?, discount = ?, total_price = ?, modified_date = CURRENT WHERE order_item_id = ?";
    private static final String DELETE_ORDER_ITEM = "DELETE FROM order_items WHERE order_item_id = ?";
    private static final String SELECT_BY_ID = "SELECT order_item_id, order_id, product_id, quantity, unit_price, discount, total_price, created_date, modified_date FROM order_items WHERE order_item_id = ?";
    private static final String SELECT_ALL = "SELECT order_item_id, order_id, product_id, quantity, unit_price, discount, total_price, created_date, modified_date FROM order_items";
    
    // Search by order - SQL strings 1811-1850
    private static final String SELECT_BY_ORDER_ID = "SELECT order_item_id, order_id, product_id, quantity, unit_price, discount, total_price, created_date, modified_date FROM order_items WHERE order_id = ?";
    private static final String COUNT_BY_ORDER = "SELECT COUNT(*) FROM order_items WHERE order_id = ?";
    private static final String SUM_BY_ORDER = "SELECT SUM(total_price) FROM order_items WHERE order_id = ?";
    private static final String SELECT_BY_ORDER_AND_PRODUCT = "SELECT order_item_id, order_id, product_id, quantity, unit_price, discount, total_price, created_date, modified_date FROM order_items WHERE order_id = ? AND product_id = ?";
    private static final String DELETE_BY_ORDER = "DELETE FROM order_items WHERE order_id = ?";
    private static final String SELECT_MAX_QUANTITY_BY_ORDER = "SELECT MAX(quantity) FROM order_items WHERE order_id = ?";
    private static final String SELECT_MIN_QUANTITY_BY_ORDER = "SELECT MIN(quantity) FROM order_items WHERE order_id = ?";
    private static final String SELECT_AVG_QUANTITY_BY_ORDER = "SELECT AVG(quantity) FROM order_items WHERE order_id = ?";
    private static final String SELECT_DISTINCT_PRODUCTS_BY_ORDER = "SELECT DISTINCT product_id FROM order_items WHERE order_id = ?";
    private static final String COUNT_DISTINCT_PRODUCTS_BY_ORDER = "SELECT COUNT(DISTINCT product_id) FROM order_items WHERE order_id = ?";
    
    // Search by product - SQL strings 1851-1890
    private static final String SELECT_BY_PRODUCT_ID = "SELECT order_item_id, order_id, product_id, quantity, unit_price, discount, total_price, created_date, modified_date FROM order_items WHERE product_id = ?";
    private static final String COUNT_BY_PRODUCT = "SELECT COUNT(*) FROM order_items WHERE product_id = ?";
    private static final String SUM_QUANTITY_BY_PRODUCT = "SELECT SUM(quantity) FROM order_items WHERE product_id = ?";
    private static final String SUM_REVENUE_BY_PRODUCT = "SELECT SUM(total_price) FROM order_items WHERE product_id = ?";
    private static final String AVG_PRICE_BY_PRODUCT = "SELECT AVG(unit_price) FROM order_items WHERE product_id = ?";
    private static final String SELECT_ORDERS_WITH_PRODUCT = "SELECT DISTINCT order_id FROM order_items WHERE product_id = ?";
    private static final String COUNT_ORDERS_WITH_PRODUCT = "SELECT COUNT(DISTINCT order_id) FROM order_items WHERE product_id = ?";
    
    // Quantity queries - SQL strings 1891-1930
    private static final String SELECT_BY_QUANTITY_RANGE = "SELECT order_item_id, order_id, product_id, quantity, unit_price, discount, total_price, created_date, modified_date FROM order_items WHERE quantity BETWEEN ? AND ?";
    private static final String SELECT_BY_QUANTITY_GT = "SELECT order_item_id, order_id, product_id, quantity, unit_price, discount, total_price, created_date, modified_date FROM order_items WHERE quantity > ?";
    private static final String SELECT_BY_QUANTITY_LT = "SELECT order_item_id, order_id, product_id, quantity, unit_price, discount, total_price, created_date, modified_date FROM order_items WHERE quantity < ?";
    private static final String SELECT_HIGH_QUANTITY_ITEMS = "SELECT order_item_id, order_id, product_id, quantity, unit_price, discount, total_price, created_date, modified_date FROM order_items WHERE quantity > 10 ORDER BY quantity DESC";
    private static final String SELECT_LOW_QUANTITY_ITEMS = "SELECT order_item_id, order_id, product_id, quantity, unit_price, discount, total_price, created_date, modified_date FROM order_items WHERE quantity = 1";
    
    // Price queries - SQL strings 1931-1970
    private static final String SELECT_BY_UNIT_PRICE_RANGE = "SELECT order_item_id, order_id, product_id, quantity, unit_price, discount, total_price, created_date, modified_date FROM order_items WHERE unit_price BETWEEN ? AND ?";
    private static final String SELECT_BY_UNIT_PRICE_GT = "SELECT order_item_id, order_id, product_id, quantity, unit_price, discount, total_price, created_date, modified_date FROM order_items WHERE unit_price > ?";
    private static final String SELECT_BY_UNIT_PRICE_LT = "SELECT order_item_id, order_id, product_id, quantity, unit_price, discount, total_price, created_date, modified_date FROM order_items WHERE unit_price < ?";
    private static final String SELECT_BY_TOTAL_PRICE_RANGE = "SELECT order_item_id, order_id, product_id, quantity, unit_price, discount, total_price, created_date, modified_date FROM order_items WHERE total_price BETWEEN ? AND ?";
    private static final String SELECT_BY_TOTAL_PRICE_GT = "SELECT order_item_id, order_id, product_id, quantity, unit_price, discount, total_price, created_date, modified_date FROM order_items WHERE total_price > ?";
    private static final String SELECT_HIGH_VALUE_ITEMS = "SELECT order_item_id, order_id, product_id, quantity, unit_price, discount, total_price, created_date, modified_date FROM order_items WHERE total_price > 500 ORDER BY total_price DESC";
    
    // Discount queries - SQL strings 1971-2010
    private static final String SELECT_WITH_DISCOUNT = "SELECT order_item_id, order_id, product_id, quantity, unit_price, discount, total_price, created_date, modified_date FROM order_items WHERE discount > 0";
    private static final String SELECT_WITHOUT_DISCOUNT = "SELECT order_item_id, order_id, product_id, quantity, unit_price, discount, total_price, created_date, modified_date FROM order_items WHERE discount = 0";
    private static final String SELECT_BY_DISCOUNT_RANGE = "SELECT order_item_id, order_id, product_id, quantity, unit_price, discount, total_price, created_date, modified_date FROM order_items WHERE discount BETWEEN ? AND ?";
    private static final String SELECT_BY_DISCOUNT_GT = "SELECT order_item_id, order_id, product_id, quantity, unit_price, discount, total_price, created_date, modified_date FROM order_items WHERE discount > ?";
    private static final String SUM_DISCOUNTS = "SELECT SUM(discount * quantity) FROM order_items";
    private static final String SUM_DISCOUNTS_BY_ORDER = "SELECT SUM(discount * quantity) FROM order_items WHERE order_id = ?";
    private static final String AVG_DISCOUNT = "SELECT AVG(discount) FROM order_items WHERE discount > 0";
    private static final String MAX_DISCOUNT = "SELECT MAX(discount) FROM order_items";
    private static final String SELECT_HIGH_DISCOUNT_ITEMS = "SELECT order_item_id, order_id, product_id, quantity, unit_price, discount, total_price, created_date, modified_date FROM order_items WHERE discount > unit_price * 0.3";
    
    // Update operations - SQL strings 2011-2050
    private static final String UPDATE_QUANTITY = "UPDATE order_items SET quantity = ?, total_price = unit_price * ? - discount, modified_date = CURRENT WHERE order_item_id = ?";
    private static final String UPDATE_UNIT_PRICE = "UPDATE order_items SET unit_price = ?, total_price = ? * quantity - discount, modified_date = CURRENT WHERE order_item_id = ?";
    private static final String UPDATE_DISCOUNT = "UPDATE order_items SET discount = ?, total_price = unit_price * quantity - ?, modified_date = CURRENT WHERE order_item_id = ?";
    private static final String UPDATE_TOTAL_PRICE = "UPDATE order_items SET total_price = ?, modified_date = CURRENT WHERE order_item_id = ?";
    private static final String INCREMENT_QUANTITY = "UPDATE order_items SET quantity = quantity + ?, total_price = unit_price * (quantity + ?) - discount, modified_date = CURRENT WHERE order_item_id = ?";
    private static final String DECREMENT_QUANTITY = "UPDATE order_items SET quantity = quantity - ?, total_price = unit_price * (quantity - ?) - discount, modified_date = CURRENT WHERE order_item_id = ?";
    private static final String APPLY_DISCOUNT_PERCENT = "UPDATE order_items SET discount = unit_price * quantity * ? / 100, total_price = unit_price * quantity * (1 - ? / 100), modified_date = CURRENT WHERE order_item_id = ?";
    private static final String REMOVE_DISCOUNT = "UPDATE order_items SET discount = 0, total_price = unit_price * quantity, modified_date = CURRENT WHERE order_item_id = ?";
    
    // Aggregation queries - SQL strings 2051-2090
    private static final String COUNT_ALL = "SELECT COUNT(*) FROM order_items";
    private static final String SUM_ALL_REVENUE = "SELECT SUM(total_price) FROM order_items";
    private static final String SUM_ALL_QUANTITY = "SELECT SUM(quantity) FROM order_items";
    private static final String AVG_UNIT_PRICE = "SELECT AVG(unit_price) FROM order_items";
    private static final String AVG_QUANTITY = "SELECT AVG(quantity) FROM order_items";
    private static final String AVG_TOTAL_PRICE = "SELECT AVG(total_price) FROM order_items";
    private static final String MAX_UNIT_PRICE = "SELECT MAX(unit_price) FROM order_items";
    private static final String MAX_TOTAL_PRICE = "SELECT MAX(total_price) FROM order_items";
    private static final String MIN_UNIT_PRICE = "SELECT MIN(unit_price) FROM order_items";
    private static final String MIN_TOTAL_PRICE = "SELECT MIN(total_price) FROM order_items";
    
    // Group by queries - SQL strings 2091-2130
    private static final String GROUP_BY_PRODUCT = "SELECT product_id, COUNT(*) as order_count, SUM(quantity) as total_quantity, SUM(total_price) as total_revenue FROM order_items GROUP BY product_id";
    private static final String GROUP_BY_PRODUCT_TOP_SELLERS = "SELECT product_id, SUM(quantity) as total_sold FROM order_items GROUP BY product_id ORDER BY total_sold DESC";
    private static final String GROUP_BY_PRODUCT_TOP_REVENUE = "SELECT product_id, SUM(total_price) as total_revenue FROM order_items GROUP BY product_id ORDER BY total_revenue DESC";
    private static final String GROUP_BY_ORDER = "SELECT order_id, COUNT(*) as item_count, SUM(total_price) as order_total FROM order_items GROUP BY order_id";
    private static final String COUNT_ITEMS_PER_ORDER = "SELECT order_id, COUNT(*) as item_count FROM order_items GROUP BY order_id ORDER BY item_count DESC";
    private static final String AVG_ITEMS_PER_ORDER = "SELECT AVG(item_count) FROM (SELECT order_id, COUNT(*) as item_count FROM order_items GROUP BY order_id) t";
    
    // Having queries - SQL strings 2131-2160
    private static final String PRODUCTS_SOLD_MULTIPLE_TIMES = "SELECT product_id, COUNT(*) as times_sold FROM order_items GROUP BY product_id HAVING COUNT(*) > 1";
    private static final String PRODUCTS_HIGH_QUANTITY_SOLD = "SELECT product_id, SUM(quantity) as total_sold FROM order_items GROUP BY product_id HAVING SUM(quantity) > ?";
    private static final String PRODUCTS_HIGH_REVENUE = "SELECT product_id, SUM(total_price) as revenue FROM order_items GROUP BY product_id HAVING SUM(total_price) > ?";
    private static final String ORDERS_WITH_MULTIPLE_ITEMS = "SELECT order_id, COUNT(*) as item_count FROM order_items GROUP BY order_id HAVING COUNT(*) > 1";
    private static final String ORDERS_HIGH_VALUE = "SELECT order_id, SUM(total_price) as total FROM order_items GROUP BY order_id HAVING SUM(total_price) > ?";
    
    // Join queries - SQL strings 2161-2200
    private static final String SELECT_WITH_PRODUCT_INFO = "SELECT oi.order_item_id, oi.order_id, oi.quantity, oi.unit_price, oi.total_price, p.product_name, p.product_code FROM order_items oi INNER JOIN product p ON oi.product_id = p.product_id";
    private static final String SELECT_WITH_ORDER_INFO = "SELECT oi.order_item_id, oi.product_id, oi.quantity, oi.total_price, o.order_date, o.order_status FROM order_items oi INNER JOIN orders o ON oi.order_id = o.order_id";
    private static final String SELECT_WITH_CUSTOMER_INFO = "SELECT oi.order_item_id, oi.product_id, oi.quantity, oi.total_price, c.first_name, c.last_name, c.email FROM order_items oi INNER JOIN orders o ON oi.order_id = o.order_id INNER JOIN customer c ON o.customer_id = c.customer_id";
    private static final String SELECT_ITEMS_BY_CUSTOMER = "SELECT oi.* FROM order_items oi INNER JOIN orders o ON oi.order_id = o.order_id WHERE o.customer_id = ?";
    private static final String SELECT_POPULAR_PRODUCTS = "SELECT p.product_name, COUNT(oi.order_item_id) as times_ordered, SUM(oi.quantity) as total_quantity FROM order_items oi INNER JOIN product p ON oi.product_id = p.product_id GROUP BY p.product_name ORDER BY times_ordered DESC";
    
    public OrderItemDAO() {}
}
