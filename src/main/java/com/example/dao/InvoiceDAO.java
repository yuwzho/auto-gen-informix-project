package com.example.dao;

import com.example.model.Invoice;
import com.example.util.ConnectionManager;

import java.sql.*;

/**
 * Data Access Object for Invoice entity
 * SQL strings 2601-3000
 */
public class InvoiceDAO {
    
    // Basic CRUD - SQL strings 2601-2610
    private static final String INSERT_INVOICE = "INSERT INTO invoice (invoice_id, order_id, customer_id, invoice_number, invoice_date, due_date, subtotal, tax_amount, total_amount, paid_amount, payment_status, notes, created_date, modified_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT, CURRENT)";
    private static final String UPDATE_INVOICE = "UPDATE invoice SET order_id = ?, customer_id = ?, invoice_number = ?, invoice_date = ?, due_date = ?, subtotal = ?, tax_amount = ?, total_amount = ?, paid_amount = ?, payment_status = ?, notes = ?, modified_date = CURRENT WHERE invoice_id = ?";
    private static final String DELETE_INVOICE = "DELETE FROM invoice WHERE invoice_id = ?";
    private static final String SELECT_BY_ID = "SELECT invoice_id, order_id, customer_id, invoice_number, invoice_date, due_date, subtotal, tax_amount, total_amount, paid_amount, payment_status, notes, created_date, modified_date FROM invoice WHERE invoice_id = ?";
    private static final String SELECT_ALL = "SELECT invoice_id, order_id, customer_id, invoice_number, invoice_date, due_date, subtotal, tax_amount, total_amount, paid_amount, payment_status, notes, created_date, modified_date FROM invoice";
    
    // Search queries - SQL strings 2611-2650
    private static final String SELECT_BY_INVOICE_NUMBER = "SELECT invoice_id, order_id, customer_id, invoice_number, invoice_date, due_date, subtotal, tax_amount, total_amount, paid_amount, payment_status, notes, created_date, modified_date FROM invoice WHERE invoice_number = ?";
    private static final String SELECT_BY_ORDER_ID = "SELECT invoice_id, order_id, customer_id, invoice_number, invoice_date, due_date, subtotal, tax_amount, total_amount, paid_amount, payment_status, notes, created_date, modified_date FROM invoice WHERE order_id = ?";
    private static final String SELECT_BY_CUSTOMER_ID = "SELECT invoice_id, order_id, customer_id, invoice_number, invoice_date, due_date, subtotal, tax_amount, total_amount, paid_amount, payment_status, notes, created_date, modified_date FROM invoice WHERE customer_id = ?";
    private static final String SELECT_BY_PAYMENT_STATUS = "SELECT invoice_id, order_id, customer_id, invoice_number, invoice_date, due_date, subtotal, tax_amount, total_amount, paid_amount, payment_status, notes, created_date, modified_date FROM invoice WHERE payment_status = ?";
    private static final String SELECT_PAID_INVOICES = "SELECT invoice_id, order_id, customer_id, invoice_number, invoice_date, due_date, subtotal, tax_amount, total_amount, paid_amount, payment_status, notes, created_date, modified_date FROM invoice WHERE payment_status = 'PAID'";
    private static final String SELECT_UNPAID_INVOICES = "SELECT invoice_id, order_id, customer_id, invoice_number, invoice_date, due_date, subtotal, tax_amount, total_amount, paid_amount, payment_status, notes, created_date, modified_date FROM invoice WHERE payment_status = 'UNPAID'";
    private static final String SELECT_PARTIAL_PAID_INVOICES = "SELECT invoice_id, order_id, customer_id, invoice_number, invoice_date, due_date, subtotal, tax_amount, total_amount, paid_amount, payment_status, notes, created_date, modified_date FROM invoice WHERE payment_status = 'PARTIAL'";
    private static final String SELECT_OVERDUE_INVOICES = "SELECT invoice_id, order_id, customer_id, invoice_number, invoice_date, due_date, subtotal, tax_amount, total_amount, paid_amount, payment_status, notes, created_date, modified_date FROM invoice WHERE payment_status != 'PAID' AND due_date < CURRENT";
    
    // Date range queries - SQL strings 2651-2690
    private static final String SELECT_BY_INVOICE_DATE_RANGE = "SELECT invoice_id, order_id, customer_id, invoice_number, invoice_date, due_date, subtotal, tax_amount, total_amount, paid_amount, payment_status, notes, created_date, modified_date FROM invoice WHERE invoice_date BETWEEN ? AND ?";
    private static final String SELECT_BY_DUE_DATE_RANGE = "SELECT invoice_id, order_id, customer_id, invoice_number, invoice_date, due_date, subtotal, tax_amount, total_amount, paid_amount, payment_status, notes, created_date, modified_date FROM invoice WHERE due_date BETWEEN ? AND ?";
    private static final String SELECT_INVOICES_THIS_MONTH = "SELECT invoice_id, order_id, customer_id, invoice_number, invoice_date, due_date, subtotal, tax_amount, total_amount, paid_amount, payment_status, notes, created_date, modified_date FROM invoice WHERE MONTH(invoice_date) = MONTH(CURRENT) AND YEAR(invoice_date) = YEAR(CURRENT)";
    private static final String SELECT_INVOICES_THIS_YEAR = "SELECT invoice_id, order_id, customer_id, invoice_number, invoice_date, due_date, subtotal, tax_amount, total_amount, paid_amount, payment_status, notes, created_date, modified_date FROM invoice WHERE YEAR(invoice_date) = YEAR(CURRENT)";
    private static final String SELECT_DUE_THIS_WEEK = "SELECT invoice_id, order_id, customer_id, invoice_number, invoice_date, due_date, subtotal, tax_amount, total_amount, paid_amount, payment_status, notes, created_date, modified_date FROM invoice WHERE due_date BETWEEN CURRENT AND CURRENT + 7 UNITS DAY";
    private static final String SELECT_DUE_THIS_MONTH = "SELECT invoice_id, order_id, customer_id, invoice_number, invoice_date, due_date, subtotal, tax_amount, total_amount, paid_amount, payment_status, notes, created_date, modified_date FROM invoice WHERE MONTH(due_date) = MONTH(CURRENT) AND YEAR(due_date) = YEAR(CURRENT)";
    
    // Amount queries - SQL strings 2691-2730
    private static final String SELECT_BY_TOTAL_RANGE = "SELECT invoice_id, order_id, customer_id, invoice_number, invoice_date, due_date, subtotal, tax_amount, total_amount, paid_amount, payment_status, notes, created_date, modified_date FROM invoice WHERE total_amount BETWEEN ? AND ?";
    private static final String SELECT_BY_TOTAL_GT = "SELECT invoice_id, order_id, customer_id, invoice_number, invoice_date, due_date, subtotal, tax_amount, total_amount, paid_amount, payment_status, notes, created_date, modified_date FROM invoice WHERE total_amount > ?";
    private static final String SELECT_BY_TOTAL_LT = "SELECT invoice_id, order_id, customer_id, invoice_number, invoice_date, due_date, subtotal, tax_amount, total_amount, paid_amount, payment_status, notes, created_date, modified_date FROM invoice WHERE total_amount < ?";
    private static final String SELECT_HIGH_VALUE_INVOICES = "SELECT invoice_id, order_id, customer_id, invoice_number, invoice_date, due_date, subtotal, tax_amount, total_amount, paid_amount, payment_status, notes, created_date, modified_date FROM invoice WHERE total_amount > 1000 ORDER BY total_amount DESC";
    private static final String SELECT_BY_OUTSTANDING_BALANCE = "SELECT invoice_id, order_id, customer_id, invoice_number, invoice_date, due_date, subtotal, tax_amount, total_amount, paid_amount, (total_amount - paid_amount) as balance, payment_status, notes, created_date, modified_date FROM invoice WHERE total_amount > paid_amount";
    
    // Update operations - SQL strings 2731-2770
    private static final String UPDATE_PAYMENT_STATUS = "UPDATE invoice SET payment_status = ?, modified_date = CURRENT WHERE invoice_id = ?";
    private static final String UPDATE_PAID_AMOUNT = "UPDATE invoice SET paid_amount = ?, modified_date = CURRENT WHERE invoice_id = ?";
    private static final String UPDATE_DUE_DATE = "UPDATE invoice SET due_date = ?, modified_date = CURRENT WHERE invoice_id = ?";
    private static final String UPDATE_NOTES = "UPDATE invoice SET notes = ?, modified_date = CURRENT WHERE invoice_id = ?";
    private static final String MARK_AS_PAID = "UPDATE invoice SET payment_status = 'PAID', paid_amount = total_amount, modified_date = CURRENT WHERE invoice_id = ?";
    private static final String MARK_AS_UNPAID = "UPDATE invoice SET payment_status = 'UNPAID', paid_amount = 0, modified_date = CURRENT WHERE invoice_id = ?";
    private static final String ADD_PAYMENT = "UPDATE invoice SET paid_amount = paid_amount + ?, payment_status = CASE WHEN paid_amount + ? >= total_amount THEN 'PAID' ELSE 'PARTIAL' END, modified_date = CURRENT WHERE invoice_id = ?";
    private static final String VOID_INVOICE = "UPDATE invoice SET payment_status = 'VOID', modified_date = CURRENT WHERE invoice_id = ?";
    
    // Aggregation queries - SQL strings 2771-2810
    private static final String COUNT_ALL = "SELECT COUNT(*) FROM invoice";
    private static final String COUNT_BY_STATUS = "SELECT COUNT(*) FROM invoice WHERE payment_status = ?";
    private static final String COUNT_PAID = "SELECT COUNT(*) FROM invoice WHERE payment_status = 'PAID'";
    private static final String COUNT_UNPAID = "SELECT COUNT(*) FROM invoice WHERE payment_status = 'UNPAID'";
    private static final String COUNT_OVERDUE = "SELECT COUNT(*) FROM invoice WHERE payment_status != 'PAID' AND due_date < CURRENT";
    private static final String SUM_ALL_INVOICES = "SELECT SUM(total_amount) FROM invoice";
    private static final String SUM_PAID_INVOICES = "SELECT SUM(total_amount) FROM invoice WHERE payment_status = 'PAID'";
    private static final String SUM_UNPAID_INVOICES = "SELECT SUM(total_amount) FROM invoice WHERE payment_status = 'UNPAID'";
    private static final String SUM_OUTSTANDING_BALANCE = "SELECT SUM(total_amount - paid_amount) FROM invoice WHERE payment_status != 'PAID'";
    private static final String SUM_BY_CUSTOMER = "SELECT SUM(total_amount) FROM invoice WHERE customer_id = ?";
    private static final String SUM_UNPAID_BY_CUSTOMER = "SELECT SUM(total_amount - paid_amount) FROM invoice WHERE customer_id = ? AND payment_status != 'PAID'";
    private static final String AVG_INVOICE_AMOUNT = "SELECT AVG(total_amount) FROM invoice";
    private static final String MAX_INVOICE_AMOUNT = "SELECT MAX(total_amount) FROM invoice";
    private static final String MIN_INVOICE_AMOUNT = "SELECT MIN(total_amount) FROM invoice";
    
    // Group by queries - SQL strings 2811-2850
    private static final String GROUP_BY_STATUS = "SELECT payment_status, COUNT(*) as count, SUM(total_amount) as total FROM invoice GROUP BY payment_status";
    private static final String GROUP_BY_CUSTOMER = "SELECT customer_id, COUNT(*) as invoice_count, SUM(total_amount) as total_billed, SUM(paid_amount) as total_paid FROM invoice GROUP BY customer_id";
    private static final String GROUP_BY_MONTH = "SELECT YEAR(invoice_date) as year, MONTH(invoice_date) as month, COUNT(*) as count, SUM(total_amount) as total FROM invoice GROUP BY year, month ORDER BY year DESC, month DESC";
    private static final String REVENUE_BY_MONTH = "SELECT YEAR(invoice_date) as year, MONTH(invoice_date) as month, SUM(total_amount) as revenue FROM invoice WHERE payment_status = 'PAID' GROUP BY year, month ORDER BY year DESC, month DESC";
    private static final String OUTSTANDING_BY_CUSTOMER = "SELECT customer_id, SUM(total_amount - paid_amount) as outstanding FROM invoice WHERE payment_status != 'PAID' GROUP BY customer_id ORDER BY outstanding DESC";
    
    // Having queries - SQL strings 2851-2880
    private static final String CUSTOMERS_HIGH_OUTSTANDING = "SELECT customer_id, SUM(total_amount - paid_amount) as balance FROM invoice WHERE payment_status != 'PAID' GROUP BY customer_id HAVING SUM(total_amount - paid_amount) > ?";
    private static final String CUSTOMERS_MANY_INVOICES = "SELECT customer_id, COUNT(*) as invoice_count FROM invoice GROUP BY customer_id HAVING COUNT(*) > ?";
    
    // Ordering queries - SQL strings 2881-2910
    private static final String SELECT_ALL_ORDER_BY_DATE = "SELECT invoice_id, order_id, customer_id, invoice_number, invoice_date, due_date, subtotal, tax_amount, total_amount, paid_amount, payment_status, notes, created_date, modified_date FROM invoice ORDER BY invoice_date DESC";
    private static final String SELECT_ALL_ORDER_BY_DUE_DATE = "SELECT invoice_id, order_id, customer_id, invoice_number, invoice_date, due_date, subtotal, tax_amount, total_amount, paid_amount, payment_status, notes, created_date, modified_date FROM invoice ORDER BY due_date";
    private static final String SELECT_ALL_ORDER_BY_AMOUNT = "SELECT invoice_id, order_id, customer_id, invoice_number, invoice_date, due_date, subtotal, tax_amount, total_amount, paid_amount, payment_status, notes, created_date, modified_date FROM invoice ORDER BY total_amount DESC";
    private static final String SELECT_ALL_ORDER_BY_NUMBER = "SELECT invoice_id, order_id, customer_id, invoice_number, invoice_date, due_date, subtotal, tax_amount, total_amount, paid_amount, payment_status, notes, created_date, modified_date FROM invoice ORDER BY invoice_number";
    
    // Limit queries - SQL strings 2911-2930
    private static final String SELECT_TOP_10_RECENT = "SELECT FIRST 10 invoice_id, order_id, customer_id, invoice_number, invoice_date, due_date, subtotal, tax_amount, total_amount, paid_amount, payment_status, notes, created_date, modified_date FROM invoice ORDER BY invoice_date DESC";
    private static final String SELECT_TOP_10_BY_AMOUNT = "SELECT FIRST 10 invoice_id, order_id, customer_id, invoice_number, invoice_date, due_date, subtotal, tax_amount, total_amount, paid_amount, payment_status, notes, created_date, modified_date FROM invoice ORDER BY total_amount DESC";
    private static final String SELECT_TOP_OVERDUE = "SELECT FIRST 20 invoice_id, order_id, customer_id, invoice_number, invoice_date, due_date, subtotal, tax_amount, total_amount, paid_amount, payment_status, notes, created_date, modified_date FROM invoice WHERE payment_status != 'PAID' AND due_date < CURRENT ORDER BY due_date ASC";
    
    // Complex business queries - SQL strings 2931-2970
    private static final String SELECT_AGING_REPORT = "SELECT invoice_id, customer_id, invoice_number, invoice_date, due_date, total_amount, paid_amount, (total_amount - paid_amount) as balance, (CURRENT - due_date) UNITS DAY as days_overdue FROM invoice WHERE payment_status != 'PAID' AND due_date < CURRENT ORDER BY days_overdue DESC";
    private static final String SELECT_COLLECTION_REPORT = "SELECT customer_id, COUNT(*) as overdue_count, SUM(total_amount - paid_amount) as total_overdue FROM invoice WHERE payment_status != 'PAID' AND due_date < CURRENT GROUP BY customer_id ORDER BY total_overdue DESC";
    private static final String SELECT_CASH_FLOW_PROJECTION = "SELECT DATE(due_date) as due_day, SUM(total_amount - paid_amount) as expected_payment FROM invoice WHERE payment_status != 'PAID' AND due_date >= CURRENT GROUP BY due_day ORDER BY due_day";
    private static final String SELECT_PAYMENT_HISTORY = "SELECT invoice_id, invoice_number, invoice_date, total_amount, paid_amount, payment_status, modified_date FROM invoice WHERE customer_id = ? ORDER BY invoice_date DESC";
    
    // Statistical queries - SQL strings 2971-3000
    private static final String SELECT_INVOICE_STATS = "SELECT COUNT(*) as total, SUM(total_amount) as total_billed, SUM(paid_amount) as total_paid, AVG(total_amount) as avg_invoice FROM invoice";
    private static final String SELECT_PAYMENT_RATE = "SELECT COUNT(CASE WHEN payment_status = 'PAID' THEN 1 END) * 100.0 / COUNT(*) as payment_rate FROM invoice";
    private static final String SELECT_AVG_DAYS_TO_PAYMENT = "SELECT AVG((modified_date - invoice_date) UNITS DAY) FROM invoice WHERE payment_status = 'PAID'";
    private static final String SELECT_INVOICE_VOLUME_TREND = "SELECT DATE(invoice_date) as day, COUNT(*) as invoices, SUM(total_amount) as amount FROM invoice WHERE invoice_date > CURRENT - 30 UNITS DAY GROUP BY day ORDER BY day";
    private static final String SELECT_OVERDUE_BY_AGING = "SELECT CASE WHEN (CURRENT - due_date) UNITS DAY < 30 THEN '0-30 days' WHEN (CURRENT - due_date) UNITS DAY < 60 THEN '31-60 days' WHEN (CURRENT - due_date) UNITS DAY < 90 THEN '61-90 days' ELSE 'Over 90 days' END as aging_bucket, COUNT(*) as count, SUM(total_amount - paid_amount) as balance FROM invoice WHERE payment_status != 'PAID' AND due_date < CURRENT GROUP BY aging_bucket";
    private static final String SELECT_REVENUE_RECOGNITION = "SELECT YEAR(invoice_date) as year, MONTH(invoice_date) as month, SUM(CASE WHEN payment_status = 'PAID' THEN total_amount ELSE 0 END) as cash_basis, SUM(total_amount) as accrual_basis FROM invoice GROUP BY year, month ORDER BY year DESC, month DESC";
    private static final String SELECT_CUSTOMER_PAYMENT_BEHAVIOR = "SELECT customer_id, COUNT(*) as total_invoices, COUNT(CASE WHEN payment_status = 'PAID' THEN 1 END) as paid_invoices, AVG(CASE WHEN payment_status = 'PAID' THEN (modified_date - invoice_date) UNITS DAY END) as avg_days_to_pay FROM invoice GROUP BY customer_id";
    private static final String SELECT_TAX_COLLECTED = "SELECT SUM(tax_amount) FROM invoice WHERE payment_status = 'PAID' AND invoice_date BETWEEN ? AND ?";
    private static final String SELECT_MONTHLY_RECEIVABLES = "SELECT YEAR(invoice_date) as year, MONTH(invoice_date) as month, SUM(total_amount - paid_amount) as receivables FROM invoice WHERE payment_status != 'PAID' GROUP BY year, month ORDER BY year DESC, month DESC";
    private static final String SELECT_INVOICE_COMPLETION_RATE = "SELECT COUNT(CASE WHEN payment_status = 'PAID' THEN 1 END) * 100.0 / NULLIF(COUNT(*), 0) as completion_rate FROM invoice WHERE invoice_date BETWEEN ? AND ?";
    
    public InvoiceDAO() {}
}
