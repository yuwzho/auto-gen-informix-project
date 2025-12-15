package com.example.dao;

import com.example.model.Employee;
import com.example.util.ConnectionManager;

import java.sql.*;

/**
 * Data Access Object for Employee entity
 * SQL strings 2201-2600
 */
public class EmployeeDAO {
    
    // Basic CRUD - SQL strings 2201-2210
    private static final String INSERT_EMPLOYEE = "INSERT INTO employee (employee_id, first_name, last_name, email, phone, department, position, manager_id, hire_date, birth_date, status, address, city, state, zip_code, created_date, modified_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT, CURRENT)";
    private static final String UPDATE_EMPLOYEE = "UPDATE employee SET first_name = ?, last_name = ?, email = ?, phone = ?, department = ?, position = ?, manager_id = ?, hire_date = ?, birth_date = ?, status = ?, address = ?, city = ?, state = ?, zip_code = ?, modified_date = CURRENT WHERE employee_id = ?";
    private static final String DELETE_EMPLOYEE = "DELETE FROM employee WHERE employee_id = ?";
    private static final String SELECT_BY_ID = "SELECT employee_id, first_name, last_name, email, phone, department, position, manager_id, hire_date, birth_date, status, address, city, state, zip_code, created_date, modified_date FROM employee WHERE employee_id = ?";
    private static final String SELECT_ALL = "SELECT employee_id, first_name, last_name, email, phone, department, position, manager_id, hire_date, birth_date, status, address, city, state, zip_code, created_date, modified_date FROM employee";
    
    // Search queries - SQL strings 2211-2250
    private static final String SELECT_BY_EMAIL = "SELECT employee_id, first_name, last_name, email, phone, department, position, manager_id, hire_date, birth_date, status, address, city, state, zip_code, created_date, modified_date FROM employee WHERE email = ?";
    private static final String SELECT_BY_PHONE = "SELECT employee_id, first_name, last_name, email, phone, department, position, manager_id, hire_date, birth_date, status, address, city, state, zip_code, created_date, modified_date FROM employee WHERE phone = ?";
    private static final String SELECT_BY_FIRST_NAME = "SELECT employee_id, first_name, last_name, email, phone, department, position, manager_id, hire_date, birth_date, status, address, city, state, zip_code, created_date, modified_date FROM employee WHERE first_name = ?";
    private static final String SELECT_BY_LAST_NAME = "SELECT employee_id, first_name, last_name, email, phone, department, position, manager_id, hire_date, birth_date, status, address, city, state, zip_code, created_date, modified_date FROM employee WHERE last_name = ?";
    private static final String SELECT_BY_FULL_NAME = "SELECT employee_id, first_name, last_name, email, phone, department, position, manager_id, hire_date, birth_date, status, address, city, state, zip_code, created_date, modified_date FROM employee WHERE first_name = ? AND last_name = ?";
    private static final String SELECT_BY_DEPARTMENT = "SELECT employee_id, first_name, last_name, email, phone, department, position, manager_id, hire_date, birth_date, status, address, city, state, zip_code, created_date, modified_date FROM employee WHERE department = ?";
    private static final String SELECT_BY_POSITION = "SELECT employee_id, first_name, last_name, email, phone, department, position, manager_id, hire_date, birth_date, status, address, city, state, zip_code, created_date, modified_date FROM employee WHERE position = ?";
    private static final String SELECT_BY_STATUS = "SELECT employee_id, first_name, last_name, email, phone, department, position, manager_id, hire_date, birth_date, status, address, city, state, zip_code, created_date, modified_date FROM employee WHERE status = ?";
    private static final String SELECT_BY_MANAGER_ID = "SELECT employee_id, first_name, last_name, email, phone, department, position, manager_id, hire_date, birth_date, status, address, city, state, zip_code, created_date, modified_date FROM employee WHERE manager_id = ?";
    private static final String SELECT_BY_CITY = "SELECT employee_id, first_name, last_name, email, phone, department, position, manager_id, hire_date, birth_date, status, address, city, state, zip_code, created_date, modified_date FROM employee WHERE city = ?";
    private static final String SELECT_BY_STATE = "SELECT employee_id, first_name, last_name, email, phone, department, position, manager_id, hire_date, birth_date, status, address, city, state, zip_code, created_date, modified_date FROM employee WHERE state = ?";
    
    // Department queries - SQL strings 2251-2290
    private static final String SELECT_BY_DEPT_AND_STATUS = "SELECT employee_id, first_name, last_name, email, phone, department, position, manager_id, hire_date, birth_date, status, address, city, state, zip_code, created_date, modified_date FROM employee WHERE department = ? AND status = ?";
    private static final String SELECT_BY_DEPT_AND_POSITION = "SELECT employee_id, first_name, last_name, email, phone, department, position, manager_id, hire_date, birth_date, status, address, city, state, zip_code, created_date, modified_date FROM employee WHERE department = ? AND position = ?";
    private static final String COUNT_BY_DEPARTMENT = "SELECT COUNT(*) FROM employee WHERE department = ?";
    private static final String COUNT_BY_POSITION = "SELECT COUNT(*) FROM employee WHERE position = ?";
    private static final String COUNT_BY_STATUS = "SELECT COUNT(*) FROM employee WHERE status = ?";
    private static final String COUNT_BY_MANAGER = "SELECT COUNT(*) FROM employee WHERE manager_id = ?";
    private static final String SELECT_ACTIVE_EMPLOYEES = "SELECT employee_id, first_name, last_name, email, phone, department, position, manager_id, hire_date, birth_date, status, address, city, state, zip_code, created_date, modified_date FROM employee WHERE status = 'ACTIVE'";
    private static final String SELECT_INACTIVE_EMPLOYEES = "SELECT employee_id, first_name, last_name, email, phone, department, position, manager_id, hire_date, birth_date, status, address, city, state, zip_code, created_date, modified_date FROM employee WHERE status = 'INACTIVE'";
    private static final String SELECT_MANAGERS = "SELECT employee_id, first_name, last_name, email, phone, department, position, manager_id, hire_date, birth_date, status, address, city, state, zip_code, created_date, modified_date FROM employee WHERE position LIKE '%Manager%'";
    private static final String SELECT_WITHOUT_MANAGER = "SELECT employee_id, first_name, last_name, email, phone, department, position, manager_id, hire_date, birth_date, status, address, city, state, zip_code, created_date, modified_date FROM employee WHERE manager_id IS NULL";
    
    // Date queries - SQL strings 2291-2330
    private static final String SELECT_BY_HIRE_DATE_RANGE = "SELECT employee_id, first_name, last_name, email, phone, department, position, manager_id, hire_date, birth_date, status, address, city, state, zip_code, created_date, modified_date FROM employee WHERE hire_date BETWEEN ? AND ?";
    private static final String SELECT_HIRED_AFTER = "SELECT employee_id, first_name, last_name, email, phone, department, position, manager_id, hire_date, birth_date, status, address, city, state, zip_code, created_date, modified_date FROM employee WHERE hire_date > ?";
    private static final String SELECT_HIRED_BEFORE = "SELECT employee_id, first_name, last_name, email, phone, department, position, manager_id, hire_date, birth_date, status, address, city, state, zip_code, created_date, modified_date FROM employee WHERE hire_date < ?";
    private static final String SELECT_HIRED_THIS_YEAR = "SELECT employee_id, first_name, last_name, email, phone, department, position, manager_id, hire_date, birth_date, status, address, city, state, zip_code, created_date, modified_date FROM employee WHERE YEAR(hire_date) = YEAR(CURRENT)";
    private static final String SELECT_HIRED_THIS_MONTH = "SELECT employee_id, first_name, last_name, email, phone, department, position, manager_id, hire_date, birth_date, status, address, city, state, zip_code, created_date, modified_date FROM employee WHERE MONTH(hire_date) = MONTH(CURRENT) AND YEAR(hire_date) = YEAR(CURRENT)";
    private static final String SELECT_BY_BIRTH_DATE_RANGE = "SELECT employee_id, first_name, last_name, email, phone, department, position, manager_id, hire_date, birth_date, status, address, city, state, zip_code, created_date, modified_date FROM employee WHERE birth_date BETWEEN ? AND ?";
    private static final String SELECT_BIRTHDAYS_THIS_MONTH = "SELECT employee_id, first_name, last_name, email, phone, department, position, manager_id, hire_date, birth_date, status, address, city, state, zip_code, created_date, modified_date FROM employee WHERE MONTH(birth_date) = MONTH(CURRENT)";
    private static final String SELECT_ANNIVERSARIES_THIS_MONTH = "SELECT employee_id, first_name, last_name, email, phone, department, position, manager_id, hire_date, birth_date, status, address, city, state, zip_code, created_date, modified_date FROM employee WHERE MONTH(hire_date) = MONTH(CURRENT)";
    
    // Tenure queries - SQL strings 2331-2360
    private static final String SELECT_LONG_TENURE = "SELECT employee_id, first_name, last_name, department, hire_date, (CURRENT - hire_date) UNITS YEAR as years_employed FROM employee WHERE hire_date < CURRENT - 5 UNITS YEAR ORDER BY hire_date ASC";
    private static final String SELECT_NEW_HIRES = "SELECT employee_id, first_name, last_name, department, hire_date FROM employee WHERE hire_date > CURRENT - 90 UNITS DAY ORDER BY hire_date DESC";
    private static final String SELECT_BY_TENURE_YEARS = "SELECT employee_id, first_name, last_name, department, hire_date, (CURRENT - hire_date) UNITS YEAR as tenure FROM employee WHERE (CURRENT - hire_date) UNITS YEAR > ?";
    private static final String AVG_TENURE = "SELECT AVG((CURRENT - hire_date) UNITS YEAR) as avg_tenure_years FROM employee WHERE status = 'ACTIVE'";
    
    // Update operations - SQL strings 2361-2400
    private static final String UPDATE_FIRST_NAME = "UPDATE employee SET first_name = ?, modified_date = CURRENT WHERE employee_id = ?";
    private static final String UPDATE_LAST_NAME = "UPDATE employee SET last_name = ?, modified_date = CURRENT WHERE employee_id = ?";
    private static final String UPDATE_EMAIL = "UPDATE employee SET email = ?, modified_date = CURRENT WHERE employee_id = ?";
    private static final String UPDATE_PHONE = "UPDATE employee SET phone = ?, modified_date = CURRENT WHERE employee_id = ?";
    private static final String UPDATE_DEPARTMENT = "UPDATE employee SET department = ?, modified_date = CURRENT WHERE employee_id = ?";
    private static final String UPDATE_POSITION = "UPDATE employee SET position = ?, modified_date = CURRENT WHERE employee_id = ?";
    private static final String UPDATE_MANAGER = "UPDATE employee SET manager_id = ?, modified_date = CURRENT WHERE employee_id = ?";
    private static final String UPDATE_STATUS = "UPDATE employee SET status = ?, modified_date = CURRENT WHERE employee_id = ?";
    private static final String UPDATE_ADDRESS = "UPDATE employee SET address = ?, city = ?, state = ?, zip_code = ?, modified_date = CURRENT WHERE employee_id = ?";
    private static final String PROMOTE_EMPLOYEE = "UPDATE employee SET position = ?, modified_date = CURRENT WHERE employee_id = ?";
    private static final String TRANSFER_EMPLOYEE = "UPDATE employee SET department = ?, modified_date = CURRENT WHERE employee_id = ?";
    private static final String ACTIVATE_EMPLOYEE = "UPDATE employee SET status = 'ACTIVE', modified_date = CURRENT WHERE employee_id = ?";
    private static final String DEACTIVATE_EMPLOYEE = "UPDATE employee SET status = 'INACTIVE', modified_date = CURRENT WHERE employee_id = ?";
    
    // Aggregation queries - SQL strings 2401-2440
    private static final String COUNT_ALL = "SELECT COUNT(*) FROM employee";
    private static final String COUNT_ACTIVE = "SELECT COUNT(*) FROM employee WHERE status = 'ACTIVE'";
    private static final String COUNT_INACTIVE = "SELECT COUNT(*) FROM employee WHERE status = 'INACTIVE'";
    
    // Group by queries - SQL strings 2441-2480
    private static final String GROUP_BY_DEPARTMENT = "SELECT department, COUNT(*) as employee_count FROM employee GROUP BY department";
    private static final String GROUP_BY_POSITION = "SELECT position, COUNT(*) as employee_count FROM employee GROUP BY position";
    private static final String GROUP_BY_STATUS = "SELECT status, COUNT(*) as employee_count FROM employee GROUP BY status";
    private static final String GROUP_BY_MANAGER = "SELECT manager_id, COUNT(*) as direct_reports FROM employee WHERE manager_id IS NOT NULL GROUP BY manager_id";
    private static final String GROUP_BY_STATE = "SELECT state, COUNT(*) as employee_count FROM employee GROUP BY state";
    private static final String GROUP_BY_CITY = "SELECT city, COUNT(*) as employee_count FROM employee GROUP BY city";
    private static final String DEPT_WITH_MOST_EMPLOYEES = "SELECT department, COUNT(*) as count FROM employee GROUP BY department ORDER BY count DESC";
    private static final String MANAGER_WITH_MOST_REPORTS = "SELECT manager_id, COUNT(*) as reports FROM employee WHERE manager_id IS NOT NULL GROUP BY manager_id ORDER BY reports DESC";
    
    // Ordering queries - SQL strings 2481-2510
    private static final String SELECT_ALL_ORDER_BY_NAME = "SELECT employee_id, first_name, last_name, email, phone, department, position, manager_id, hire_date, birth_date, status, address, city, state, zip_code, created_date, modified_date FROM employee ORDER BY last_name, first_name";
    private static final String SELECT_ALL_ORDER_BY_HIRE_DATE = "SELECT employee_id, first_name, last_name, email, phone, department, position, manager_id, hire_date, birth_date, status, address, city, state, zip_code, created_date, modified_date FROM employee ORDER BY hire_date";
    private static final String SELECT_ALL_ORDER_BY_DEPT = "SELECT employee_id, first_name, last_name, email, phone, department, position, manager_id, hire_date, birth_date, status, address, city, state, zip_code, created_date, modified_date FROM employee ORDER BY department, last_name";
    
    // LIKE queries - SQL strings 2511-2540
    private static final String SELECT_BY_NAME_LIKE = "SELECT employee_id, first_name, last_name, email, phone, department, position, manager_id, hire_date, birth_date, status, address, city, state, zip_code, created_date, modified_date FROM employee WHERE first_name LIKE ? OR last_name LIKE ?";
    private static final String SELECT_BY_EMAIL_LIKE = "SELECT employee_id, first_name, last_name, email, phone, department, position, manager_id, hire_date, birth_date, status, address, city, state, zip_code, created_date, modified_date FROM employee WHERE email LIKE ?";
    private static final String SELECT_BY_DEPT_LIKE = "SELECT employee_id, first_name, last_name, email, phone, department, position, manager_id, hire_date, birth_date, status, address, city, state, zip_code, created_date, modified_date FROM employee WHERE department LIKE ?";
    
    // EXISTS queries - SQL strings 2541-2560
    private static final String EXISTS_BY_EMAIL = "SELECT COUNT(*) FROM employee WHERE email = ?";
    private static final String EXISTS_BY_PHONE = "SELECT COUNT(*) FROM employee WHERE phone = ?";
    private static final String CHECK_DUPLICATE_EMAIL = "SELECT COUNT(*) FROM employee WHERE email = ? AND employee_id != ?";
    
    // Distinct queries - SQL strings 2561-2580
    private static final String SELECT_DISTINCT_DEPARTMENTS = "SELECT DISTINCT department FROM employee WHERE department IS NOT NULL ORDER BY department";
    private static final String SELECT_DISTINCT_POSITIONS = "SELECT DISTINCT position FROM employee WHERE position IS NOT NULL ORDER BY position";
    private static final String SELECT_DISTINCT_STATES = "SELECT DISTINCT state FROM employee WHERE state IS NOT NULL ORDER BY state";
    private static final String SELECT_DISTINCT_CITIES = "SELECT DISTINCT city FROM employee WHERE city IS NOT NULL ORDER BY city";
    
    // Report queries - SQL strings 2581-2600
    private static final String SELECT_EMPLOYEE_DIRECTORY = "SELECT employee_id, first_name, last_name, email, phone, department, position FROM employee WHERE status = 'ACTIVE' ORDER BY department, last_name";
    private static final String SELECT_ORG_CHART_DATA = "SELECT e.employee_id, e.first_name, e.last_name, e.position, e.manager_id, m.first_name as manager_first_name, m.last_name as manager_last_name FROM employee e LEFT JOIN employee m ON e.manager_id = m.employee_id WHERE e.status = 'ACTIVE'";
    private static final String SELECT_HEADCOUNT_BY_DEPT = "SELECT department, COUNT(*) as headcount FROM employee WHERE status = 'ACTIVE' GROUP BY department ORDER BY headcount DESC";
    private static final String SELECT_TURNOVER_REPORT = "SELECT department, COUNT(CASE WHEN status = 'INACTIVE' THEN 1 END) as left, COUNT(CASE WHEN status = 'ACTIVE' THEN 1 END) as active FROM employee GROUP BY department";
    
    public EmployeeDAO() {}
}
