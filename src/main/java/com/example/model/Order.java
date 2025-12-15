package com.example.model;

import java.math.BigDecimal;
import java.util.Date;

/**
 * Order entity representing order table
 */
public class Order {
    private Long orderId;
    private Long customerId;
    private Date orderDate;
    private Date shippedDate;
    private Date deliveredDate;
    private String orderStatus;
    private BigDecimal totalAmount;
    private BigDecimal taxAmount;
    private BigDecimal shippingAmount;
    private String shippingAddress;
    private String shippingCity;
    private String shippingState;
    private String shippingZip;
    private String paymentMethod;
    private String paymentStatus;
    private Date createdDate;
    private Date modifiedDate;
    
    // Constructors
    public Order() {}
    
    public Order(Long orderId, Long customerId, Date orderDate) {
        this.orderId = orderId;
        this.customerId = customerId;
        this.orderDate = orderDate;
    }
    
    // Getters and Setters
    public Long getOrderId() { return orderId; }
    public void setOrderId(Long orderId) { this.orderId = orderId; }
    
    public Long getCustomerId() { return customerId; }
    public void setCustomerId(Long customerId) { this.customerId = customerId; }
    
    public Date getOrderDate() { return orderDate; }
    public void setOrderDate(Date orderDate) { this.orderDate = orderDate; }
    
    public Date getShippedDate() { return shippedDate; }
    public void setShippedDate(Date shippedDate) { this.shippedDate = shippedDate; }
    
    public Date getDeliveredDate() { return deliveredDate; }
    public void setDeliveredDate(Date deliveredDate) { this.deliveredDate = deliveredDate; }
    
    public String getOrderStatus() { return orderStatus; }
    public void setOrderStatus(String orderStatus) { this.orderStatus = orderStatus; }
    
    public BigDecimal getTotalAmount() { return totalAmount; }
    public void setTotalAmount(BigDecimal totalAmount) { this.totalAmount = totalAmount; }
    
    public BigDecimal getTaxAmount() { return taxAmount; }
    public void setTaxAmount(BigDecimal taxAmount) { this.taxAmount = taxAmount; }
    
    public BigDecimal getShippingAmount() { return shippingAmount; }
    public void setShippingAmount(BigDecimal shippingAmount) { this.shippingAmount = shippingAmount; }
    
    public String getShippingAddress() { return shippingAddress; }
    public void setShippingAddress(String shippingAddress) { this.shippingAddress = shippingAddress; }
    
    public String getShippingCity() { return shippingCity; }
    public void setShippingCity(String shippingCity) { this.shippingCity = shippingCity; }
    
    public String getShippingState() { return shippingState; }
    public void setShippingState(String shippingState) { this.shippingState = shippingState; }
    
    public String getShippingZip() { return shippingZip; }
    public void setShippingZip(String shippingZip) { this.shippingZip = shippingZip; }
    
    public String getPaymentMethod() { return paymentMethod; }
    public void setPaymentMethod(String paymentMethod) { this.paymentMethod = paymentMethod; }
    
    public String getPaymentStatus() { return paymentStatus; }
    public void setPaymentStatus(String paymentStatus) { this.paymentStatus = paymentStatus; }
    
    public Date getCreatedDate() { return createdDate; }
    public void setCreatedDate(Date createdDate) { this.createdDate = createdDate; }
    
    public Date getModifiedDate() { return modifiedDate; }
    public void setModifiedDate(Date modifiedDate) { this.modifiedDate = modifiedDate; }
}
