package com.ecommerce.platform.order;

import com.ecommerce.platform.inventory.dto.CreateProductRequest;
import com.ecommerce.platform.inventory.service.InventoryService;
import com.ecommerce.platform.order.dto.CreateOrderRequest;
import com.ecommerce.platform.order.dto.OrderResponse;
import com.ecommerce.platform.order.model.OrderStatus;
import com.ecommerce.platform.order.service.OrderService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Transactional
public class OrderServiceTest {

    @Autowired
    private OrderService orderService;

    @Autowired
    private InventoryService inventoryService;

    private String testSku;

    @BeforeEach
    void setUp() {
        testSku = "SMARTPHONE-X";
        CreateProductRequest productRequest = new CreateProductRequest(
                testSku,
                "Flagship Smartphone X",
                "Next-gen smartphone",
                new BigDecimal("999.00"),
                20
        );
        inventoryService.createProduct(productRequest);
    }

    @Test
    @DisplayName("Should successfully place an order and compute total price")
    void testCreateOrderSuccess() {
        CreateOrderRequest request = new CreateOrderRequest(
                "alice@example.com",
                testSku,
                2
        );

        OrderResponse response = orderService.createOrder(request);

        assertNotNull(response);
        assertNotNull(response.orderNumber());
        assertTrue(response.orderNumber().startsWith("ORD-"));
        assertEquals(OrderStatus.PENDING, response.status());
        assertEquals(new BigDecimal("1998.00"), response.totalAmount());
    }
}
