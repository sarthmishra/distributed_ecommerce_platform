package com.ecommerce.platform.inventory;

import com.ecommerce.platform.common.exception.InsufficientStockException;
import com.ecommerce.platform.inventory.dto.CreateProductRequest;
import com.ecommerce.platform.inventory.dto.ProductResponse;
import com.ecommerce.platform.inventory.service.InventoryService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@ActiveProfiles("test")
@Transactional
public class InventoryServiceTest {

    @Autowired
    private InventoryService inventoryService;

    private String testSku;

    @BeforeEach
    void setUp() {
        testSku = "LAPTOP-PRO-15";
        CreateProductRequest request = new CreateProductRequest(
                testSku,
                "Pro Laptop 15 inch",
                "High performance developer laptop",
                new BigDecimal("1499.99"),
                10
        );
        inventoryService.createProduct(request);
    }

    @Test
    @DisplayName("Should successfully create and retrieve product by SKU")
    void testCreateAndGetProduct() {
        ProductResponse response = inventoryService.getProductBySku(testSku);
        assertNotNull(response);
        assertEquals(testSku, response.sku());
        assertEquals(10, response.stockQuantity());
    }

    @Test
    @DisplayName("Should successfully reserve stock when sufficient quantity is available")
    void testReserveStockSuccess() {
        ProductResponse response = inventoryService.reserveStock(testSku, 3);
        assertEquals(7, response.stockQuantity());
    }

    @Test
    @DisplayName("Should throw InsufficientStockException when reserving more than available stock")
    void testReserveStockInsufficient() {
        assertThrows(InsufficientStockException.class, () -> {
            inventoryService.reserveStock(testSku, 15);
        });
    }

    @Test
    @DisplayName("Should successfully restore stock when releasing stock")
    void testReleaseStock() {
        inventoryService.reserveStock(testSku, 4);
        ProductResponse response = inventoryService.releaseStock(testSku, 2);
        assertEquals(8, response.stockQuantity());
    }
}
