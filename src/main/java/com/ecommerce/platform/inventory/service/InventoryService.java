package com.ecommerce.platform.inventory.service;

import com.ecommerce.platform.common.exception.InsufficientStockException;
import com.ecommerce.platform.common.exception.ResourceNotFoundException;
import com.ecommerce.platform.inventory.dto.CreateProductRequest;
import com.ecommerce.platform.inventory.dto.ProductResponse;
import com.ecommerce.platform.inventory.model.Product;
import com.ecommerce.platform.inventory.repository.ProductRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

    private final ProductRepository productRepository;

    public InventoryService(ProductRepository productRepository) {
        this.productRepository = productRepository;
    }

    @Transactional
    public ProductResponse createProduct(CreateProductRequest request) {
        if (productRepository.existsBySku(request.sku())) {
            throw new IllegalArgumentException("Product with SKU " + request.sku() + " already exists");
        }

        Product product = new Product(
                null,
                request.sku(),
                request.name(),
                request.description(),
                request.price(),
                request.stockQuantity()
        );

        Product savedProduct = productRepository.save(product);
        log.info("Successfully created product with ID: {} and SKU: {}", savedProduct.getId(), savedProduct.getSku());
        return ProductResponse.fromEntity(savedProduct);
    }

    @Transactional(readOnly = true)
    public ProductResponse getProductBySku(String sku) {
        Product product = productRepository.findBySku(sku)
                .orElseThrow(() -> new ResourceNotFoundException("Product not found with SKU: " + sku));
        return ProductResponse.fromEntity(product);
    }

    @Transactional(readOnly = true)
    public List<ProductResponse> getAllProducts() {
        return productRepository.findAll().stream()
                .map(ProductResponse::fromEntity)
                .collect(Collectors.toList());
    }

    @Transactional(noRollbackFor = InsufficientStockException.class)
    public ProductResponse reserveStock(String sku, int quantity) {
        // change: findBySkuWithLock instead of findBySku - takes a row lock so two
        // concurrent reservations for the same SKU are serialized, not raced
        Product product = productRepository.findBySkuWithLock(sku)
                .orElseThrow(() -> new ResourceNotFoundException("Product not found with SKU: " + sku));

        if (product.getStockQuantity() < quantity) {
            log.warn("Stock reservation failed for SKU: {}. Requested: {}, Available: {}", sku, quantity, product.getStockQuantity());
            throw new InsufficientStockException("Insufficient stock for SKU: " + sku + ". Available: " + product.getStockQuantity());
        }

        product.setStockQuantity(product.getStockQuantity() - quantity);
        Product updatedProduct = productRepository.save(product);
        log.info("Stock reserved for SKU: {}. Reserved quantity: {}, Remaining stock: {}", sku, quantity, updatedProduct.getStockQuantity());
        return ProductResponse.fromEntity(updatedProduct);
    }

    @Transactional
    public ProductResponse releaseStock(String sku, int quantity) {
        Product product = productRepository.findBySkuWithLock(sku)
                .orElseThrow(() -> new ResourceNotFoundException("Product not found with SKU: " + sku));

        product.setStockQuantity(product.getStockQuantity() + quantity);
        Product updatedProduct = productRepository.save(product);
        log.info("Stock released for SKU: {}. Restored quantity: {}, Total stock: {}", sku, quantity, updatedProduct.getStockQuantity());
        return ProductResponse.fromEntity(updatedProduct);
    }
}