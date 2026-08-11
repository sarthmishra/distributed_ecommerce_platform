package com.ecommerce.platform.inventory.repository;

import com.ecommerce.platform.inventory.model.Product;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface ProductRepository extends JpaRepository<Product, Long> {
    Optional<Product> findBySku(String sku);
    boolean existsBySku(String sku);

    // method: findBySkuWithLock
    // does: same as findBySku but takes a row-level DB lock, so two concurrent
    //       reservations for the same SKU can't both pass the stock check at once
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT p FROM Product p WHERE p.sku = :sku")
    Optional<Product> findBySkuWithLock(String sku);
}