package service

import (
	"fmt"
	"testing"
	"time"

	"github.com/dujiao-next/internal/constants"
	"github.com/dujiao-next/internal/models"
	"github.com/dujiao-next/internal/repository"

	"github.com/glebarez/sqlite"
	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

func setupCardSecretServiceTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := fmt.Sprintf("file:card_secret_service_test_%d?mode=memory&cache=shared", time.Now().UnixNano())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		DisableForeignKeyConstraintWhenMigrating: true,
	})
	if err != nil {
		t.Fatalf("open sqlite failed: %v", err)
	}
	if err := db.AutoMigrate(
		&models.Product{},
		&models.ProductSKU{},
		&models.CardSecretBatch{},
		&models.CardSecret{},
	); err != nil {
		t.Fatalf("auto migrate failed: %v", err)
	}
	models.DB = db
	return db
}

func TestCreateCardSecretBatchFallbackToDefaultSKU(t *testing.T) {
	db := setupCardSecretServiceTestDB(t)

	product := &models.Product{
		CategoryID:      1,
		Slug:            "card-secret-product-default",
		TitleJSON:       models.JSON{"zh-CN": "卡密商品"},
		PriceAmount:     models.NewMoneyFromDecimal(decimal.NewFromInt(20)),
		PurchaseType:    constants.ProductPurchaseMember,
		FulfillmentType: constants.FulfillmentTypeAuto,
		IsActive:        true,
	}
	if err := db.Create(product).Error; err != nil {
		t.Fatalf("create product failed: %v", err)
	}

	defaultSKU := &models.ProductSKU{
		ProductID:   product.ID,
		SKUCode:     models.DefaultSKUCode,
		PriceAmount: models.NewMoneyFromDecimal(decimal.NewFromInt(20)),
		IsActive:    true,
	}
	if err := db.Create(defaultSKU).Error; err != nil {
		t.Fatalf("create default sku failed: %v", err)
	}
	otherSKU := &models.ProductSKU{
		ProductID:   product.ID,
		SKUCode:     "PRO",
		PriceAmount: models.NewMoneyFromDecimal(decimal.NewFromInt(30)),
		IsActive:    true,
	}
	if err := db.Create(otherSKU).Error; err != nil {
		t.Fatalf("create other sku failed: %v", err)
	}

	svc := NewCardSecretService(
		repository.NewCardSecretRepository(db),
		repository.NewCardSecretBatchRepository(db),
		repository.NewProductRepository(db),
		repository.NewProductSKURepository(db),
	)

	batch, created, err := svc.CreateCardSecretBatch(CreateCardSecretBatchInput{
		ProductID: product.ID,
		Secrets:   []string{"AAA-001", "AAA-002"},
		Source:    constants.CardSecretSourceManual,
		AdminID:   1,
	})
	if err != nil {
		t.Fatalf("create card secret batch failed: %v", err)
	}
	if created != 2 {
		t.Fatalf("created count want 2 got %d", created)
	}
	if batch.SKUID != defaultSKU.ID {
		t.Fatalf("batch sku_id want default %d got %d", defaultSKU.ID, batch.SKUID)
	}

	var secretRows []models.CardSecret
	if err := db.Where("batch_id = ?", batch.ID).Find(&secretRows).Error; err != nil {
		t.Fatalf("query card secrets failed: %v", err)
	}
	if len(secretRows) != 2 {
		t.Fatalf("secret rows want 2 got %d", len(secretRows))
	}
	for _, row := range secretRows {
		if row.SKUID != defaultSKU.ID {
			t.Fatalf("secret sku_id want default %d got %d", defaultSKU.ID, row.SKUID)
		}
	}
}
