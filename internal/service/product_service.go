package service

import (
	"strconv"
	"strings"

	"github.com/dujiao-next/internal/constants"
	"github.com/dujiao-next/internal/models"
	"github.com/dujiao-next/internal/repository"

	"github.com/shopspring/decimal"
	"gorm.io/gorm"
)

// ProductService 商品业务服务
type ProductService struct {
	repo           repository.ProductRepository
	productSKURepo repository.ProductSKURepository
}

// NewProductService 创建商品服务
func NewProductService(repo repository.ProductRepository, productSKURepo repository.ProductSKURepository) *ProductService {
	return &ProductService{
		repo:           repo,
		productSKURepo: productSKURepo,
	}
}

// CreateProductInput 创建/更新商品输入
type CreateProductInput struct {
	CategoryID           uint
	Slug                 string
	SeoMetaJSON          map[string]interface{}
	TitleJSON            map[string]interface{}
	DescriptionJSON      map[string]interface{}
	ContentJSON          map[string]interface{}
	ManualFormSchemaJSON map[string]interface{}
	PriceAmount          decimal.Decimal
	Images               []string
	Tags                 []string
	PurchaseType         string
	FulfillmentType      string
	ManualStockTotal     *int
	SKUs                 []ProductSKUInput
	IsActive             *bool
	SortOrder            int
}

type ProductSKUInput struct {
	ID               uint
	SKUCode          string
	SpecValuesJSON   map[string]interface{}
	PriceAmount      decimal.Decimal
	ManualStockTotal int
	IsActive         *bool
	SortOrder        int
}

// ListPublic 获取公开商品列表
func (s *ProductService) ListPublic(categoryID, search string, page, pageSize int) ([]models.Product, int64, error) {
	filter := repository.ProductListFilter{
		Page:         page,
		PageSize:     pageSize,
		CategoryID:   categoryID,
		Search:       search,
		OnlyActive:   true,
		WithCategory: true,
	}
	return s.repo.List(filter)
}

// GetPublicBySlug 获取公开商品详情
func (s *ProductService) GetPublicBySlug(slug string) (*models.Product, error) {
	product, err := s.repo.GetBySlug(slug, true)
	if err != nil {
		return nil, err
	}
	if product == nil {
		return nil, ErrNotFound
	}
	return product, nil
}

// ListAdmin 获取后台商品列表
func (s *ProductService) ListAdmin(categoryID, search, manualStockStatus string, page, pageSize int) ([]models.Product, int64, error) {
	filter := repository.ProductListFilter{
		Page:              page,
		PageSize:          pageSize,
		CategoryID:        categoryID,
		Search:            search,
		ManualStockStatus: normalizeManualStockStatus(manualStockStatus),
		OnlyActive:        false,
		WithCategory:      true,
	}
	return s.repo.List(filter)
}

// GetAdminByID 获取后台商品详情
func (s *ProductService) GetAdminByID(id string) (*models.Product, error) {
	product, err := s.repo.GetByID(id)
	if err != nil {
		return nil, err
	}
	if product == nil {
		return nil, ErrNotFound
	}
	return product, nil
}

// Create 创建商品
func (s *ProductService) Create(input CreateProductInput) (*models.Product, error) {
	count, err := s.repo.CountBySlug(input.Slug, nil)
	if err != nil {
		return nil, err
	}
	if count > 0 {
		return nil, ErrSlugExists
	}

	isActive := true
	if input.IsActive != nil {
		isActive = *input.IsActive
	}
	purchaseType := normalizePurchaseType(input.PurchaseType)
	if purchaseType == "" {
		return nil, ErrProductPurchaseInvalid
	}
	fulfillmentType := normalizeFulfillmentType(input.FulfillmentType)
	if fulfillmentType == "" {
		return nil, ErrFulfillmentInvalid
	}

	priceAmount := input.PriceAmount.Round(2)
	if len(input.SKUs) == 0 && priceAmount.LessThanOrEqual(decimal.Zero) {
		return nil, ErrProductPriceInvalid
	}

	manualStockTotal := 0
	if input.ManualStockTotal != nil {
		manualStockTotal = *input.ManualStockTotal
	}
	if manualStockTotal < 0 {
		return nil, ErrManualStockInvalid
	}

	var normalizedSKUs []normalizedProductSKU
	if len(input.SKUs) > 0 {
		if s.productSKURepo == nil {
			return nil, ErrProductSKUInvalid
		}
		var normalizeErr error
		normalizedSKUs, priceAmount, manualStockTotal, normalizeErr = normalizeProductSKUInputs(input.SKUs, fulfillmentType, nil)
		if normalizeErr != nil {
			return nil, normalizeErr
		}
	}

	product := models.Product{
		CategoryID:           input.CategoryID,
		Slug:                 input.Slug,
		SeoMetaJSON:          models.JSON(input.SeoMetaJSON),
		TitleJSON:            models.JSON(input.TitleJSON),
		DescriptionJSON:      models.JSON(input.DescriptionJSON),
		ContentJSON:          models.JSON(input.ContentJSON),
		ManualFormSchemaJSON: models.JSON{},
		PriceAmount:          models.NewMoneyFromDecimal(priceAmount),
		Images:               models.StringArray(input.Images),
		Tags:                 models.StringArray(input.Tags),
		PurchaseType:         purchaseType,
		FulfillmentType:      fulfillmentType,
		ManualStockTotal:     manualStockTotal,
		ManualStockLocked:    0,
		ManualStockSold:      0,
		IsActive:             isActive,
		SortOrder:            input.SortOrder,
	}
	if fulfillmentType == constants.FulfillmentTypeManual {
		_, normalizedSchemaJSON, err := parseManualFormSchema(models.JSON(input.ManualFormSchemaJSON))
		if err != nil {
			return nil, err
		}
		product.ManualFormSchemaJSON = normalizedSchemaJSON
	}

	if err := models.DB.Transaction(func(tx *gorm.DB) error {
		productRepo := s.repo.WithTx(tx)
		var skuRepo repository.ProductSKURepository
		if s.productSKURepo != nil {
			skuRepo = s.productSKURepo.WithTx(tx)
		}
		if err := productRepo.Create(&product); err != nil {
			return err
		}
		if len(normalizedSKUs) > 0 {
			return applyProductSKUs(skuRepo, product.ID, normalizedSKUs)
		}
		return syncSingleProductSKU(skuRepo, product.ID, priceAmount, manualStockTotal, true)
	}); err != nil {
		return nil, err
	}
	return s.repo.GetByID(strconv.FormatUint(uint64(product.ID), 10))
}

// Update 更新商品
func (s *ProductService) Update(id string, input CreateProductInput) (*models.Product, error) {
	priceAmount := input.PriceAmount.Round(2)
	if len(input.SKUs) == 0 && priceAmount.LessThanOrEqual(decimal.Zero) {
		return nil, ErrProductPriceInvalid
	}
	product, err := s.repo.GetByID(id)
	if err != nil {
		return nil, err
	}
	if product == nil {
		return nil, ErrNotFound
	}

	count, err := s.repo.CountBySlug(input.Slug, &id)
	if err != nil {
		return nil, err
	}
	if count > 0 {
		return nil, ErrSlugExists
	}

	product.CategoryID = input.CategoryID
	product.Slug = input.Slug
	product.SeoMetaJSON = models.JSON(input.SeoMetaJSON)
	product.TitleJSON = models.JSON(input.TitleJSON)
	product.DescriptionJSON = models.JSON(input.DescriptionJSON)
	product.ContentJSON = models.JSON(input.ContentJSON)
	product.ManualFormSchemaJSON = models.JSON{}
	product.PriceAmount = models.NewMoneyFromDecimal(priceAmount)
	product.SortOrder = input.SortOrder
	product.Images = models.StringArray(input.Images)
	product.Tags = models.StringArray(input.Tags)
	if input.IsActive != nil {
		product.IsActive = *input.IsActive
	}
	rawPurchaseType := strings.TrimSpace(input.PurchaseType)
	if rawPurchaseType == "" {
		rawPurchaseType = product.PurchaseType
	}
	purchaseType := normalizePurchaseType(rawPurchaseType)
	if purchaseType == "" {
		return nil, ErrProductPurchaseInvalid
	}
	product.PurchaseType = purchaseType
	rawFulfillmentType := strings.TrimSpace(input.FulfillmentType)
	if rawFulfillmentType == "" {
		rawFulfillmentType = product.FulfillmentType
	}
	fulfillmentType := normalizeFulfillmentType(rawFulfillmentType)
	if fulfillmentType == "" {
		return nil, ErrFulfillmentInvalid
	}
	product.FulfillmentType = fulfillmentType
	if fulfillmentType == constants.FulfillmentTypeManual {
		_, normalizedSchemaJSON, err := parseManualFormSchema(models.JSON(input.ManualFormSchemaJSON))
		if err != nil {
			return nil, err
		}
		product.ManualFormSchemaJSON = normalizedSchemaJSON
	}

	manualStockTotal := product.ManualStockTotal
	if input.ManualStockTotal != nil {
		manualStockTotal = *input.ManualStockTotal
	}
	if manualStockTotal < 0 {
		return nil, ErrManualStockInvalid
	}

	var normalizedSKUs []normalizedProductSKU
	if len(input.SKUs) > 0 {
		if s.productSKURepo == nil {
			return nil, ErrProductSKUInvalid
		}
		existingSKUs, listErr := s.productSKURepo.ListByProduct(product.ID, false)
		if listErr != nil {
			return nil, listErr
		}
		existingSKUMap := make(map[uint]models.ProductSKU, len(existingSKUs))
		for _, sku := range existingSKUs {
			existingSKUMap[sku.ID] = sku
		}
		var normalizeErr error
		normalizedSKUs, priceAmount, manualStockTotal, normalizeErr = normalizeProductSKUInputs(input.SKUs, fulfillmentType, existingSKUMap)
		if normalizeErr != nil {
			return nil, normalizeErr
		}
	} else if manualStockTotal < product.ManualStockLocked+product.ManualStockSold {
		return nil, ErrManualStockInvalid
	}

	product.PriceAmount = models.NewMoneyFromDecimal(priceAmount)
	product.ManualStockTotal = manualStockTotal

	if err := models.DB.Transaction(func(tx *gorm.DB) error {
		productRepo := s.repo.WithTx(tx)
		var skuRepo repository.ProductSKURepository
		if s.productSKURepo != nil {
			skuRepo = s.productSKURepo.WithTx(tx)
		}
		if err := productRepo.Update(product); err != nil {
			return err
		}
		if len(normalizedSKUs) > 0 {
			return applyProductSKUs(skuRepo, product.ID, normalizedSKUs)
		}
		return syncSingleProductSKU(skuRepo, product.ID, priceAmount, product.ManualStockTotal, true)
	}); err != nil {
		return nil, err
	}
	return s.repo.GetByID(id)
}

func syncSingleProductSKU(skuRepo repository.ProductSKURepository, productID uint, priceAmount decimal.Decimal, manualStockTotal int, createWhenMissing bool) error {
	if skuRepo == nil || productID == 0 {
		return nil
	}
	skus, err := skuRepo.ListByProduct(productID, false)
	if err != nil {
		return err
	}
	if len(skus) == 0 {
		if !createWhenMissing {
			return nil
		}
		return skuRepo.Create(&models.ProductSKU{
			ProductID:         productID,
			SKUCode:           models.DefaultSKUCode,
			SpecValuesJSON:    models.JSON{},
			PriceAmount:       models.NewMoneyFromDecimal(priceAmount),
			ManualStockTotal:  manualStockTotal,
			ManualStockLocked: 0,
			ManualStockSold:   0,
			IsActive:          true,
			SortOrder:         0,
		})
	}
	if len(skus) > 1 {
		return nil
	}
	sku := skus[0]
	if manualStockTotal < sku.ManualStockLocked+sku.ManualStockSold {
		return ErrManualStockInvalid
	}
	sku.PriceAmount = models.NewMoneyFromDecimal(priceAmount)
	sku.ManualStockTotal = manualStockTotal
	sku.IsActive = true
	return skuRepo.Update(&sku)
}

type normalizedProductSKU struct {
	ID               uint
	SKUCode          string
	SpecValuesJSON   models.JSON
	PriceAmount      models.Money
	ManualStockTotal int
	IsActive         bool
	SortOrder        int
}

func normalizeProductSKUInputs(inputs []ProductSKUInput, fulfillmentType string, existingSKUMap map[uint]models.ProductSKU) ([]normalizedProductSKU, decimal.Decimal, int, error) {
	if len(inputs) == 0 {
		return nil, decimal.Zero, 0, ErrProductSKUInvalid
	}
	seenCode := make(map[string]struct{}, len(inputs))
	normalized := make([]normalizedProductSKU, 0, len(inputs))
	hasActive := false
	minActivePrice := decimal.Zero
	manualStockTotal := 0

	for _, input := range inputs {
		skuCode := strings.TrimSpace(input.SKUCode)
		if skuCode == "" {
			return nil, decimal.Zero, 0, ErrProductSKUInvalid
		}
		codeKey := strings.ToLower(skuCode)
		if _, exists := seenCode[codeKey]; exists {
			return nil, decimal.Zero, 0, ErrProductSKUInvalid
		}
		seenCode[codeKey] = struct{}{}

		priceAmount := input.PriceAmount.Round(2)
		if priceAmount.LessThanOrEqual(decimal.Zero) {
			return nil, decimal.Zero, 0, ErrProductPriceInvalid
		}

		manualTotal := input.ManualStockTotal
		if manualTotal < 0 {
			return nil, decimal.Zero, 0, ErrManualStockInvalid
		}
		if fulfillmentType != constants.FulfillmentTypeManual {
			manualTotal = 0
		}
		if existingSKUMap != nil && input.ID > 0 {
			existing, ok := existingSKUMap[input.ID]
			if !ok {
				return nil, decimal.Zero, 0, ErrProductSKUInvalid
			}
			if manualTotal < existing.ManualStockLocked+existing.ManualStockSold {
				return nil, decimal.Zero, 0, ErrManualStockInvalid
			}
		}

		isActive := true
		if input.IsActive != nil {
			isActive = *input.IsActive
		}
		specValues := models.JSON{}
		if input.SpecValuesJSON != nil {
			specValues = models.JSON(input.SpecValuesJSON)
		}

		normalized = append(normalized, normalizedProductSKU{
			ID:               input.ID,
			SKUCode:          skuCode,
			SpecValuesJSON:   specValues,
			PriceAmount:      models.NewMoneyFromDecimal(priceAmount),
			ManualStockTotal: manualTotal,
			IsActive:         isActive,
			SortOrder:        input.SortOrder,
		})

		if isActive {
			if !hasActive || priceAmount.LessThan(minActivePrice) {
				minActivePrice = priceAmount
			}
			hasActive = true
			if fulfillmentType == constants.FulfillmentTypeManual {
				manualStockTotal += manualTotal
			}
		}
	}

	if !hasActive {
		return nil, decimal.Zero, 0, ErrProductSKUInvalid
	}
	if fulfillmentType != constants.FulfillmentTypeManual {
		manualStockTotal = 0
	}
	return normalized, minActivePrice, manualStockTotal, nil
}

func applyProductSKUs(skuRepo repository.ProductSKURepository, productID uint, rows []normalizedProductSKU) error {
	if skuRepo == nil || productID == 0 || len(rows) == 0 {
		return ErrProductSKUInvalid
	}
	existingRows, err := skuRepo.ListByProduct(productID, false)
	if err != nil {
		return err
	}
	existingByID := make(map[uint]models.ProductSKU, len(existingRows))
	existingByCode := make(map[string]models.ProductSKU, len(existingRows))
	for _, row := range existingRows {
		existingByID[row.ID] = row
		existingByCode[strings.ToLower(strings.TrimSpace(row.SKUCode))] = row
	}

	kept := make(map[uint]struct{}, len(rows))
	for _, row := range rows {
		if row.ID > 0 {
			existing, ok := existingByID[row.ID]
			if !ok {
				return ErrProductSKUInvalid
			}
			existing.SKUCode = row.SKUCode
			existing.SpecValuesJSON = row.SpecValuesJSON
			existing.PriceAmount = row.PriceAmount
			existing.ManualStockTotal = row.ManualStockTotal
			existing.IsActive = row.IsActive
			existing.SortOrder = row.SortOrder
			if err := skuRepo.Update(&existing); err != nil {
				return err
			}
			kept[existing.ID] = struct{}{}
			existingByCode[strings.ToLower(strings.TrimSpace(existing.SKUCode))] = existing
			continue
		}

		codeKey := strings.ToLower(strings.TrimSpace(row.SKUCode))
		if existing, ok := existingByCode[codeKey]; ok {
			existing.SpecValuesJSON = row.SpecValuesJSON
			existing.PriceAmount = row.PriceAmount
			existing.ManualStockTotal = row.ManualStockTotal
			existing.IsActive = row.IsActive
			existing.SortOrder = row.SortOrder
			if err := skuRepo.Update(&existing); err != nil {
				return err
			}
			kept[existing.ID] = struct{}{}
			continue
		}

		item := models.ProductSKU{
			ProductID:         productID,
			SKUCode:           row.SKUCode,
			SpecValuesJSON:    row.SpecValuesJSON,
			PriceAmount:       row.PriceAmount,
			ManualStockTotal:  row.ManualStockTotal,
			ManualStockLocked: 0,
			ManualStockSold:   0,
			IsActive:          row.IsActive,
			SortOrder:         row.SortOrder,
		}
		if err := skuRepo.Create(&item); err != nil {
			return err
		}
		kept[item.ID] = struct{}{}
	}

	for _, existing := range existingRows {
		if _, ok := kept[existing.ID]; ok {
			continue
		}
		if !existing.IsActive {
			continue
		}
		existing.IsActive = false
		if err := skuRepo.Update(&existing); err != nil {
			return err
		}
	}
	return nil
}

func normalizePurchaseType(raw string) string {
	value := strings.ToLower(strings.TrimSpace(raw))
	switch value {
	case "", constants.ProductPurchaseMember:
		return constants.ProductPurchaseMember
	case constants.ProductPurchaseGuest:
		return constants.ProductPurchaseGuest
	default:
		return ""
	}
}

func normalizeFulfillmentType(raw string) string {
	value := strings.ToLower(strings.TrimSpace(raw))
	switch value {
	case "", constants.FulfillmentTypeManual:
		return constants.FulfillmentTypeManual
	case constants.FulfillmentTypeAuto:
		return constants.FulfillmentTypeAuto
	default:
		return ""
	}
}

func normalizeManualStockStatus(raw string) string {
	value := strings.ToLower(strings.TrimSpace(raw))
	switch value {
	case "", "all":
		return ""
	case "low", "normal", "unlimited":
		return value
	default:
		return ""
	}
}

// Delete 删除商品
func (s *ProductService) Delete(id string) error {
	product, err := s.repo.GetByID(id)
	if err != nil {
		return err
	}
	if product == nil {
		return ErrNotFound
	}
	return s.repo.Delete(id)
}
