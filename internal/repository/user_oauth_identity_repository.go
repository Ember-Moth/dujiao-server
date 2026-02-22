package repository

import (
	"errors"
	"strings"

	"github.com/dujiao-next/internal/models"

	"gorm.io/gorm"
)

// UserOAuthIdentityRepository 用户第三方身份映射仓储接口
type UserOAuthIdentityRepository interface {
	GetByProviderUserID(provider, providerUserID string) (*models.UserOAuthIdentity, error)
	GetByUserProvider(userID uint, provider string) (*models.UserOAuthIdentity, error)
	Create(identity *models.UserOAuthIdentity) error
	Update(identity *models.UserOAuthIdentity) error
	DeleteByID(id uint) error
	WithTx(tx *gorm.DB) *GormUserOAuthIdentityRepository
}

// GormUserOAuthIdentityRepository GORM 实现
type GormUserOAuthIdentityRepository struct {
	db *gorm.DB
}

// NewUserOAuthIdentityRepository 创建仓储
func NewUserOAuthIdentityRepository(db *gorm.DB) *GormUserOAuthIdentityRepository {
	return &GormUserOAuthIdentityRepository{db: db}
}

// WithTx 绑定事务
func (r *GormUserOAuthIdentityRepository) WithTx(tx *gorm.DB) *GormUserOAuthIdentityRepository {
	if tx == nil {
		return r
	}
	return &GormUserOAuthIdentityRepository{db: tx}
}

// GetByProviderUserID 按提供方用户ID查询绑定
func (r *GormUserOAuthIdentityRepository) GetByProviderUserID(provider, providerUserID string) (*models.UserOAuthIdentity, error) {
	provider = strings.ToLower(strings.TrimSpace(provider))
	providerUserID = strings.TrimSpace(providerUserID)
	if provider == "" || providerUserID == "" {
		return nil, nil
	}
	var identity models.UserOAuthIdentity
	if err := r.db.Where("provider = ? AND provider_user_id = ?", provider, providerUserID).First(&identity).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &identity, nil
}

// GetByUserProvider 按用户查询某个提供方绑定
func (r *GormUserOAuthIdentityRepository) GetByUserProvider(userID uint, provider string) (*models.UserOAuthIdentity, error) {
	provider = strings.ToLower(strings.TrimSpace(provider))
	if userID == 0 || provider == "" {
		return nil, nil
	}
	var identity models.UserOAuthIdentity
	if err := r.db.Where("user_id = ? AND provider = ?", userID, provider).First(&identity).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil, nil
		}
		return nil, err
	}
	return &identity, nil
}

// Create 创建绑定
func (r *GormUserOAuthIdentityRepository) Create(identity *models.UserOAuthIdentity) error {
	if identity == nil {
		return nil
	}
	return r.db.Create(identity).Error
}

// Update 更新绑定
func (r *GormUserOAuthIdentityRepository) Update(identity *models.UserOAuthIdentity) error {
	if identity == nil {
		return nil
	}
	return r.db.Save(identity).Error
}

// DeleteByID 删除绑定
func (r *GormUserOAuthIdentityRepository) DeleteByID(id uint) error {
	if id == 0 {
		return nil
	}
	return r.db.Delete(&models.UserOAuthIdentity{}, id).Error
}
