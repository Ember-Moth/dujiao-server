package models

import (
	"time"

	"gorm.io/gorm"
)

// WalletAccount 用户钱包账户
type WalletAccount struct {
	ID        uint           `gorm:"primarykey" json:"id"`                                 // 主键
	UserID    uint           `gorm:"uniqueIndex;not null" json:"user_id"`                  // 用户ID
	Balance   Money          `gorm:"type:decimal(20,2);not null;default:0" json:"balance"` // 当前余额
	CreatedAt time.Time      `gorm:"index" json:"created_at"`                              // 创建时间
	UpdatedAt time.Time      `gorm:"index" json:"updated_at"`                              // 更新时间
	DeletedAt gorm.DeletedAt `gorm:"index" json:"-"`                                       // 软删除时间
}

// TableName 指定表名
func (WalletAccount) TableName() string {
	return "wallet_accounts"
}
