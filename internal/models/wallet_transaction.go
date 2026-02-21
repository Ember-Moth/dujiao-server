package models

import (
	"time"

	"gorm.io/gorm"
)

// WalletTransaction 钱包流水明细
type WalletTransaction struct {
	ID            uint           `gorm:"primarykey" json:"id"`                                        // 主键
	UserID        uint           `gorm:"index;not null" json:"user_id"`                               // 用户ID
	OrderID       *uint          `gorm:"index" json:"order_id,omitempty"`                             // 关联订单ID
	Type          string         `gorm:"type:varchar(40);index;not null" json:"type"`                 // 交易类型
	Direction     string         `gorm:"type:varchar(16);index;not null" json:"direction"`            // 资金方向
	Amount        Money          `gorm:"type:decimal(20,2);not null" json:"amount"`                   // 交易金额
	BalanceBefore Money          `gorm:"type:decimal(20,2);not null;default:0" json:"balance_before"` // 变更前余额
	BalanceAfter  Money          `gorm:"type:decimal(20,2);not null;default:0" json:"balance_after"`  // 变更后余额
	Currency      string         `gorm:"type:varchar(16);not null;default:'CNY'" json:"currency"`     // 币种
	Reference     string         `gorm:"type:varchar(120);uniqueIndex" json:"reference"`              // 幂等参考号
	Remark        string         `gorm:"type:varchar(255)" json:"remark"`                             // 备注
	CreatedAt     time.Time      `gorm:"index" json:"created_at"`                                     // 创建时间
	UpdatedAt     time.Time      `gorm:"index" json:"updated_at"`                                     // 更新时间
	DeletedAt     gorm.DeletedAt `gorm:"index" json:"-"`                                              // 软删除时间
}

// TableName 指定表名
func (WalletTransaction) TableName() string {
	return "wallet_transactions"
}
