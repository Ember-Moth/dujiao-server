package models

import (
	"time"

	"gorm.io/gorm"
)

// WalletRechargeOrder 钱包充值支付单
type WalletRechargeOrder struct {
	ID              uint           `gorm:"primarykey" json:"id"`                                     // 主键
	RechargeNo      string         `gorm:"type:varchar(40);uniqueIndex;not null" json:"recharge_no"` // 充值单号
	UserID          uint           `gorm:"index;not null" json:"user_id"`                            // 用户ID
	PaymentID       uint           `gorm:"uniqueIndex;not null" json:"payment_id"`                   // 关联支付ID
	ChannelID       uint           `gorm:"index;not null" json:"channel_id"`                         // 支付渠道ID
	ProviderType    string         `gorm:"type:varchar(32);not null" json:"provider_type"`           // 提供方类型
	ChannelType     string         `gorm:"type:varchar(32);not null" json:"channel_type"`            // 渠道类型
	InteractionMode string         `gorm:"type:varchar(32);not null" json:"interaction_mode"`        // 交互方式
	Amount          Money          `gorm:"type:decimal(20,2);not null" json:"amount"`                // 充值金额
	PayableAmount   Money          `gorm:"type:decimal(20,2);not null" json:"payable_amount"`        // 实际支付金额（含手续费）
	FeeRate         Money          `gorm:"type:decimal(6,2);not null;default:0" json:"fee_rate"`     // 手续费比例
	FeeAmount       Money          `gorm:"type:decimal(20,2);not null;default:0" json:"fee_amount"`  // 手续费金额
	Currency        string         `gorm:"type:varchar(16);not null;default:'CNY'" json:"currency"`  // 币种
	Status          string         `gorm:"type:varchar(20);index;not null" json:"status"`            // 充值状态
	Remark          string         `gorm:"type:varchar(255)" json:"remark"`                          // 备注
	PaidAt          *time.Time     `gorm:"index" json:"paid_at"`                                     // 支付完成时间
	CreatedAt       time.Time      `gorm:"index" json:"created_at"`                                  // 创建时间
	UpdatedAt       time.Time      `gorm:"index" json:"updated_at"`                                  // 更新时间
	DeletedAt       gorm.DeletedAt `gorm:"index" json:"-"`                                           // 软删除时间
}

// TableName 指定表名
func (WalletRechargeOrder) TableName() string {
	return "wallet_recharge_orders"
}
