package service

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/dujiao-next/internal/cache"
	"github.com/dujiao-next/internal/config"
	"github.com/dujiao-next/internal/constants"
)

// TelegramLoginPayload Telegram 登录载荷
type TelegramLoginPayload struct {
	ID        int64
	FirstName string
	LastName  string
	Username  string
	PhotoURL  string
	AuthDate  int64
	Hash      string
}

// TelegramIdentityVerified Telegram 身份校验结果
type TelegramIdentityVerified struct {
	Provider       string
	ProviderUserID string
	Username       string
	AvatarURL      string
	FirstName      string
	LastName       string
	AuthAt         time.Time
}

// TelegramAuthService Telegram 登录校验服务
type TelegramAuthService struct {
	cfg config.TelegramAuthConfig
}

// NewTelegramAuthService 创建 Telegram 登录校验服务
func NewTelegramAuthService(cfg config.TelegramAuthConfig) *TelegramAuthService {
	return &TelegramAuthService{cfg: normalizeTelegramAuthConfig(cfg)}
}

// SetConfig 更新运行时配置
func (s *TelegramAuthService) SetConfig(cfg config.TelegramAuthConfig) {
	if s == nil {
		return
	}
	s.cfg = normalizeTelegramAuthConfig(cfg)
}

// PublicConfig 返回前台可见配置
func (s *TelegramAuthService) PublicConfig() map[string]interface{} {
	if s == nil {
		return map[string]interface{}{
			"enabled":      false,
			"bot_username": "",
		}
	}
	cfg := normalizeTelegramAuthConfig(s.cfg)
	return map[string]interface{}{
		"enabled":      cfg.Enabled,
		"bot_username": strings.TrimSpace(cfg.BotUsername),
	}
}

// VerifyLogin 校验 Telegram 登录载荷
func (s *TelegramAuthService) VerifyLogin(ctx context.Context, payload TelegramLoginPayload) (*TelegramIdentityVerified, error) {
	if s == nil {
		return nil, ErrTelegramAuthConfigInvalid
	}
	cfg := normalizeTelegramAuthConfig(s.cfg)
	if !cfg.Enabled {
		return nil, ErrTelegramAuthDisabled
	}
	if strings.TrimSpace(cfg.BotToken) == "" {
		return nil, ErrTelegramAuthConfigInvalid
	}
	normalized, err := normalizeTelegramLoginPayload(payload)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	authAt := time.Unix(normalized.AuthDate, 0)
	if authAt.After(now.Add(time.Minute)) {
		return nil, ErrTelegramAuthPayloadInvalid
	}
	if now.Sub(authAt) > time.Duration(cfg.LoginExpireSeconds)*time.Second {
		return nil, ErrTelegramAuthExpired
	}

	dataCheckString := buildTelegramDataCheckString(normalized)
	expected := buildTelegramHash(cfg.BotToken, dataCheckString)
	if !hmac.Equal([]byte(expected), []byte(normalized.Hash)) {
		return nil, ErrTelegramAuthSignatureInvalid
	}

	replayTTL := time.Duration(cfg.ReplayTTLSeconds) * time.Second
	replayKey := fmt.Sprintf("telegram:auth:replay:%d:%s", normalized.ID, normalized.Hash)
	ok, err := cache.SetNX(ctx, replayKey, "1", replayTTL)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, ErrTelegramAuthReplay
	}

	return &TelegramIdentityVerified{
		Provider:       constants.UserOAuthProviderTelegram,
		ProviderUserID: strconv.FormatInt(normalized.ID, 10),
		Username:       normalized.Username,
		AvatarURL:      normalized.PhotoURL,
		FirstName:      normalized.FirstName,
		LastName:       normalized.LastName,
		AuthAt:         authAt,
	}, nil
}

func normalizeTelegramAuthConfig(cfg config.TelegramAuthConfig) config.TelegramAuthConfig {
	cfg.BotUsername = strings.TrimSpace(cfg.BotUsername)
	cfg.BotToken = strings.TrimSpace(cfg.BotToken)
	if cfg.LoginExpireSeconds <= 0 {
		cfg.LoginExpireSeconds = 300
	}
	if cfg.ReplayTTLSeconds <= 0 {
		cfg.ReplayTTLSeconds = cfg.LoginExpireSeconds
	}
	if cfg.ReplayTTLSeconds < 60 {
		cfg.ReplayTTLSeconds = 60
	}
	return cfg
}

func normalizeTelegramLoginPayload(payload TelegramLoginPayload) (TelegramLoginPayload, error) {
	normalized := TelegramLoginPayload{
		ID:        payload.ID,
		FirstName: strings.TrimSpace(payload.FirstName),
		LastName:  strings.TrimSpace(payload.LastName),
		Username:  strings.TrimSpace(payload.Username),
		PhotoURL:  strings.TrimSpace(payload.PhotoURL),
		AuthDate:  payload.AuthDate,
		Hash:      strings.ToLower(strings.TrimSpace(payload.Hash)),
	}
	if normalized.ID <= 0 || normalized.AuthDate <= 0 || normalized.Hash == "" {
		return TelegramLoginPayload{}, ErrTelegramAuthPayloadInvalid
	}
	return normalized, nil
}

func buildTelegramDataCheckString(payload TelegramLoginPayload) string {
	values := map[string]string{
		"auth_date": strconv.FormatInt(payload.AuthDate, 10),
		"id":        strconv.FormatInt(payload.ID, 10),
	}
	if payload.FirstName != "" {
		values["first_name"] = payload.FirstName
	}
	if payload.LastName != "" {
		values["last_name"] = payload.LastName
	}
	if payload.Username != "" {
		values["username"] = payload.Username
	}
	if payload.PhotoURL != "" {
		values["photo_url"] = payload.PhotoURL
	}

	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, fmt.Sprintf("%s=%s", key, values[key]))
	}
	return strings.Join(parts, "\n")
}

func buildTelegramHash(botToken, dataCheckString string) string {
	secret := sha256.Sum256([]byte(strings.TrimSpace(botToken)))
	mac := hmac.New(sha256.New, secret[:])
	_, _ = mac.Write([]byte(dataCheckString))
	return hex.EncodeToString(mac.Sum(nil))
}
