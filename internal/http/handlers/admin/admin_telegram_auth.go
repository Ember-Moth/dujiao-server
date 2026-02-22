package admin

import (
	"errors"

	"github.com/dujiao-next/internal/cache"
	"github.com/dujiao-next/internal/http/response"
	"github.com/dujiao-next/internal/service"

	"github.com/gin-gonic/gin"
)

// GetTelegramAuthSettings 获取 Telegram 登录配置（脱敏）
func (h *Handler) GetTelegramAuthSettings(c *gin.Context) {
	setting, err := h.SettingService.GetTelegramAuthSetting(h.Config.TelegramAuth)
	if err != nil {
		respondError(c, response.CodeInternal, "error.settings_fetch_failed", err)
		return
	}
	response.Success(c, service.MaskTelegramAuthSettingForAdmin(setting))
}

// UpdateTelegramAuthSettings 更新 Telegram 登录配置
func (h *Handler) UpdateTelegramAuthSettings(c *gin.Context) {
	var req service.TelegramAuthSettingPatch
	if err := c.ShouldBindJSON(&req); err != nil {
		respondError(c, response.CodeBadRequest, "error.bad_request", err)
		return
	}

	setting, err := h.SettingService.PatchTelegramAuthSetting(h.Config.TelegramAuth, req)
	if err != nil {
		switch {
		case errors.Is(err, service.ErrTelegramAuthConfigInvalid):
			respondErrorWithMsg(c, response.CodeBadRequest, err.Error(), nil)
		default:
			respondError(c, response.CodeInternal, "error.settings_save_failed", err)
		}
		return
	}

	h.Config.TelegramAuth = service.TelegramAuthSettingToConfig(setting)
	if h.TelegramAuthService != nil {
		h.TelegramAuthService.SetConfig(h.Config.TelegramAuth)
	}
	_ = cache.Del(c.Request.Context(), publicConfigCacheKey)

	response.Success(c, service.MaskTelegramAuthSettingForAdmin(setting))
}
