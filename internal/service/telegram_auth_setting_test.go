package service

import (
	"testing"

	"github.com/dujiao-next/internal/config"
	"github.com/dujiao-next/internal/constants"
)

func TestNormalizeTelegramAuthSetting(t *testing.T) {
	setting := NormalizeTelegramAuthSetting(TelegramAuthSetting{
		BotUsername:        " @demo_bot ",
		LoginExpireSeconds: 0,
		ReplayTTLSeconds:   10,
	})

	if setting.BotUsername != "demo_bot" {
		t.Fatalf("expected normalized username demo_bot, got %q", setting.BotUsername)
	}
	if setting.LoginExpireSeconds != 300 {
		t.Fatalf("expected default login expire 300, got %d", setting.LoginExpireSeconds)
	}
	if setting.ReplayTTLSeconds != 60 {
		t.Fatalf("expected minimum replay ttl 60, got %d", setting.ReplayTTLSeconds)
	}
}

func TestPatchTelegramAuthSettingKeepsTokenWhenEmpty(t *testing.T) {
	repo := newMockSettingRepo()
	svc := NewSettingService(repo)

	defaultCfg := config.TelegramAuthConfig{
		Enabled:            true,
		BotUsername:        "demo_bot",
		BotToken:           "secret-token",
		LoginExpireSeconds: 300,
		ReplayTTLSeconds:   300,
	}

	updated, err := svc.PatchTelegramAuthSetting(defaultCfg, TelegramAuthSettingPatch{
		BotUsername:        ptrString("@new_bot"),
		BotToken:           ptrString(""),
		LoginExpireSeconds: ptrInt(600),
		ReplayTTLSeconds:   ptrInt(900),
	})
	if err != nil {
		t.Fatalf("patch telegram auth setting failed: %v", err)
	}
	if updated.BotToken != "secret-token" {
		t.Fatalf("expected keep token secret-token, got %q", updated.BotToken)
	}
	if updated.BotUsername != "new_bot" {
		t.Fatalf("expected normalized username new_bot, got %q", updated.BotUsername)
	}

	saved, ok := repo.store[constants.SettingKeyTelegramAuthConfig]
	if !ok {
		t.Fatalf("telegram auth setting was not saved")
	}
	if saved["bot_token"] != "secret-token" {
		t.Fatalf("expected saved token keep old value, got %v", saved["bot_token"])
	}
}

func TestValidateTelegramAuthSetting(t *testing.T) {
	valid := NormalizeTelegramAuthSetting(TelegramAuthSetting{
		Enabled:            true,
		BotUsername:        "demo_bot",
		BotToken:           "secret",
		LoginExpireSeconds: 300,
		ReplayTTLSeconds:   300,
	})
	if err := ValidateTelegramAuthSetting(valid); err != nil {
		t.Fatalf("expected valid telegram auth config, got error: %v", err)
	}

	invalid := valid
	invalid.BotToken = ""
	if err := ValidateTelegramAuthSetting(invalid); err == nil {
		t.Fatal("expected validation error when enabled and token missing")
	}
}

func ptrInt(value int) *int {
	return &value
}
