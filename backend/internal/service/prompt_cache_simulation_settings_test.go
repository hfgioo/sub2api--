//go:build unit

package service

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/stretchr/testify/require"
)

func TestGetPromptCacheSimulationSettings_DefaultsWhenNotSet(t *testing.T) {
	repo := newMockSettingRepo()
	svc := NewSettingService(repo, &config.Config{})

	settings, err := svc.GetPromptCacheSimulationSettings(context.Background())
	require.NoError(t, err)
	require.False(t, settings.Enabled)
	require.True(t, settings.SemanticFirst)
	require.InDelta(t, 0.7, settings.FallbackReadRatio, 0.000001)
	require.InDelta(t, 0.2, settings.FallbackWriteRatio, 0.000001)
	require.Equal(t, 300, settings.TTLSeconds)
}

func TestGetPromptCacheSimulationSettings_ReadsFromDB(t *testing.T) {
	repo := newMockSettingRepo()
	data, _ := json.Marshal(PromptCacheSimulationSettings{
		Enabled:            true,
		SemanticFirst:      false,
		FallbackReadRatio:  0.55,
		FallbackWriteRatio: 0.15,
		TTLSeconds:         600,
	})
	repo.data[SettingKeyPromptCacheSimulationSettings] = string(data)
	svc := NewSettingService(repo, &config.Config{})

	settings, err := svc.GetPromptCacheSimulationSettings(context.Background())
	require.NoError(t, err)
	require.True(t, settings.Enabled)
	require.False(t, settings.SemanticFirst)
	require.InDelta(t, 0.55, settings.FallbackReadRatio, 0.000001)
	require.InDelta(t, 0.15, settings.FallbackWriteRatio, 0.000001)
	require.Equal(t, 600, settings.TTLSeconds)
}

func TestGetPromptCacheSimulationSettings_NormalizesInvalidStoredValues(t *testing.T) {
	repo := newMockSettingRepo()
	data, _ := json.Marshal(PromptCacheSimulationSettings{
		Enabled:            true,
		SemanticFirst:      true,
		FallbackReadRatio:  1.4,
		FallbackWriteRatio: 0.6,
		TTLSeconds:         0,
	})
	repo.data[SettingKeyPromptCacheSimulationSettings] = string(data)
	svc := NewSettingService(repo, &config.Config{})

	settings, err := svc.GetPromptCacheSimulationSettings(context.Background())
	require.NoError(t, err)
	require.InDelta(t, 1.0, settings.FallbackReadRatio, 0.000001)
	require.InDelta(t, 0.0, settings.FallbackWriteRatio, 0.000001)
	require.Equal(t, 60, settings.TTLSeconds)
}

func TestGetPromptCacheSimulationSettings_InvalidJSON_ReturnsDefaults(t *testing.T) {
	repo := newMockSettingRepo()
	repo.data[SettingKeyPromptCacheSimulationSettings] = "not-json"
	svc := NewSettingService(repo, &config.Config{})

	settings, err := svc.GetPromptCacheSimulationSettings(context.Background())
	require.NoError(t, err)
	require.False(t, settings.Enabled)
	require.True(t, settings.SemanticFirst)
	require.InDelta(t, 0.7, settings.FallbackReadRatio, 0.000001)
	require.InDelta(t, 0.2, settings.FallbackWriteRatio, 0.000001)
	require.Equal(t, 300, settings.TTLSeconds)
}

func TestSetPromptCacheSimulationSettings_Success(t *testing.T) {
	repo := newMockSettingRepo()
	svc := NewSettingService(repo, &config.Config{})

	err := svc.SetPromptCacheSimulationSettings(context.Background(), &PromptCacheSimulationSettings{
		Enabled:            true,
		SemanticFirst:      true,
		FallbackReadRatio:  0.6,
		FallbackWriteRatio: 0.25,
		TTLSeconds:         300,
	})
	require.NoError(t, err)

	settings, err := svc.GetPromptCacheSimulationSettings(context.Background())
	require.NoError(t, err)
	require.True(t, settings.Enabled)
	require.True(t, settings.SemanticFirst)
	require.InDelta(t, 0.6, settings.FallbackReadRatio, 0.000001)
	require.InDelta(t, 0.25, settings.FallbackWriteRatio, 0.000001)
	require.Equal(t, 300, settings.TTLSeconds)
}

func TestSetPromptCacheSimulationSettings_RejectsNil(t *testing.T) {
	svc := NewSettingService(newMockSettingRepo(), &config.Config{})
	err := svc.SetPromptCacheSimulationSettings(context.Background(), nil)
	require.Error(t, err)
}

func TestSetPromptCacheSimulationSettings_RejectsOutOfRangeReadRatio(t *testing.T) {
	svc := NewSettingService(newMockSettingRepo(), &config.Config{})

	for _, ratio := range []float64{-0.1, 1.1} {
		err := svc.SetPromptCacheSimulationSettings(context.Background(), &PromptCacheSimulationSettings{
			Enabled:            true,
			SemanticFirst:      true,
			FallbackReadRatio:  ratio,
			FallbackWriteRatio: 0.2,
			TTLSeconds:         300,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "fallback_read_ratio must be between 0-1")
	}
}

func TestSetPromptCacheSimulationSettings_RejectsOutOfRangeWriteRatio(t *testing.T) {
	svc := NewSettingService(newMockSettingRepo(), &config.Config{})

	for _, ratio := range []float64{-0.1, 1.1} {
		err := svc.SetPromptCacheSimulationSettings(context.Background(), &PromptCacheSimulationSettings{
			Enabled:            true,
			SemanticFirst:      true,
			FallbackReadRatio:  0.5,
			FallbackWriteRatio: ratio,
			TTLSeconds:         300,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "fallback_write_ratio must be between 0-1")
	}
}

func TestSetPromptCacheSimulationSettings_RejectsCombinedRatiosAboveOne(t *testing.T) {
	svc := NewSettingService(newMockSettingRepo(), &config.Config{})

	err := svc.SetPromptCacheSimulationSettings(context.Background(), &PromptCacheSimulationSettings{
		Enabled:            true,
		SemanticFirst:      true,
		FallbackReadRatio:  0.8,
		FallbackWriteRatio: 0.4,
		TTLSeconds:         300,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "fallback ratios must sum to 1 or less")
}

func TestSetPromptCacheSimulationSettings_RejectsTTLSecondsOutOfRange(t *testing.T) {
	svc := NewSettingService(newMockSettingRepo(), &config.Config{})

	for _, ttlSeconds := range []int{0, 59, 3601} {
		err := svc.SetPromptCacheSimulationSettings(context.Background(), &PromptCacheSimulationSettings{
			Enabled:            true,
			SemanticFirst:      true,
			FallbackReadRatio:  0.5,
			FallbackWriteRatio: 0.2,
			TTLSeconds:         ttlSeconds,
		})
		require.Error(t, err)
		require.Contains(t, err.Error(), "ttl_seconds must be between 60-3600")
	}
}

func TestDefaultPromptCacheSimulationSettings(t *testing.T) {
	settings := DefaultPromptCacheSimulationSettings()
	require.False(t, settings.Enabled)
	require.True(t, settings.SemanticFirst)
	require.InDelta(t, 0.7, settings.FallbackReadRatio, 0.000001)
	require.InDelta(t, 0.2, settings.FallbackWriteRatio, 0.000001)
	require.Equal(t, 300, settings.TTLSeconds)
}

func TestPromptCacheSimulationSettings_JSONRoundTrip(t *testing.T) {
	original := PromptCacheSimulationSettings{
		Enabled:            true,
		SemanticFirst:      false,
		FallbackReadRatio:  0.45,
		FallbackWriteRatio: 0.35,
		TTLSeconds:         900,
	}
	data, err := json.Marshal(original)
	require.NoError(t, err)

	var decoded PromptCacheSimulationSettings
	require.NoError(t, json.Unmarshal(data, &decoded))
	require.Equal(t, original, decoded)

	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))
	_, hasEnabled := raw["enabled"]
	_, hasSemanticFirst := raw["semantic_first"]
	_, hasReadRatio := raw["fallback_read_ratio"]
	_, hasWriteRatio := raw["fallback_write_ratio"]
	_, hasTTLSeconds := raw["ttl_seconds"]
	require.True(t, hasEnabled)
	require.True(t, hasSemanticFirst)
	require.True(t, hasReadRatio)
	require.True(t, hasWriteRatio)
	require.True(t, hasTTLSeconds)
}
