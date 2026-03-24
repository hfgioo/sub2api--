package service

import (
	"context"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/cespare/xxhash/v2"
	gocache "github.com/patrickmn/go-cache"
	"github.com/tidwall/gjson"
)

type PromptCacheSimulationDecision struct {
	InputTokens              int
	CacheCreationInputTokens int
	CacheReadInputTokens     int
	CacheCreation5mTokens    int
}

func (d *PromptCacheSimulationDecision) HasSimulation() bool {
	if d == nil {
		return false
	}
	return d.CacheCreationInputTokens > 0 || d.CacheReadInputTokens > 0 || d.CacheCreation5mTokens > 0
}

type PromptCacheSimulationService struct {
	settingService *SettingService
	cache          *gocache.Cache
}

type promptCacheSimulationCacheEntry struct {
	CachedTokens int
}

func NewPromptCacheSimulationService(settingService *SettingService) *PromptCacheSimulationService {
	return &PromptCacheSimulationService{
		settingService: settingService,
		cache:          gocache.New(5*time.Minute, time.Minute),
	}
}

func (s *PromptCacheSimulationService) Simulate(ctx context.Context, requestPath string, parsed *ParsedRequest, sessionIdentity, model string, inputTokens int) (*PromptCacheSimulationDecision, bool) {
	if s == nil || s.settingService == nil || parsed == nil || inputTokens <= 0 {
		return nil, false
	}
	if strings.TrimSpace(requestPath) != "/v1/messages" {
		return nil, false
	}
	if !strings.EqualFold(strings.TrimSpace(parsed.Model), strings.TrimSpace(model)) {
		return nil, false
	}

	settings, err := s.settingService.GetPromptCacheSimulationSettings(ctx)
	if err != nil || settings == nil || !settings.Enabled {
		return nil, false
	}

	scopeKey := buildPromptCacheSimulationScopeKey(sessionIdentity, model, parsed)
	if scopeKey == "" {
		return nil, false
	}
	decisionTTL := time.Duration(settings.TTLSeconds) * time.Second
	useSemanticHighWater := settings.SemanticFirst && promptCacheSimulationHasCacheControl(parsed)

	cacheKey := buildPromptCacheSimulationCacheKey(scopeKey, parsed)
	if cacheKey == "" {
		return nil, false
	}

	if useSemanticHighWater {
		cacheableTokens := promptCacheSimulationRatioTokens(inputTokens, settings.HitRatio)
		if cacheableTokens <= 0 {
			return nil, false
		}
		uncachedTokens := inputTokens - cacheableTokens
		if uncachedTokens < 0 {
			uncachedTokens = 0
		}

		cachedTokens := 0
		if cached, hit := s.cache.Get(cacheKey); hit {
			switch entry := cached.(type) {
			case promptCacheSimulationCacheEntry:
				cachedTokens = entry.CachedTokens
			case *promptCacheSimulationCacheEntry:
				if entry != nil {
					cachedTokens = entry.CachedTokens
				}
			}
			if cachedTokens < 0 {
				cachedTokens = 0
			}
			if cachedTokens > cacheableTokens {
				cachedTokens = cacheableTokens
			}
		}

		cacheCreationTokens := cacheableTokens - cachedTokens
		if cacheCreationTokens < 0 {
			cacheCreationTokens = 0
		}

		s.cache.Set(cacheKey, promptCacheSimulationCacheEntry{CachedTokens: cacheableTokens}, decisionTTL)
		if cachedTokens == 0 && cacheCreationTokens == 0 {
			return nil, false
		}

		return &PromptCacheSimulationDecision{
			InputTokens:              uncachedTokens,
			CacheCreationInputTokens: cacheCreationTokens,
			CacheReadInputTokens:     cachedTokens,
			CacheCreation5mTokens:    cacheCreationTokens,
		}, true
	}

	cacheReadTokens := 0
	cacheCreationTokens := 0
	if _, hit := s.cache.Get(cacheKey); hit {
		cacheReadTokens = promptCacheSimulationRatioTokens(inputTokens, settings.FallbackReadRatio*settings.HitRatio)
		s.cache.Set(cacheKey, promptCacheSimulationCacheEntry{CachedTokens: cacheReadTokens}, decisionTTL)
	} else {
		cacheCreationTokens = promptCacheSimulationRatioTokens(inputTokens, settings.FallbackWriteRatio*settings.HitRatio)
		s.cache.Set(cacheKey, promptCacheSimulationCacheEntry{CachedTokens: cacheCreationTokens}, decisionTTL)
	}
	uncachedTokens := inputTokens - cacheReadTokens - cacheCreationTokens
	if uncachedTokens < 0 {
		uncachedTokens = 0
	}
	if cacheReadTokens == 0 && cacheCreationTokens == 0 {
		return nil, false
	}

	return &PromptCacheSimulationDecision{
		InputTokens:              uncachedTokens,
		CacheCreationInputTokens: cacheCreationTokens,
		CacheReadInputTokens:     cacheReadTokens,
		CacheCreation5mTokens:    cacheCreationTokens,
	}, true
}

func buildPromptCacheSimulationScopeKey(sessionIdentity, model string, parsed *ParsedRequest) string {
	effectiveSessionIdentity := strings.TrimSpace(sessionIdentity)
	if effectiveSessionIdentity == "" {
		effectiveSessionIdentity = strings.TrimSpace(derivePromptCacheSimulationSessionIdentity(parsed))
	}
	if effectiveSessionIdentity == "" {
		return ""
	}
	normalizedModel := strings.ToLower(strings.TrimSpace(model))
	if normalizedModel == "" && parsed != nil {
		normalizedModel = strings.ToLower(strings.TrimSpace(parsed.Model))
	}
	if normalizedModel == "" {
		return ""
	}
	// scope key must be stable across all turns of the same conversation;
	// do NOT mix in message content or turn-level data here.
	return promptCacheSimulationHashString(effectiveSessionIdentity + "|" + normalizedModel)
}

func buildPromptCacheSimulationCacheKey(scopeKey string, parsed *ParsedRequest) string {
	if scopeKey == "" || parsed == nil {
		return ""
	}
	// If the request has any cache_control markers, use the stable semantic key
	// (scopeKey alone) so the same session always hits the same cache entry across
	// growing conversations.
	if promptCacheSimulationHasCacheControl(parsed) {
		return "semantic|" + scopeKey
	}
	// No cache_control: fall back to content-hash keying.
	fallbackKey := buildPromptCacheSimulationFallbackKey(parsed)
	if fallbackKey == "" {
		return ""
	}
	return "fallback|" + scopeKey + "|" + fallbackKey
}

func promptCacheSimulationHasCacheControl(parsed *ParsedRequest) bool {
	if parsed == nil {
		return false
	}
	if promptCacheSimulationToolsHaveAnyBoundary(parsed.Body) {
		return true
	}
	if promptCacheSimulationSystemHasAnyBoundary(parsed.System) {
		return true
	}
	return promptCacheSimulationMessagesHaveAnyCacheControl(parsed.Messages)
}

func promptCacheSimulationToolsHaveAnyBoundary(body []byte) bool {
	if len(body) == 0 {
		return false
	}
	tools := gjson.GetBytes(body, "tools")
	if !tools.Exists() || !tools.IsArray() {
		return false
	}
	found := false
	tools.ForEach(func(_, value gjson.Result) bool {
		if value.Get("cache_control.type").String() == "ephemeral" {
			found = true
			return false
		}
		return true
	})
	return found
}

func promptCacheSimulationSystemHasAnyBoundary(system any) bool {
	parts, ok := system.([]any)
	if !ok {
		return false
	}
	for _, part := range parts {
		partMap, ok := part.(map[string]any)
		if !ok {
			continue
		}
		if _, ok := partMap["cache_control"].(map[string]any); ok {
			return true
		}
	}
	return false
}

func promptCacheSimulationMessagesHaveAnyCacheControl(messages []any) bool {
	for _, msg := range messages {
		msgMap, ok := msg.(map[string]any)
		if !ok {
			continue
		}
		parts, ok := msgMap["content"].([]any)
		if !ok {
			continue
		}
		for _, part := range parts {
			partMap, ok := part.(map[string]any)
			if !ok {
				continue
			}
			if _, ok := partMap["cache_control"].(map[string]any); ok {
				return true
			}
		}
	}
	return false
}

func derivePromptCacheSimulationSessionIdentity(parsed *ParsedRequest) string {
	if parsed == nil {
		return ""
	}
	if parsed.MetadataUserID != "" {
		return "metadata:" + strings.TrimSpace(parsed.MetadataUserID)
	}
	if parsed.SessionContext != nil {
		return strings.Join([]string{
			"session",
			strings.TrimSpace(parsed.SessionContext.ClientIP),
			strings.TrimSpace(parsed.SessionContext.UserAgent),
			strconv.FormatInt(parsed.SessionContext.APIKeyID, 10),
		}, "|")
	}
	if len(parsed.Body) > 0 {
		return "body:" + promptCacheSimulationHashBytes(parsed.Body)
	}
	return ""
}

func buildPromptCacheSimulationFallbackKey(parsed *ParsedRequest) string {
	cacheable := strings.TrimSpace(extractPromptCacheSimulationCacheableText(parsed))
	if cacheable == "" {
		return ""
	}
	return promptCacheSimulationHashString(cacheable)
}

func extractPromptCacheSimulationCacheableText(parsed *ParsedRequest) string {
	if parsed == nil {
		return ""
	}

	var builder strings.Builder
	appendPromptCacheSimulationTools(&builder, parsed.Body, true)
	appendPromptCacheSimulationSystem(&builder, parsed.System, true)
	appendPromptCacheSimulationMessages(&builder, parsed.Messages, true)
	return builder.String()
}

func appendPromptCacheSimulationTools(builder *strings.Builder, body []byte, cacheableOnly bool) {
	if builder == nil || len(body) == 0 {
		return
	}
	tools := gjson.GetBytes(body, "tools")
	if !tools.Exists() || !tools.IsArray() {
		return
	}
	tools.ForEach(func(_, value gjson.Result) bool {
		if cacheableOnly && value.Get("cache_control.type").String() != "ephemeral" {
			return true
		}
		builder.WriteString(value.Raw)
		builder.WriteString("\n")
		return true
	})
}

func appendPromptCacheSimulationSystem(builder *strings.Builder, system any, cacheableOnly bool) {
	if builder == nil || system == nil {
		return
	}
	if !cacheableOnly {
		builder.WriteString(extractPromptCacheSimulationTextValue(system))
		return
	}
	parts, ok := system.([]any)
	if !ok {
		return
	}
	for _, part := range parts {
		partMap, ok := part.(map[string]any)
		if !ok {
			continue
		}
		cc, ok := partMap["cache_control"].(map[string]any)
		if !ok || cc["type"] != "ephemeral" {
			continue
		}
		if text, _ := partMap["text"].(string); text != "" {
			builder.WriteString(text)
		}
	}
}

func appendPromptCacheSimulationMessages(builder *strings.Builder, messages []any, cacheableOnly bool) {
	if builder == nil {
		return
	}
	for _, msg := range messages {
		msgMap, ok := msg.(map[string]any)
		if !ok {
			continue
		}
		content, exists := msgMap["content"]
		if !exists {
			continue
		}
		if !cacheableOnly {
			builder.WriteString(extractPromptCacheSimulationTextValue(content))
			continue
		}
		parts, ok := content.([]any)
		if !ok {
			continue
		}
		for _, part := range parts {
			partMap, ok := part.(map[string]any)
			if !ok {
				continue
			}
			cc, ok := partMap["cache_control"].(map[string]any)
			if !ok || cc["type"] != "ephemeral" {
				continue
			}
			if partMap["type"] == "text" {
				if text, _ := partMap["text"].(string); text != "" {
					builder.WriteString(text)
				}
			}
		}
	}
}

func extractPromptCacheSimulationTextValue(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case []any:
		var builder strings.Builder
		for _, part := range v {
			partMap, ok := part.(map[string]any)
			if !ok {
				continue
			}
			if partMap["type"] == "text" {
				if text, _ := partMap["text"].(string); text != "" {
					builder.WriteString(text)
				}
			}
		}
		return builder.String()
	default:
		return ""
	}
}

func promptCacheSimulationRatioTokens(inputTokens int, ratio float64) int {
	if inputTokens <= 0 || ratio <= 0 {
		return 0
	}
	value := int(math.Round(float64(inputTokens) * ratio))
	if value < 0 {
		return 0
	}
	if value > inputTokens {
		return inputTokens
	}
	return value
}

func promptCacheSimulationHashString(value string) string {
	return promptCacheSimulationHashBytes([]byte(value))
}

func promptCacheSimulationHashBytes(value []byte) string {
	return strconv.FormatUint(xxhash.Sum64(value), 36)
}
