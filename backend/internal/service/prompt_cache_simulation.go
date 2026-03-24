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

	if settings.SemanticFirst {
		if semanticKey, ok := buildPromptCacheSimulationSemanticKey(parsed); ok {
			cacheKey := "semantic|" + scopeKey + "|" + semanticKey
			if _, hit := s.cache.Get(cacheKey); hit {
				s.cache.Set(cacheKey, true, decisionTTL)
				return &PromptCacheSimulationDecision{
					InputTokens:          0,
					CacheReadInputTokens: inputTokens,
				}, true
			}
			s.cache.Set(cacheKey, true, decisionTTL)
			return &PromptCacheSimulationDecision{
				InputTokens:              0,
				CacheCreationInputTokens: inputTokens,
				CacheCreation5mTokens:    inputTokens,
			}, true
		}
	}

	fallbackKey := buildPromptCacheSimulationFallbackKey(parsed)
	if fallbackKey == "" {
		return nil, false
	}
	cacheKey := "fallback|" + scopeKey + "|" + fallbackKey
	if _, hit := s.cache.Get(cacheKey); hit {
		readTokens := promptCacheSimulationRatioTokens(inputTokens, settings.FallbackReadRatio)
		if readTokens <= 0 {
			return nil, false
		}
		remainingInputTokens := inputTokens - readTokens
		if remainingInputTokens < 0 {
			remainingInputTokens = 0
		}
		s.cache.Set(cacheKey, true, decisionTTL)
		return &PromptCacheSimulationDecision{
			InputTokens:          remainingInputTokens,
			CacheReadInputTokens: readTokens,
		}, true
	}

	createTokens := promptCacheSimulationRatioTokens(inputTokens, settings.FallbackWriteRatio)
	if createTokens <= 0 {
		return nil, false
	}
	remainingInputTokens := inputTokens - createTokens
	if remainingInputTokens < 0 {
		remainingInputTokens = 0
	}
	s.cache.Set(cacheKey, true, decisionTTL)
	return &PromptCacheSimulationDecision{
		InputTokens:              remainingInputTokens,
		CacheCreationInputTokens: createTokens,
		CacheCreation5mTokens:    createTokens,
	}, true
}

func buildPromptCacheSimulationScopeKey(sessionIdentity, model string, parsed *ParsedRequest) string {
	sessionIdentity = strings.TrimSpace(sessionIdentity)
	model = strings.TrimSpace(model)
	if sessionIdentity == "" && parsed != nil {
		sessionIdentity = derivePromptCacheSimulationSessionIdentity(parsed)
	}
	if sessionIdentity == "" {
		return ""
	}
	return sessionIdentity + "|model:" + model
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

func buildPromptCacheSimulationSemanticKey(parsed *ParsedRequest) (string, bool) {
	full := strings.TrimSpace(extractPromptCacheSimulationFullText(parsed))
	cacheable := strings.TrimSpace(extractPromptCacheSimulationCacheableText(parsed))
	if full == "" || cacheable == "" || full != cacheable {
		return "", false
	}
	return promptCacheSimulationHashString(full), true
}

func extractPromptCacheSimulationFullText(parsed *ParsedRequest) string {
	if parsed == nil {
		return ""
	}

	var builder strings.Builder
	appendPromptCacheSimulationTools(&builder, parsed.Body, false)
	appendPromptCacheSimulationSystem(&builder, parsed.System, false)
	appendPromptCacheSimulationMessages(&builder, parsed.Messages, false)
	return builder.String()
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
