package service

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Wei-Shaw/sub2api/internal/config"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// ---------- reconcileCachedTokens 单元测试 ----------

func TestReconcileCachedTokens_NilUsage(t *testing.T) {
	assert.False(t, reconcileCachedTokens(nil))
}

func TestReconcileCachedTokens_AlreadyHasCacheRead(t *testing.T) {
	// 已有标准字段，不应覆盖
	usage := map[string]any{
		"cache_read_input_tokens": float64(100),
		"cached_tokens":           float64(50),
	}
	assert.False(t, reconcileCachedTokens(usage))
	assert.Equal(t, float64(100), usage["cache_read_input_tokens"])
}

func TestReconcileCachedTokens_KimiStyle(t *testing.T) {
	// Kimi 风格：cache_read_input_tokens=0，cached_tokens>0
	usage := map[string]any{
		"input_tokens":                float64(23),
		"cache_creation_input_tokens": float64(0),
		"cache_read_input_tokens":     float64(0),
		"cached_tokens":               float64(23),
	}
	assert.True(t, reconcileCachedTokens(usage))
	assert.Equal(t, float64(23), usage["cache_read_input_tokens"])
}

func TestReconcileCachedTokens_NoCachedTokens(t *testing.T) {
	// 无 cached_tokens 字段（原生 Claude）
	usage := map[string]any{
		"input_tokens":                float64(100),
		"cache_read_input_tokens":     float64(0),
		"cache_creation_input_tokens": float64(0),
	}
	assert.False(t, reconcileCachedTokens(usage))
	assert.Equal(t, float64(0), usage["cache_read_input_tokens"])
}

func TestReconcileCachedTokens_CachedTokensZero(t *testing.T) {
	// cached_tokens 为 0，不应覆盖
	usage := map[string]any{
		"cache_read_input_tokens": float64(0),
		"cached_tokens":           float64(0),
	}
	assert.False(t, reconcileCachedTokens(usage))
	assert.Equal(t, float64(0), usage["cache_read_input_tokens"])
}

func TestReconcileCachedTokens_MissingCacheReadField(t *testing.T) {
	// cache_read_input_tokens 字段完全不存在，cached_tokens > 0
	usage := map[string]any{
		"cached_tokens": float64(42),
	}
	assert.True(t, reconcileCachedTokens(usage))
	assert.Equal(t, float64(42), usage["cache_read_input_tokens"])
}

// ---------- 流式 message_start 事件 reconcile 测试 ----------

func TestStreamingReconcile_MessageStart(t *testing.T) {
	// 模拟 Kimi 返回的 message_start SSE 事件
	eventJSON := `{
		"type": "message_start",
		"message": {
			"id": "msg_123",
			"type": "message",
			"role": "assistant",
			"model": "kimi",
			"usage": {
				"input_tokens": 23,
				"cache_creation_input_tokens": 0,
				"cache_read_input_tokens": 0,
				"cached_tokens": 23
			}
		}
	}`

	var event map[string]any
	require.NoError(t, json.Unmarshal([]byte(eventJSON), &event))

	eventType, _ := event["type"].(string)
	require.Equal(t, "message_start", eventType)

	// 模拟 processSSEEvent 中的 reconcile 逻辑
	if msg, ok := event["message"].(map[string]any); ok {
		if u, ok := msg["usage"].(map[string]any); ok {
			reconcileCachedTokens(u)
		}
	}

	// 验证 cache_read_input_tokens 已被填充
	msg, ok := event["message"].(map[string]any)
	require.True(t, ok)
	usage, ok := msg["usage"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, float64(23), usage["cache_read_input_tokens"])

	// 验证重新序列化后 JSON 也包含正确值
	data, err := json.Marshal(event)
	require.NoError(t, err)
	assert.Equal(t, int64(23), gjson.GetBytes(data, "message.usage.cache_read_input_tokens").Int())
}

func TestStreamingReconcile_MessageStart_NativeClaude(t *testing.T) {
	// 原生 Claude 不返回 cached_tokens，reconcile 不应改变任何值
	eventJSON := `{
		"type": "message_start",
		"message": {
			"usage": {
				"input_tokens": 100,
				"cache_creation_input_tokens": 50,
				"cache_read_input_tokens": 30
			}
		}
	}`

	var event map[string]any
	require.NoError(t, json.Unmarshal([]byte(eventJSON), &event))

	if msg, ok := event["message"].(map[string]any); ok {
		if u, ok := msg["usage"].(map[string]any); ok {
			reconcileCachedTokens(u)
		}
	}

	msg, ok := event["message"].(map[string]any)
	require.True(t, ok)
	usage, ok := msg["usage"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, float64(30), usage["cache_read_input_tokens"])
}

// ---------- 流式 message_delta 事件 reconcile 测试 ----------

func TestStreamingReconcile_MessageDelta(t *testing.T) {
	// 模拟 Kimi 返回的 message_delta SSE 事件
	eventJSON := `{
		"type": "message_delta",
		"usage": {
			"output_tokens": 7,
			"cache_read_input_tokens": 0,
			"cached_tokens": 15
		}
	}`

	var event map[string]any
	require.NoError(t, json.Unmarshal([]byte(eventJSON), &event))

	eventType, _ := event["type"].(string)
	require.Equal(t, "message_delta", eventType)

	// 模拟 processSSEEvent 中的 reconcile 逻辑
	usage, ok := event["usage"].(map[string]any)
	require.True(t, ok)
	reconcileCachedTokens(usage)
	assert.Equal(t, float64(15), usage["cache_read_input_tokens"])
}

func TestStreamingReconcile_MessageDelta_NativeClaude(t *testing.T) {
	// 原生 Claude 的 message_delta 通常没有 cached_tokens
	eventJSON := `{
		"type": "message_delta",
		"usage": {
			"output_tokens": 50
		}
	}`

	var event map[string]any
	require.NoError(t, json.Unmarshal([]byte(eventJSON), &event))

	usage, ok := event["usage"].(map[string]any)
	require.True(t, ok)
	reconcileCachedTokens(usage)
	_, hasCacheRead := usage["cache_read_input_tokens"]
	assert.False(t, hasCacheRead, "不应为原生 Claude 响应注入 cache_read_input_tokens")
}

// ---------- 非流式响应 reconcile 测试 ----------

func TestNonStreamingReconcile_KimiResponse(t *testing.T) {
	// 模拟 Kimi 非流式响应
	body := []byte(`{
		"id": "msg_123",
		"type": "message",
		"role": "assistant",
		"content": [{"type": "text", "text": "hello"}],
		"model": "kimi",
		"usage": {
			"input_tokens": 23,
			"output_tokens": 7,
			"cache_creation_input_tokens": 0,
			"cache_read_input_tokens": 0,
			"cached_tokens": 23,
			"prompt_tokens": 23,
			"completion_tokens": 7
		}
	}`)

	// 模拟 handleNonStreamingResponse 中的逻辑
	var response struct {
		Usage ClaudeUsage `json:"usage"`
	}
	require.NoError(t, json.Unmarshal(body, &response))

	// reconcile
	if response.Usage.CacheReadInputTokens == 0 {
		cachedTokens := gjson.GetBytes(body, "usage.cached_tokens").Int()
		if cachedTokens > 0 {
			response.Usage.CacheReadInputTokens = int(cachedTokens)
			if newBody, err := sjson.SetBytes(body, "usage.cache_read_input_tokens", cachedTokens); err == nil {
				body = newBody
			}
		}
	}

	// 验证内部 usage（计费用）
	assert.Equal(t, 23, response.Usage.CacheReadInputTokens)
	assert.Equal(t, 23, response.Usage.InputTokens)
	assert.Equal(t, 7, response.Usage.OutputTokens)

	// 验证返回给客户端的 JSON body
	assert.Equal(t, int64(23), gjson.GetBytes(body, "usage.cache_read_input_tokens").Int())
}

func TestNonStreamingReconcile_NativeClaude(t *testing.T) {
	// 原生 Claude 响应：cache_read_input_tokens 已有值
	body := []byte(`{
		"usage": {
			"input_tokens": 100,
			"output_tokens": 50,
			"cache_creation_input_tokens": 20,
			"cache_read_input_tokens": 30
		}
	}`)

	var response struct {
		Usage ClaudeUsage `json:"usage"`
	}
	require.NoError(t, json.Unmarshal(body, &response))

	// CacheReadInputTokens == 30，条件不成立，整个 reconcile 分支不会执行
	assert.NotZero(t, response.Usage.CacheReadInputTokens)
	assert.Equal(t, 30, response.Usage.CacheReadInputTokens)
}

func TestPromptCacheSimulationService_SemanticFirstFullPromptCreatesThenReads(t *testing.T) {
	repo := newMockSettingRepo()
	data, err := json.Marshal(PromptCacheSimulationSettings{
		Enabled:            true,
		SemanticFirst:      true,
		FallbackReadRatio:  0.7,
		FallbackWriteRatio: 0.2,
		TTLSeconds:         300,
	})
	require.NoError(t, err)
	repo.data[SettingKeyPromptCacheSimulationSettings] = string(data)

	settingSvc := NewSettingService(repo, nil)
	simSvc := NewPromptCacheSimulationService(settingSvc)
	parsed, err := ParseGatewayRequest([]byte(`{
		"model":"claude-sonnet-4",
		"system":[{"type":"text","text":"sys","cache_control":{"type":"ephemeral"}}],
		"messages":[{"role":"user","content":[{"type":"text","text":"hello","cache_control":{"type":"ephemeral"}}]}],
		"tools":[{"name":"lookup","input_schema":{"type":"object"},"cache_control":{"type":"ephemeral"}}]
	}`), PlatformAnthropic)
	require.NoError(t, err)
	parsed.MetadataUserID = "user-1"

	first, ok := simSvc.Simulate(context.Background(), "/v1/messages", parsed, "metadata:user-1", "claude-sonnet-4", 100)
	require.True(t, ok)
	require.Zero(t, first.InputTokens)
	require.Equal(t, 100, first.CacheCreationInputTokens)
	require.Equal(t, 100, first.CacheCreation5mTokens)
	require.Zero(t, first.CacheReadInputTokens)

	second, ok := simSvc.Simulate(context.Background(), "/v1/messages", parsed, "metadata:user-1", "claude-sonnet-4", 100)
	require.True(t, ok)
	require.Zero(t, second.InputTokens)
	require.Zero(t, second.CacheCreationInputTokens)
	require.Equal(t, 100, second.CacheReadInputTokens)
}

func TestPromptCacheSimulationService_FallbackUsesRatiosWhenSemanticUnavailable(t *testing.T) {
	repo := newMockSettingRepo()
	data, err := json.Marshal(PromptCacheSimulationSettings{
		Enabled:            true,
		SemanticFirst:      true,
		FallbackReadRatio:  0.7,
		FallbackWriteRatio: 0.2,
		TTLSeconds:         300,
	})
	require.NoError(t, err)
	repo.data[SettingKeyPromptCacheSimulationSettings] = string(data)

	settingSvc := NewSettingService(repo, nil)
	simSvc := NewPromptCacheSimulationService(settingSvc)
	parsed, err := ParseGatewayRequest([]byte(`{
		"model":"claude-sonnet-4",
		"system":[{"type":"text","text":"uncached system"}],
		"messages":[{"role":"user","content":[{"type":"text","text":"hello","cache_control":{"type":"ephemeral"}},{"type":"text","text":"tail"}]}]
	}`), PlatformAnthropic)
	require.NoError(t, err)
	parsed.MetadataUserID = "user-1"

	first, ok := simSvc.Simulate(context.Background(), "/v1/messages", parsed, "metadata:user-1", "claude-sonnet-4", 100)
	require.True(t, ok)
	require.Equal(t, 80, first.InputTokens)
	require.Equal(t, 20, first.CacheCreationInputTokens)
	require.Equal(t, 20, first.CacheCreation5mTokens)
	require.Zero(t, first.CacheReadInputTokens)

	second, ok := simSvc.Simulate(context.Background(), "/v1/messages", parsed, "metadata:user-1", "claude-sonnet-4", 100)
	require.True(t, ok)
	require.Equal(t, 30, second.InputTokens)
	require.Zero(t, second.CacheCreationInputTokens)
	require.Zero(t, second.CacheCreation5mTokens)
	require.Equal(t, 70, second.CacheReadInputTokens)
}

func TestPromptCacheSimulationService_IsolatedBySessionAndModel(t *testing.T) {
	repo := newMockSettingRepo()
	data, err := json.Marshal(PromptCacheSimulationSettings{
		Enabled:            true,
		SemanticFirst:      true,
		FallbackReadRatio:  0.7,
		FallbackWriteRatio: 0.2,
		TTLSeconds:         300,
	})
	require.NoError(t, err)
	repo.data[SettingKeyPromptCacheSimulationSettings] = string(data)

	settingSvc := NewSettingService(repo, nil)
	simSvc := NewPromptCacheSimulationService(settingSvc)
	parsed, err := ParseGatewayRequest([]byte(`{
		"model":"claude-sonnet-4",
		"messages":[{"role":"user","content":[{"type":"text","text":"hello","cache_control":{"type":"ephemeral"}}]}]
	}`), PlatformAnthropic)
	require.NoError(t, err)
	parsedOtherModel, err := ParseGatewayRequest([]byte(`{
		"model":"claude-opus-4",
		"messages":[{"role":"user","content":[{"type":"text","text":"hello","cache_control":{"type":"ephemeral"}}]}]
	}`), PlatformAnthropic)
	require.NoError(t, err)

	first, ok := simSvc.Simulate(context.Background(), "/v1/messages", parsed, "metadata:user-a", "claude-sonnet-4", 50)
	require.True(t, ok)
	require.Zero(t, first.InputTokens)
	require.Equal(t, 50, first.CacheCreationInputTokens)

	otherSession, ok := simSvc.Simulate(context.Background(), "/v1/messages", parsed, "metadata:user-b", "claude-sonnet-4", 50)
	require.True(t, ok)
	require.Zero(t, otherSession.InputTokens)
	require.Equal(t, 50, otherSession.CacheCreationInputTokens)

	otherModel, ok := simSvc.Simulate(context.Background(), "/v1/messages", parsedOtherModel, "metadata:user-a", "claude-opus-4", 50)
	require.True(t, ok)
	require.Zero(t, otherModel.InputTokens)
	require.Equal(t, 50, otherModel.CacheCreationInputTokens)
}

func TestPromptCacheSimulationService_PreservesUpstreamUsageAndFallsBackToSessionContext(t *testing.T) {
	repo := newMockSettingRepo()
	data, err := json.Marshal(PromptCacheSimulationSettings{
		Enabled:            true,
		SemanticFirst:      true,
		FallbackReadRatio:  0.7,
		FallbackWriteRatio: 0.2,
		TTLSeconds:         300,
	})
	require.NoError(t, err)
	repo.data[SettingKeyPromptCacheSimulationSettings] = string(data)

	settingSvc := NewSettingService(repo, &config.Config{})
	svc := NewGatewayService(
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		&RateLimitService{},
		nil,
		&config.Config{},
		nil,
		nil,
		NewBillingService(&config.Config{}, nil),
		nil,
		&BillingCacheService{},
		nil,
		nil,
		&DeferredService{},
		nil,
		nil,
		nil,
		settingSvc,
		nil,
	)

	parsed, err := ParseGatewayRequest([]byte(`{
		"model":"claude-sonnet-4",
		"messages":[{"role":"user","content":[{"type":"text","text":"hello","cache_control":{"type":"ephemeral"}}]}]
	}`), PlatformAnthropic)
	require.NoError(t, err)
	parsed.SessionContext = &SessionContext{ClientIP: "10.0.0.1", UserAgent: "ua-test", APIKeyID: 42}

	upstreamUsage := &ClaudeUsage{InputTokens: 100, CacheReadInputTokens: 9}
	require.False(t, svc.applyPromptCacheSimulationToUsage(context.Background(), "/v1/messages", parsed, "claude-sonnet-4", upstreamUsage))
	require.Equal(t, 100, upstreamUsage.InputTokens)
	require.Equal(t, 9, upstreamUsage.CacheReadInputTokens)
	require.Zero(t, upstreamUsage.CacheCreationInputTokens)

	first := &ClaudeUsage{InputTokens: 100}
	require.True(t, svc.applyPromptCacheSimulationToUsage(context.Background(), "/v1/messages", parsed, "claude-sonnet-4", first))
	require.Zero(t, first.InputTokens)
	require.Equal(t, 100, first.CacheCreationInputTokens)
	require.Equal(t, 100, first.CacheCreation5mTokens)
	require.Zero(t, first.CacheReadInputTokens)

	second := &ClaudeUsage{InputTokens: 100}
	require.True(t, svc.applyPromptCacheSimulationToUsage(context.Background(), "/v1/messages", parsed, "claude-sonnet-4", second))
	require.Zero(t, second.InputTokens)
	require.Equal(t, 100, second.CacheReadInputTokens)
	require.Zero(t, second.CacheCreationInputTokens)
}

func TestPromptCacheSimulationService_DisabledForNonAnthropicUsageShapes(t *testing.T) {
	service := &PromptCacheSimulationService{}
	decision, ok := service.Simulate(context.Background(), "/v1/messages", nil, "metadata:user-1", "claude-sonnet-4", 100)
	require.False(t, ok)
	require.Nil(t, decision)
}

func TestPromptCacheSimulationService_DoesNotSimulateOutsideMessagesPath(t *testing.T) {
	repo := newMockSettingRepo()
	data, err := json.Marshal(PromptCacheSimulationSettings{
		Enabled:            true,
		SemanticFirst:      true,
		FallbackReadRatio:  0.7,
		FallbackWriteRatio: 0.2,
		TTLSeconds:         300,
	})
	require.NoError(t, err)
	repo.data[SettingKeyPromptCacheSimulationSettings] = string(data)

	settingSvc := NewSettingService(repo, nil)
	service := NewPromptCacheSimulationService(settingSvc)
	parsed, err := ParseGatewayRequest([]byte(`{
		"model":"claude-sonnet-4",
		"messages":[{"role":"user","content":[{"type":"text","text":"hello","cache_control":{"type":"ephemeral"}}]}],
		"metadata":{"user_id":"user-1"}
	}`), PlatformAnthropic)
	require.NoError(t, err)

	decision, ok := service.Simulate(context.Background(), "/v1/complete", parsed, "metadata:user-1", "claude-sonnet-4", 100)
	require.False(t, ok)
	require.Nil(t, decision)
}

func TestPromptCacheSimulationService_DoesNotSimulateForModelMismatch(t *testing.T) {
	repo := newMockSettingRepo()
	data, err := json.Marshal(PromptCacheSimulationSettings{
		Enabled:            true,
		SemanticFirst:      true,
		FallbackReadRatio:  0.7,
		FallbackWriteRatio: 0.2,
		TTLSeconds:         300,
	})
	require.NoError(t, err)
	repo.data[SettingKeyPromptCacheSimulationSettings] = string(data)

	settingSvc := NewSettingService(repo, nil)
	service := NewPromptCacheSimulationService(settingSvc)
	parsed, err := ParseGatewayRequest([]byte(`{
		"model":"claude-sonnet-4",
		"messages":[{"role":"user","content":[{"type":"text","text":"hello","cache_control":{"type":"ephemeral"}}]}],
		"metadata":{"user_id":"user-1"}
	}`), PlatformAnthropic)
	require.NoError(t, err)

	decision, ok := service.Simulate(context.Background(), "/v1/messages", parsed, "metadata:user-1", "claude-opus-4", 100)
	require.False(t, ok)
	require.Nil(t, decision)
}

func TestNonStreamingReconcile_NoCachedTokens(t *testing.T) {
	// 没有 cached_tokens 字段
	body := []byte(`{
		"usage": {
			"input_tokens": 100,
			"output_tokens": 50,
			"cache_creation_input_tokens": 0,
			"cache_read_input_tokens": 0
		}
	}`)

	var response struct {
		Usage ClaudeUsage `json:"usage"`
	}
	require.NoError(t, json.Unmarshal(body, &response))

	if response.Usage.CacheReadInputTokens == 0 {
		cachedTokens := gjson.GetBytes(body, "usage.cached_tokens").Int()
		if cachedTokens > 0 {
			response.Usage.CacheReadInputTokens = int(cachedTokens)
			if newBody, err := sjson.SetBytes(body, "usage.cache_read_input_tokens", cachedTokens); err == nil {
				body = newBody
			}
		}
	}

	// cache_read_input_tokens 应保持为 0
	assert.Equal(t, 0, response.Usage.CacheReadInputTokens)
	assert.Equal(t, int64(0), gjson.GetBytes(body, "usage.cache_read_input_tokens").Int())
}

func TestHandleNonStreamingResponse_PromptCacheSimulationPatchesBodyAndUsage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repo := newMockSettingRepo()
	data, err := json.Marshal(PromptCacheSimulationSettings{
		Enabled:            true,
		SemanticFirst:      true,
		FallbackReadRatio:  0.7,
		FallbackWriteRatio: 0.2,
		TTLSeconds:         300,
	})
	require.NoError(t, err)
	repo.data[SettingKeyPromptCacheSimulationSettings] = string(data)

	cfg := &config.Config{}
	svc := NewGatewayService(
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		&RateLimitService{},
		nil,
		cfg,
		nil,
		nil,
		NewBillingService(cfg, nil),
		nil,
		&BillingCacheService{},
		nil,
		nil,
		&DeferredService{},
		nil,
		nil,
		nil,
		NewSettingService(repo, cfg),
		nil,
	)

	parsed, err := ParseGatewayRequest([]byte(`{
		"model":"claude-sonnet-4",
		"messages":[{"role":"user","content":[{"type":"text","text":"hello","cache_control":{"type":"ephemeral"}}]}],
		"metadata":{"user_id":"user-1"}
	}`), PlatformAnthropic)
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)

	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{},
		Body: io.NopCloser(strings.NewReader(`{
			"type":"message",
			"model":"claude-sonnet-4",
			"usage":{"input_tokens":10,"output_tokens":3,"cache_creation_input_tokens":0,"cache_read_input_tokens":0}
		}`)),
	}

	usage, err := svc.handleNonStreamingResponse(context.Background(), resp, c, &Account{ID: 1}, "claude-sonnet-4", "claude-sonnet-4", parsed)
	require.NoError(t, err)
	require.NotNil(t, usage)
	require.Zero(t, usage.InputTokens)
	require.Equal(t, 10, usage.CacheCreationInputTokens)
	require.Equal(t, 10, usage.CacheCreation5mTokens)
	require.Zero(t, usage.CacheReadInputTokens)
	require.Equal(t, int64(0), gjson.Get(rec.Body.String(), "usage.input_tokens").Int())
	require.Equal(t, int64(10), gjson.Get(rec.Body.String(), "usage.cache_creation_input_tokens").Int())
	require.Equal(t, int64(10), gjson.Get(rec.Body.String(), "usage.cache_creation.ephemeral_5m_input_tokens").Int())
}

func TestHandleNonStreamingResponse_PreservesAuthoritativeUpstreamCacheFields(t *testing.T) {
	gin.SetMode(gin.TestMode)
	repo := newMockSettingRepo()
	data, err := json.Marshal(PromptCacheSimulationSettings{
		Enabled:            true,
		SemanticFirst:      true,
		FallbackReadRatio:  0.7,
		FallbackWriteRatio: 0.2,
		TTLSeconds:         300,
	})
	require.NoError(t, err)
	repo.data[SettingKeyPromptCacheSimulationSettings] = string(data)

	cfg := &config.Config{}
	svc := NewGatewayService(
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		&RateLimitService{},
		nil,
		cfg,
		nil,
		nil,
		NewBillingService(cfg, nil),
		nil,
		&BillingCacheService{},
		nil,
		nil,
		&DeferredService{},
		nil,
		nil,
		nil,
		NewSettingService(repo, cfg),
		nil,
	)

	parsed, err := ParseGatewayRequest([]byte(`{
		"model":"claude-sonnet-4",
		"messages":[{"role":"user","content":[{"type":"text","text":"hello","cache_control":{"type":"ephemeral"}}]}],
		"metadata":{"user_id":"user-1"}
	}`), PlatformAnthropic)
	require.NoError(t, err)

	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/messages", nil)

	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{},
		Body: io.NopCloser(strings.NewReader(`{
			"type":"message",
			"model":"claude-sonnet-4",
			"usage":{
				"input_tokens":10,
				"output_tokens":3,
				"cache_creation_input_tokens":0,
				"cache_read_input_tokens":0,
				"cache_creation":{"ephemeral_5m_input_tokens":4}
			}
		}`)),
	}

	usage, err := svc.handleNonStreamingResponse(context.Background(), resp, c, &Account{ID: 1}, "claude-sonnet-4", "claude-sonnet-4", parsed)
	require.NoError(t, err)
	require.NotNil(t, usage)
	require.Zero(t, usage.CacheCreationInputTokens)
	require.Equal(t, 4, usage.CacheCreation5mTokens)
	require.Zero(t, usage.CacheReadInputTokens)
	require.Equal(t, int64(0), gjson.Get(rec.Body.String(), "usage.cache_creation_input_tokens").Int())
	require.Equal(t, int64(4), gjson.Get(rec.Body.String(), "usage.cache_creation.ephemeral_5m_input_tokens").Int())
}

