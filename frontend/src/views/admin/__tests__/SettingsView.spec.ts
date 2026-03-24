import { beforeEach, describe, expect, it, vi } from 'vitest'
import { flushPromises, mount } from '@vue/test-utils'

import SettingsView from '../SettingsView.vue'

const {
  getSettings,
  updateSettings,
  getAdminApiKey,
  deleteAdminApiKey,
  regenerateAdminApiKey,
  testSmtpConnection,
  sendTestEmail,
  getOverloadCooldownSettings,
  updateOverloadCooldownSettings,
  getStreamTimeoutSettings,
  updateStreamTimeoutSettings,
  getPromptCacheSimulationSettings,
  updatePromptCacheSimulationSettings,
  getRectifierSettings,
  updateRectifierSettings,
  getBetaPolicySettings,
  updateBetaPolicySettings,
  getGroups,
  fetchPublicSettings,
  fetchAdminSettings,
  showSuccess,
  showError,
  copyToClipboard
} = vi.hoisted(() => ({
  getSettings: vi.fn(),
  updateSettings: vi.fn(),
  getAdminApiKey: vi.fn(),
  deleteAdminApiKey: vi.fn(),
  regenerateAdminApiKey: vi.fn(),
  testSmtpConnection: vi.fn(),
  sendTestEmail: vi.fn(),
  getOverloadCooldownSettings: vi.fn(),
  updateOverloadCooldownSettings: vi.fn(),
  getStreamTimeoutSettings: vi.fn(),
  updateStreamTimeoutSettings: vi.fn(),
  getPromptCacheSimulationSettings: vi.fn(),
  updatePromptCacheSimulationSettings: vi.fn(),
  getRectifierSettings: vi.fn(),
  updateRectifierSettings: vi.fn(),
  getBetaPolicySettings: vi.fn(),
  updateBetaPolicySettings: vi.fn(),
  getGroups: vi.fn(),
  fetchPublicSettings: vi.fn(),
  fetchAdminSettings: vi.fn(),
  showSuccess: vi.fn(),
  showError: vi.fn(),
  copyToClipboard: vi.fn()
}))

vi.mock('@/api', () => ({
  adminAPI: {
    settings: {
      getSettings,
      updateSettings,
      getAdminApiKey,
      deleteAdminApiKey,
      regenerateAdminApiKey,
      testSmtpConnection,
      sendTestEmail,
      getOverloadCooldownSettings,
      updateOverloadCooldownSettings,
      getStreamTimeoutSettings,
      updateStreamTimeoutSettings,
      getPromptCacheSimulationSettings,
      updatePromptCacheSimulationSettings,
      getRectifierSettings,
      updateRectifierSettings,
      getBetaPolicySettings,
      updateBetaPolicySettings
    },
    groups: {
      getAll: getGroups
    }
  }
}))

vi.mock('@/stores', () => ({
  useAppStore: () => ({
    showSuccess,
    showError,
    fetchPublicSettings
  })
}))

vi.mock('@/stores/adminSettings', () => ({
  useAdminSettingsStore: () => ({
    fetch: fetchAdminSettings
  })
}))

vi.mock('@/composables/useClipboard', () => ({
  useClipboard: () => ({
    copyToClipboard
  })
}))

vi.mock('vue-i18n', async () => {
  const actual = await vi.importActual<typeof import('vue-i18n')>('vue-i18n')
  return {
    ...actual,
    useI18n: () => ({
      t: (key: string) => key
    })
  }
})

const createSettingsResponse = () => ({
  registration_enabled: true,
  email_verify_enabled: false,
  registration_email_suffix_whitelist: [],
  promo_code_enabled: true,
  password_reset_enabled: false,
  frontend_url: '',
  invitation_code_enabled: false,
  totp_enabled: false,
  totp_encryption_key_configured: false,
  smtp_host: '',
  smtp_port: 587,
  smtp_username: '',
  smtp_password_configured: false,
  smtp_from_email: '',
  smtp_from_name: '',
  smtp_use_tls: true,
  turnstile_enabled: false,
  turnstile_site_key: '',
  turnstile_secret_key_configured: false,
  linuxdo_connect_enabled: false,
  linuxdo_connect_client_id: '',
  linuxdo_connect_client_secret_configured: false,
  linuxdo_connect_redirect_url: '',
  site_name: 'Sub2API',
  site_logo: '',
  site_subtitle: '',
  api_base_url: '',
  contact_info: '',
  doc_url: '',
  home_content: '',
  hide_ccs_import_button: false,
  purchase_subscription_enabled: false,
  purchase_subscription_url: '',
  sora_client_enabled: false,
  custom_menu_items: [],
  default_concurrency: 1,
  default_balance: 0,
  default_subscriptions: [],
  enable_model_fallback: false,
  fallback_model_anthropic: '',
  fallback_model_openai: '',
  fallback_model_gemini: '',
  fallback_model_antigravity: '',
  enable_identity_patch: false,
  identity_patch_prompt: '',
  ops_monitoring_enabled: false,
  ops_realtime_monitoring_enabled: false,
  ops_query_mode_default: 'auto',
  ops_metrics_interval_seconds: 60,
  min_claude_code_version: '',
  max_claude_code_version: '',
  allow_ungrouped_key_scheduling: false,
  backend_mode_enabled: false
})

const AppLayoutStub = { template: '<div><slot /></div>' }
const BackupSettingsStub = { template: '<div />' }
const DataManagementSettingsStub = { template: '<div />' }
const IconStub = { template: '<span />' }
const GroupBadgeStub = { template: '<span />' }
const GroupOptionItemStub = { template: '<span />' }
const ImageUploadStub = { template: '<div />' }
const SelectStub = {
  props: ['modelValue', 'options'],
  emits: ['update:modelValue'],
  template: '<select />'
}
const ToggleStub = {
  props: ['modelValue'],
  emits: ['update:modelValue'],
  template: '<button type="button" @click="$emit(\'update:modelValue\', !modelValue)">{{ modelValue }}</button>'
}

function mountView() {
  return mount(SettingsView, {
    global: {
      stubs: {
        AppLayout: AppLayoutStub,
        BackupSettings: BackupSettingsStub,
        DataManagementSettings: DataManagementSettingsStub,
        Icon: IconStub,
        GroupBadge: GroupBadgeStub,
        GroupOptionItem: GroupOptionItemStub,
        ImageUpload: ImageUploadStub,
        Select: SelectStub,
        Toggle: ToggleStub
      }
    }
  })
}

describe('admin SettingsView prompt cache simulation card', () => {
  beforeEach(() => {
    vi.clearAllMocks()

    getSettings.mockResolvedValue(createSettingsResponse())
    getGroups.mockResolvedValue([])
    getAdminApiKey.mockResolvedValue({ exists: false, masked_key: '' })
    getOverloadCooldownSettings.mockResolvedValue({ enabled: true, cooldown_minutes: 10 })
    getStreamTimeoutSettings.mockResolvedValue({
      enabled: true,
      action: 'temp_unsched',
      temp_unsched_minutes: 5,
      threshold_count: 3,
      threshold_window_minutes: 10
    })
    getRectifierSettings.mockResolvedValue({
      enabled: true,
      thinking_signature_enabled: true,
      thinking_budget_enabled: true
    })
    getBetaPolicySettings.mockResolvedValue({ rules: [] })
    getPromptCacheSimulationSettings.mockResolvedValue({
      enabled: true,
      semantic_first: true,
      hit_ratio: 1,
      fallback_read_ratio: 0.7,
      fallback_write_ratio: 0.2,
      ttl_seconds: 300
    })
    updatePromptCacheSimulationSettings.mockImplementation(async (payload) => payload)
  })

  it('loads dedicated prompt cache simulation settings on mount', async () => {
    const wrapper = mountView()
    await flushPromises()

    expect(getPromptCacheSimulationSettings).toHaveBeenCalledTimes(1)

    await wrapper.findAll('button').find((button) => button.text().includes('admin.settings.tabs.gateway'))?.trigger('click')
    await flushPromises()

    const promptCacheCard = wrapper.findAll('.card').find((card) =>
      card.text().includes('admin.settings.promptCacheSimulation.title')
    )

    expect(promptCacheCard?.exists()).toBe(true)
    expect(promptCacheCard?.findAll('input[type="number"]')).toHaveLength(4)
  })

  it('saves dedicated prompt cache simulation settings without using the main form submit', async () => {
    const wrapper = mountView()
    await flushPromises()

    await wrapper.findAll('button').find((button) => button.text().includes('admin.settings.tabs.gateway'))?.trigger('click')
    await flushPromises()

    const promptCacheCard = wrapper.findAll('.card').find((card) =>
      card.text().includes('admin.settings.promptCacheSimulation.title')
    )

    expect(promptCacheCard?.exists()).toBe(true)

    const numberInputs = promptCacheCard!.findAll('input[type="number"]')
    await numberInputs[0].setValue('0.85')
    await numberInputs[1].setValue('0.55')
    await numberInputs[2].setValue('0.25')
    await numberInputs[3].setValue('600')

    await promptCacheCard!.findAll('button').find((button) => button.text().includes('common.save'))?.trigger('click')
    await flushPromises()

    expect(updatePromptCacheSimulationSettings).toHaveBeenCalledWith({
      enabled: true,
      semantic_first: true,
      hit_ratio: 0.85,
      fallback_read_ratio: 0.55,
      fallback_write_ratio: 0.25,
      ttl_seconds: 600
    })
    expect(updateSettings).not.toHaveBeenCalled()
    expect(showSuccess).toHaveBeenCalledWith('admin.settings.promptCacheSimulation.saved')
  })
})
