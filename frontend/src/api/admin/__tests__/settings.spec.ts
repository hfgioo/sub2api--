import { beforeEach, describe, expect, it, vi } from 'vitest'

const { mockGet, mockPut } = vi.hoisted(() => ({
  mockGet: vi.fn(),
  mockPut: vi.fn()
}))

vi.mock('../../client', () => ({
  apiClient: {
    get: mockGet,
    put: mockPut
  }
}))

import settingsAPI, * as settingsModule from '../settings'

describe('admin settings prompt cache simulation api', () => {
  beforeEach(() => {
    mockGet.mockReset()
    mockPut.mockReset()
  })

  it('gets prompt cache simulation settings from the dedicated endpoint', async () => {
    const expected = {
      enabled: true,
      semantic_first: true,
      fallback_read_ratio: 0.7,
      fallback_write_ratio: 0.2,
      ttl_seconds: 300
    }
    mockGet.mockResolvedValue({ data: expected })

    const getPromptCacheSimulationSettings = (settingsModule as Record<string, any>).getPromptCacheSimulationSettings

    expect(getPromptCacheSimulationSettings).toBeTypeOf('function')
    await expect(getPromptCacheSimulationSettings()).resolves.toEqual(expected)
    expect(mockGet).toHaveBeenCalledWith('/admin/settings/prompt-cache-simulation')
    expect((settingsAPI as Record<string, any>).getPromptCacheSimulationSettings).toBe(getPromptCacheSimulationSettings)
  })

  it('updates prompt cache simulation settings through the dedicated endpoint', async () => {
    const payload = {
      enabled: true,
      semantic_first: false,
      fallback_read_ratio: 0.5,
      fallback_write_ratio: 0.25,
      ttl_seconds: 600
    }
    mockPut.mockResolvedValue({ data: payload })

    const updatePromptCacheSimulationSettings = (settingsModule as Record<string, any>).updatePromptCacheSimulationSettings

    expect(updatePromptCacheSimulationSettings).toBeTypeOf('function')
    await expect(updatePromptCacheSimulationSettings(payload)).resolves.toEqual(payload)
    expect(mockPut).toHaveBeenCalledWith('/admin/settings/prompt-cache-simulation', payload)
    expect((settingsAPI as Record<string, any>).updatePromptCacheSimulationSettings).toBe(updatePromptCacheSimulationSettings)
  })
})
