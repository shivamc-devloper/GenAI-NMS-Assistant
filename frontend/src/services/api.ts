import type {
  CpuUsage,
  DeviceDetailResponse,
  MemoryUsage,
  ApiError,
} from '../types';
import { API } from '../config/api.config';

async function handleResponse<T>(response: Response): Promise<T> {
  if (!response.ok) {
    const message = await response.text();
    throw {
      status: response.status,
      message: message || 'Request failed',
    } satisfies ApiError;
  }
  return response.json() as Promise<T>;
}

/**
 * Builds a URL with query parameters
 * @param baseUrl The base URL
 * @param params Query parameters as key-value pairs
 * @returns Complete URL with query string
 */
function buildUrl(baseUrl: string, params: Record<string, string> = {}): string {
  const url = new URL(baseUrl);
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null) {
      url.searchParams.set(key, value);
    }
  });
  return url.toString();
}

// Device API
export const deviceApi = {
  /**
   * Fetch device details
   * @param host Device hostname or IP
   */
  fetchDetails: async (host: string): Promise<DeviceDetailResponse> => {
    const url = buildUrl(API.ENDPOINTS.DEVICE.DETAILS, { host });
    return handleResponse<DeviceDetailResponse>(await fetch(url));
  },

  /**
   * Fetch memory usage data for a device
   * @param deviceId LibreNMS device identifier
   */
  fetchMemoryUsage: async (deviceId: number): Promise<MemoryUsage[] | null> => {
    const url = buildUrl(API.ENDPOINTS.DEVICE.MEMORY, { host: String(deviceId) });
    return handleResponse<MemoryUsage[] | null>(await fetch(url));
  },

  /**
   * Fetch CPU usage data for a device
   * @param deviceId LibreNMS device identifier
   */
  fetchCpuUsage: async (deviceId: number): Promise<CpuUsage> => {
    const url = buildUrl(API.ENDPOINTS.DEVICE.CPU, { host: String(deviceId) });
    return handleResponse<CpuUsage>(await fetch(url));
  },

  /**
   * Update API configuration
   * @param config Partial configuration to update
   */
  updateConfig: (config: Partial<Parameters<typeof API.updateConfig>[0]>) => {
    API.updateConfig(config);
  },

  /**
   * Get current configuration
   */
  getConfig: () => API.getConfig(),
};

// Export the main API instance for direct access if needed
export { API };

// Example of how to add a new API group:
// export const authApi = {
//   login: async (credentials: { username: string; password: string }) => {
//     const url = API.ENDPOINTS.AUTH.LOGIN;
//     return handleResponse<AuthResponse>(await fetch(url, {
//       method: 'POST',
//       headers: { 'Content-Type': 'application/json' },
//       body: JSON.stringify(credentials),
//     }));
//   },
// };

export type { ApiError };
