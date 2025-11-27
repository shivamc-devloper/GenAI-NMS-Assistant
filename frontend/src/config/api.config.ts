// Types for our configuration
interface ApiConfig {
  API_BASE_URL: string;
  NETWORK: {
    DEFAULT_IP: string;
    DEFAULT_PORT: string;
  };
  VERSION: string;
}

// Default configuration
const DEFAULT_CONFIG: ApiConfig = {
  API_BASE_URL: import.meta.env.VITE_API_BASE_URL || 'http://localhost:9000',
  NETWORK: {
    DEFAULT_IP: '8',
    DEFAULT_PORT: '9000',
  },
  VERSION: 'v1',
};

// Mutable configuration
let currentConfig: ApiConfig = { ...DEFAULT_CONFIG };

/**
 * Builds a complete API URL
 * @param endpoint API endpoint path (e.g., 'cpu-usage')
 * @returns Full URL string (e.g., 'http://localhost:9000/cpu-usage')
 */
function buildApiUrl(endpoint: string): string {
  const base = currentConfig.API_BASE_URL.endsWith('/')
    ? currentConfig.API_BASE_URL.slice(0, -1)
    : currentConfig.API_BASE_URL;
  return `${base}/${endpoint}`.replace(/([^:]\/)\/+/g, '$1');
}

// API endpoints configuration
export const API = {
  // Get current configuration (read-only)
  getConfig: (): Readonly<ApiConfig> => ({ ...currentConfig }),

  // Update configuration
  updateConfig: (updates: Partial<ApiConfig>): void => {
    currentConfig = { ...currentConfig, ...updates };
    
    // If API_BASE_URL is being updated, ensure it has the protocol
    if (updates.API_BASE_URL && !updates.API_BASE_URL.startsWith('http')) {
      currentConfig.API_BASE_URL = `http://${updates.API_BASE_URL}`;
    }
    
    // If only IP or PORT is updated, update the API_BASE_URL
    if (updates.NETWORK?.DEFAULT_IP || updates.NETWORK?.DEFAULT_PORT) {
      const { DEFAULT_IP, DEFAULT_PORT } = currentConfig.NETWORK;
      currentConfig.API_BASE_URL = `http://${DEFAULT_IP}:${DEFAULT_PORT}`;
    }
  },
  
  // Reset to default configuration
  resetConfig: (): void => {
    currentConfig = { ...DEFAULT_CONFIG };
  },
  
  // Endpoints
  ENDPOINTS: {
    DEVICE: {
      get DETAILS() { return buildApiUrl('device-details'); },
      get MEMORY() { return buildApiUrl('memory-usage'); },
      get CPU() { return buildApiUrl('cpu-usage'); },
    },
    
    // Example of how to add more endpoint groups:
    // AUTH: {
    //   get LOGIN() { return buildApiUrl('auth/login'); },
    //   get LOGOUT() { return buildApiUrl('auth/logout'); },
    // },
  },
  
  // Helper methods
  getBaseUrl: (): string => currentConfig.API_BASE_URL,
  getNetworkConfig: () => ({ ...currentConfig.NETWORK }),
  getVersion: (): string => currentConfig.VERSION,
};

// Type exports
export type { ApiConfig };
export type ApiEndpoint = typeof API.ENDPOINTS;

declare global {
  interface Window {
    __API_CONFIG?: typeof API;
  }
}

// Make API globally available in development for debugging
if (import.meta.env.DEV) {
  window.__API_CONFIG = API;
  console.log('API configuration available at window.__API_CONFIG');
}
