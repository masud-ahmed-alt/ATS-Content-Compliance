/**
 * API Configuration & Request Manager
 * Centralized API handling with:
 * - Automatic base URL detection
 * - Request/response interceptors
 * - Error handling and retry logic
 * - Connection pooling
 */

// Base URLs with fallbacks
const getBaseUrl = (service) => {
  const envVar = `VITE_${service.toUpperCase()}_BASE_URL`;
  const envValue = import.meta.env[envVar];

  // Fallback URLs (development - matches docker-compose.yml)
  const fallbacks = {
    fetcher: "http://localhost:8080",
    analyzer: "http://localhost:8000",
    renderer: "http://localhost:9000",
    minio: "http://localhost:7000",
    opensearch: "http://localhost:9200",
    redis: "http://localhost:6379",
  };

  return envValue || fallbacks[service.toLowerCase()] || "";
};

export const API_CONFIG = {
  fetcherBaseUrl: getBaseUrl("fetcher"),
  analyzerBaseUrl: getBaseUrl("analyzer"),
  rendererBaseUrl: getBaseUrl("renderer"),
  minIoBaseUrl: getBaseUrl("minio"),
  openSearchBaseUrl: getBaseUrl("opensearch"),
  redisBaseUrl: getBaseUrl("redis"),
};

/**
 * HTTP Client with retry & error handling
 */
class HttpClient {
  constructor() {
    this.timeout = 30000;
    this.maxRetries = 3;
    this.retryDelay = 1000;
  }

  /**
   * Fetch with automatic retry and timeout
   */
  async fetch(url, options = {}) {
    let lastError;

    for (let attempt = 0; attempt <= this.maxRetries; attempt++) {
      try {
        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), this.timeout);

        const response = await fetch(url, {
          ...options,
          signal: controller.signal,
          headers: {
            "Content-Type": "application/json",
            ...options.headers,
          },
        });

        clearTimeout(timeoutId);

        if (!response.ok) {
          throw new Error(`HTTP ${response.status}: ${response.statusText}`);
        }

        return response;
      } catch (error) {
        lastError = error;

        // Don't retry on client errors (4xx) or if max retries exceeded
        if (error.name === "AbortError") {
          console.warn(
            `[API] Timeout on attempt ${attempt + 1}/${this.maxRetries + 1}`
          );
        } else if (error.message.startsWith("HTTP 4")) {
          throw error;
        }

        if (attempt < this.maxRetries) {
          const delay = this.retryDelay * Math.pow(2, attempt);
          console.warn(
            `[API] Retry ${attempt + 1}/${this.maxRetries} in ${delay}ms`,
            error.message
          );
          await new Promise((r) => setTimeout(r, delay));
        }
      }
    }

    throw lastError;
  }

  /**
   * GET request
   */
  async get(url) {
    const response = await this.fetch(url, { method: "GET" });
    return response.json();
  }

  /**
   * POST request
   */
  async post(url, data) {
    const response = await this.fetch(url, {
      method: "POST",
      body: JSON.stringify(data),
    });
    return response.json();
  }

  /**
   * PUT request
   */
  async put(url, data) {
    const response = await this.fetch(url, {
      method: "PUT",
      body: JSON.stringify(data),
    });
    return response.json();
  }

  /**
   * DELETE request
   */
  async delete(url) {
    const response = await this.fetch(url, { method: "DELETE" });
    return response.json();
  }
}

// Singleton instance
export const apiClient = new HttpClient();

/**
 * WebSocket Manager with auto-reconnect
 */
export class WebSocketManager {
  constructor(url) {
    this.url = url;
    this.ws = null;
    this.reconnectAttempts = 0;
    this.maxReconnectAttempts = 5;
    this.reconnectDelay = 1000;
    this.listeners = [];
  }

  /**
   * Connect to WebSocket
   */
  connect() {
    return new Promise((resolve, reject) => {
      try {
        this.ws = new WebSocket(this.url);

        this.ws.onopen = () => {
          console.log("[ws] Connected:", this.url);
          this.reconnectAttempts = 0;
          this.notify({ type: "connected" });
          resolve();
        };

        this.ws.onmessage = (event) => {
          try {
            const data = JSON.parse(event.data);
            this.notify({ type: "message", data });
          } catch (e) {
            console.error("[ws] Parse error:", e);
          }
        };

        this.ws.onerror = (error) => {
          console.error("[ws] Error:", error);
          this.notify({ type: "error", error });
          reject(error);
        };

        this.ws.onclose = () => {
          console.warn("[ws] Closed");
          this.notify({ type: "disconnected" });
          this.attemptReconnect();
        };
      } catch (error) {
        reject(error);
      }
    });
  }

  /**
   * Attempt to reconnect with exponential backoff
   */
  attemptReconnect() {
    if (this.reconnectAttempts >= this.maxReconnectAttempts) {
      console.error("[ws] Max reconnection attempts exceeded");
      return;
    }

    const delay = this.reconnectDelay * Math.pow(2, this.reconnectAttempts);
    console.log(
      `[ws] Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts + 1})`
    );

    setTimeout(() => {
      this.reconnectAttempts++;
      this.connect().catch((e) => console.error("[ws] Reconnect failed:", e));
    }, delay);
  }

  /**
   * Send message
   */
  send(data) {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(data));
    } else {
      console.warn("[ws] Not connected, cannot send");
    }
  }

  /**
   * Subscribe to events
   */
  subscribe(listener) {
    this.listeners.push(listener);
    return () => {
      this.listeners = this.listeners.filter((l) => l !== listener);
    };
  }

  /**
   * Notify all listeners
   */
  notify(event) {
    this.listeners.forEach((listener) => {
      try {
        listener(event);
      } catch (e) {
        console.error("[ws] Listener error:", e);
      }
    });
  }

  /**
   * Close connection
   */
  close() {
    if (this.ws) {
      this.ws.close();
      this.ws = null;
    }
  }

  /**
   * Check if connected
   */
  isConnected() {
    return this.ws && this.ws.readyState === WebSocket.OPEN;
  }
}
