import { maskApiKey } from "../config.js";

function todayDate() {
  return new Date().toISOString().slice(0, 10);
}

function hash(text) {
  let h = 0;
  for (let i = 0; i < text.length; i += 1) {
    h = (h * 31 + text.charCodeAt(i)) | 0;
  }
  return Math.abs(h);
}

export class GoogleEarthService {
  constructor(config) {
    this.config = config;
  }

  buildFallbackHints(date, boundsKey, earthEngineStatus) {
    const seed = hash(`${date}:${boundsKey}`);
    return {
      source: "simulated",
      rainfallMultiplier: 0.8 + (seed % 35) / 100,
      soilMoistureBias: -0.1 + (seed % 20) / 100,
      wetnessBias: (seed % 15) / 100,
      ndviBias: -0.12 + (seed % 18) / 100,
      fetchedAt: new Date().toISOString(),
      earthEngineStatus
    };
  }

  hasApiKey() {
    return Boolean(this.config.googleEarthApiKey);
  }

  buildEarthEngineUrl(pathname, params = {}) {
    const url = new URL(`https://earthengine.googleapis.com${pathname}`);
    const merged = { ...params };
    if (this.config.googleEarthApiKey) {
      merged.key = this.config.googleEarthApiKey;
    }

    for (const [key, value] of Object.entries(merged)) {
      if (value !== undefined && value !== null) {
        url.searchParams.set(key, String(value));
      }
    }

    return url;
  }

  async checkConnectivity() {
    if (!this.hasApiKey()) {
      return {
        status: "missing_api_key_fallback",
        liveEarthCallsEnabled: this.config.enableLiveEarthCalls,
        message: "GOOGLE_EARTH_API_KEY is not set. Using fallback data generation.",
        apiKeyMasked: maskApiKey(this.config.googleEarthApiKey),
        checkedAt: new Date().toISOString()
      };
    }

    if (!this.config.enableLiveEarthCalls) {
      return {
        status: "configured_offline_mode",
        liveEarthCallsEnabled: false,
        message: "API key is configured. Set ENABLE_LIVE_EARTH_CALLS=true to perform live Earth Engine checks.",
        apiKeyMasked: maskApiKey(this.config.googleEarthApiKey),
        checkedAt: new Date().toISOString()
      };
    }

    const testUrl = this.buildEarthEngineUrl(`/v1beta/projects/${this.config.earthEngineProject}/assets/LANDSAT`);

    try {
      const response = await fetch(testUrl, { method: "GET" });
      const bodyText = await response.text();
      return {
        status: response.ok ? "connected" : "connection_error",
        liveEarthCallsEnabled: true,
        httpStatus: response.status,
        apiKeyMasked: maskApiKey(this.config.googleEarthApiKey),
        checkedAt: new Date().toISOString(),
        endpoint: `${testUrl.origin}${testUrl.pathname}`,
        detail: bodyText.slice(0, 300)
      };
    } catch (error) {
      return {
        status: "connection_exception",
        liveEarthCallsEnabled: true,
        apiKeyMasked: maskApiKey(this.config.googleEarthApiKey),
        checkedAt: new Date().toISOString(),
        message: error.message
      };
    }
  }

  // Lightweight, key-aware ingestion hints used to condition synthetic data generation.
  // If live calls are unavailable, it falls back to deterministic pseudo-random hints.
  async fetchIngestionHints({ date = todayDate(), boundsKey = "default" } = {}) {
    if (!this.hasApiKey()) {
      return this.buildFallbackHints(date, boundsKey, "missing_api_key");
    }

    if (!this.config.enableLiveEarthCalls) {
      return this.buildFallbackHints(date, boundsKey, "live_calls_disabled");
    }

    // Public asset metadata call demonstrates key-based Earth Engine request construction.
    // The returned metadata is used only as a freshness signal and fallback source weighting.
    const metaUrl = this.buildEarthEngineUrl(`/v1beta/projects/${this.config.earthEngineProject}/assets/NOAA_GFS0P25`);

    try {
      const response = await fetch(metaUrl, { method: "GET" });
      if (!response.ok) {
        return {
          ...this.buildFallbackHints(date, boundsKey, `http_${response.status}`),
          earthEngineStatus: `http_${response.status}`,
          source: "simulated_with_live_probe"
        };
      }

      const payload = await response.json();
      const idText = payload?.name || payload?.id || "earthengine";
      const liveSeed = hash(`${date}:${boundsKey}:${idText}`);

      return {
        source: "earthengine_keyed_probe",
        rainfallMultiplier: 0.85 + (liveSeed % 28) / 100,
        soilMoistureBias: -0.08 + (liveSeed % 17) / 100,
        wetnessBias: (liveSeed % 10) / 100,
        ndviBias: -0.1 + (liveSeed % 15) / 100,
        fetchedAt: new Date().toISOString(),
        earthEngineStatus: "connected"
      };
    } catch {
      return {
        ...this.buildFallbackHints(date, boundsKey, "exception"),
        earthEngineStatus: "exception",
        source: "simulated_with_live_probe"
      };
    }
  }
}
