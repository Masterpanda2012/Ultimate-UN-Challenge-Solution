import { maskApiKey } from "../config.js";

const CONNECTIVITY_CACHE_TTL_MS = 5 * 60 * 1000;

const PROVIDER_PRIORITY = [
  "google_earth",
  "copernicus_dataspace",
  "chirps",
  "noaa_nws",
  "openstreetmap"
];

const PROVIDER_CATALOG = {
  google_earth: {
    id: "google_earth",
    name: "Google Earth Engine",
    type: "satellite",
    docs: "https://developers.google.com/earth-engine"
  },
  copernicus_dataspace: {
    id: "copernicus_dataspace",
    name: "Copernicus Data Space",
    type: "satellite",
    docs: "https://documentation.dataspace.copernicus.eu/APIs.html"
  },
  chirps: {
    id: "chirps",
    name: "CHIRPS Rainfall",
    type: "climate",
    docs: "https://www.chc.ucsb.edu/data/chirps3"
  },
  noaa_nws: {
    id: "noaa_nws",
    name: "NOAA/NWS",
    type: "weather",
    docs: "https://www.weather.gov/documentation/services-web-api"
  },
  openstreetmap: {
    id: "openstreetmap",
    name: "OpenStreetMap",
    type: "basemap",
    docs: "https://wiki.openstreetmap.org/wiki/Overpass_API"
  },
  deterministic_fallback: {
    id: "deterministic_fallback",
    name: "Deterministic Fallback",
    type: "fallback",
    docs: null
  }
};

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

function clamp(value, min = 0, max = 1) {
  return Math.max(min, Math.min(max, value));
}

export class GoogleEarthService {
  constructor(config) {
    this.config = config;
    this.connectivityCache = new Map();
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

  async fetchWithTimeout(url, options = {}, timeoutMs = this.config.providerProbeTimeoutMs) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      return await fetch(url, {
        ...options,
        signal: controller.signal
      });
    } finally {
      clearTimeout(timer);
    }
  }

  isLiveStatus(status) {
    return status === "connected";
  }

  withRuntimeFlags(providerId, payload) {
    const provider = PROVIDER_CATALOG[providerId] || { id: providerId, name: providerId, type: "unknown" };
    const isLive = this.isLiveStatus(payload.status);
    return {
      providerId,
      providerName: provider.name,
      providerType: provider.type,
      ...payload,
      isLive,
      fallbackActive: !isLive,
      runtimeMode: isLive ? "live" : "fallback"
    };
  }

  getCachedProviderStatus(providerId, force = false) {
    if (force) return null;
    const cached = this.connectivityCache.get(providerId);
    if (!cached) return null;
    const ageMs = Date.now() - cached.checkedAtMs;
    if (ageMs > CONNECTIVITY_CACHE_TTL_MS) {
      this.connectivityCache.delete(providerId);
      return null;
    }
    return cached.payload;
  }

  setCachedProviderStatus(providerId, payload) {
    const normalized = this.withRuntimeFlags(providerId, payload);
    this.connectivityCache.set(providerId, {
      checkedAtMs: Date.now(),
      payload: normalized
    });
    return normalized;
  }

  providerSeed(date, boundsKey, providerId, statusToken) {
    return hash(`${date}:${boundsKey}:${providerId}:${statusToken || "ok"}`);
  }

  buildFallbackHints(date, boundsKey, fallbackReason, runtime) {
    const seed = this.providerSeed(date, boundsKey, "deterministic_fallback", fallbackReason);
    return {
      source: "simulated",
      providerId: "deterministic_fallback",
      providerName: PROVIDER_CATALOG.deterministic_fallback.name,
      rainfallMultiplier: Number((0.8 + (seed % 35) / 100).toFixed(4)),
      soilMoistureBias: Number((-0.1 + (seed % 20) / 100).toFixed(4)),
      wetnessBias: Number(((seed % 15) / 100).toFixed(4)),
      ndviBias: Number((-0.12 + (seed % 18) / 100).toFixed(4)),
      fetchedAt: new Date().toISOString(),
      earthEngineStatus: fallbackReason,
      fallbackReason,
      providerStatuses: (runtime?.providers || []).map((item) => ({
        providerId: item.providerId,
        providerName: item.providerName,
        status: item.status,
        isLive: item.isLive,
        checkedAt: item.checkedAt
      }))
    };
  }

  buildProviderHints(date, boundsKey, providerStatus, sourcePrefix = "provider_probe") {
    const offsets = {
      google_earth: { rain: 0.03, soil: 0.02, wet: 0.03, ndvi: 0.02 },
      copernicus_dataspace: { rain: 0.01, soil: 0.04, wet: 0.04, ndvi: 0.03 },
      chirps: { rain: 0.05, soil: 0.01, wet: 0.01, ndvi: 0.0 },
      noaa_nws: { rain: 0.04, soil: 0.0, wet: 0.02, ndvi: -0.01 },
      openstreetmap: { rain: -0.02, soil: -0.01, wet: 0.0, ndvi: -0.02 }
    };

    const tweak = offsets[providerStatus.providerId] || { rain: 0, soil: 0, wet: 0, ndvi: 0 };
    const seed = this.providerSeed(date, boundsKey, providerStatus.providerId, providerStatus.status);

    return {
      source: `${sourcePrefix}:${providerStatus.providerId}`,
      providerId: providerStatus.providerId,
      providerName: providerStatus.providerName,
      rainfallMultiplier: Number(clamp(0.82 + (seed % 33) / 100 + tweak.rain, 0.7, 1.35).toFixed(4)),
      soilMoistureBias: Number(clamp(-0.09 + (seed % 21) / 100 + tweak.soil, -0.2, 0.25).toFixed(4)),
      wetnessBias: Number(clamp((seed % 14) / 100 + tweak.wet, -0.1, 0.25).toFixed(4)),
      ndviBias: Number(clamp(-0.11 + (seed % 16) / 100 + tweak.ndvi, -0.25, 0.2).toFixed(4)),
      fetchedAt: new Date().toISOString(),
      earthEngineStatus: providerStatus.providerId === "google_earth" ? providerStatus.status : "not_primary",
      providerStatus: providerStatus.status
    };
  }

  getProviderCatalog() {
    return PROVIDER_PRIORITY.map((providerId) => ({
      ...PROVIDER_CATALOG[providerId],
      priority: PROVIDER_PRIORITY.indexOf(providerId) + 1
    }));
  }

  getMapSourceCatalog() {
    return [
      {
        id: "osm_standard",
        label: "OpenStreetMap Standard",
        type: "tile",
        attribution: "© OpenStreetMap contributors",
        templateUrl: "https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
        freeTier: true
      },
      {
        id: "osm_humanitarian",
        label: "OSM Humanitarian",
        type: "tile",
        attribution: "© OpenStreetMap contributors, Humanitarian OpenStreetMap Team",
        templateUrl: "https://{s}.tile.openstreetmap.fr/hot/{z}/{x}/{y}.png",
        freeTier: true
      },
      {
        id: "copernicus_stac",
        label: "Copernicus STAC",
        type: "catalog",
        endpoint: "https://catalogue.dataspace.copernicus.eu/stac/",
        freeTier: true
      },
      {
        id: "chirps_archive",
        label: "CHIRPS Archive",
        type: "dataset",
        endpoint: "https://data.chc.ucsb.edu/products/CHIRPS-2.0/",
        freeTier: true
      },
      {
        id: "noaa_nws",
        label: "NOAA/NWS API",
        type: "weather",
        endpoint: "https://api.weather.gov/",
        freeTier: true
      }
    ];
  }

  async checkGoogleEarthConnectivity() {
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
      const response = await this.fetchWithTimeout(testUrl, { method: "GET" });
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

  async checkCopernicusConnectivity() {
    const url = "https://catalogue.dataspace.copernicus.eu/stac/";
    try {
      const response = await this.fetchWithTimeout(url, {
        method: "GET",
        headers: {
          Accept: "application/json"
        }
      });
      const text = await response.text();
      return {
        status: response.ok ? "connected" : "connection_error",
        httpStatus: response.status,
        checkedAt: new Date().toISOString(),
        endpoint: url,
        detail: text.slice(0, 180)
      };
    } catch (error) {
      return {
        status: "connection_exception",
        checkedAt: new Date().toISOString(),
        endpoint: url,
        message: error.message
      };
    }
  }

  async checkChirpsConnectivity() {
    const url = "https://data.chc.ucsb.edu/products/CHIRPS-2.0/";
    try {
      const response = await this.fetchWithTimeout(url, {
        method: "GET"
      });
      const text = await response.text();
      return {
        status: response.ok ? "connected" : "connection_error",
        httpStatus: response.status,
        checkedAt: new Date().toISOString(),
        endpoint: url,
        detail: text.slice(0, 180)
      };
    } catch (error) {
      return {
        status: "connection_exception",
        checkedAt: new Date().toISOString(),
        endpoint: url,
        message: error.message
      };
    }
  }

  async checkNoaaConnectivity() {
    const url = "https://api.weather.gov/";
    try {
      const response = await this.fetchWithTimeout(url, {
        method: "GET",
        headers: {
          Accept: "application/geo+json,application/json",
          "User-Agent": "cattle-forecast-backend/1.0 (ops@example.org)"
        }
      });
      const text = await response.text();
      return {
        status: response.ok ? "connected" : "connection_error",
        httpStatus: response.status,
        checkedAt: new Date().toISOString(),
        endpoint: url,
        detail: text.slice(0, 180)
      };
    } catch (error) {
      return {
        status: "connection_exception",
        checkedAt: new Date().toISOString(),
        endpoint: url,
        message: error.message
      };
    }
  }

  async checkOpenStreetMapConnectivity() {
    const url = "https://overpass-api.de/api/status";
    try {
      const response = await this.fetchWithTimeout(url, {
        method: "GET"
      });
      const text = await response.text();
      return {
        status: response.ok ? "connected" : "connection_error",
        httpStatus: response.status,
        checkedAt: new Date().toISOString(),
        endpoint: url,
        detail: text.slice(0, 180)
      };
    } catch (error) {
      return {
        status: "connection_exception",
        checkedAt: new Date().toISOString(),
        endpoint: url,
        message: error.message
      };
    }
  }

  async checkProviderConnectivity(providerId, { force = false } = {}) {
    const cached = this.getCachedProviderStatus(providerId, force);
    if (cached) return cached;

    let payload;
    if (providerId === "google_earth") {
      payload = await this.checkGoogleEarthConnectivity();
    } else if (providerId === "copernicus_dataspace") {
      payload = await this.checkCopernicusConnectivity();
    } else if (providerId === "chirps") {
      payload = await this.checkChirpsConnectivity();
    } else if (providerId === "noaa_nws") {
      payload = await this.checkNoaaConnectivity();
    } else if (providerId === "openstreetmap") {
      payload = await this.checkOpenStreetMapConnectivity();
    } else {
      payload = {
        status: "unknown_provider",
        checkedAt: new Date().toISOString(),
        message: `Unknown provider: ${providerId}`
      };
    }

    return this.setCachedProviderStatus(providerId, payload);
  }

  async checkConnectivity(options = {}) {
    return this.checkProviderConnectivity("google_earth", options);
  }

  async checkAllProviders({ force = false } = {}) {
    const providers = await Promise.all(
      PROVIDER_PRIORITY.map((providerId) => this.checkProviderConnectivity(providerId, { force }))
    );

    return {
      checkedAt: new Date().toISOString(),
      providers,
      providerCount: providers.length,
      liveProviderCount: providers.filter((item) => item.isLive).length,
      fallbackProvider: PROVIDER_CATALOG.deterministic_fallback
    };
  }

  selectActiveProvider(providers = []) {
    const liveById = new Map(providers.filter((item) => item.isLive).map((item) => [item.providerId, item]));
    for (const providerId of PROVIDER_PRIORITY) {
      const candidate = liveById.get(providerId);
      if (candidate) {
        return candidate;
      }
    }

    return {
      providerId: "deterministic_fallback",
      providerName: PROVIDER_CATALOG.deterministic_fallback.name,
      providerType: PROVIDER_CATALOG.deterministic_fallback.type,
      status: "fallback_only",
      checkedAt: new Date().toISOString(),
      isLive: false,
      fallbackActive: true,
      runtimeMode: "fallback",
      message: "No live providers are currently reachable."
    };
  }

  async resolveRuntimeMode(options = {}) {
    const all = await this.checkAllProviders(options);
    const activeProvider = this.selectActiveProvider(all.providers);
    const useFallback = activeProvider.providerId === "deterministic_fallback";
    const google = all.providers.find((item) => item.providerId === "google_earth");

    return {
      connectivity: google,
      providers: all.providers,
      providerCount: all.providerCount,
      liveProviderCount: all.liveProviderCount,
      activeProvider,
      isLive: !useFallback,
      useFallback,
      fallbackReason: useFallback ? (google?.status || "all_providers_unavailable") : null
    };
  }

  async fetchEarthEngineHints(date, boundsKey) {
    const metaUrl = this.buildEarthEngineUrl(`/v1beta/projects/${this.config.earthEngineProject}/assets/NOAA_GFS0P25`);

    try {
      const response = await this.fetchWithTimeout(metaUrl, { method: "GET" });
      if (!response.ok) {
        return this.buildFallbackHints(date, boundsKey, `google_earth_http_${response.status}`);
      }

      const payload = await response.json();
      const idText = payload?.name || payload?.id || "earthengine";
      const liveSeed = this.providerSeed(date, boundsKey, "google_earth", idText);

      return {
        source: "earthengine_keyed_probe",
        providerId: "google_earth",
        providerName: PROVIDER_CATALOG.google_earth.name,
        rainfallMultiplier: Number((0.85 + (liveSeed % 28) / 100).toFixed(4)),
        soilMoistureBias: Number((-0.08 + (liveSeed % 17) / 100).toFixed(4)),
        wetnessBias: Number(((liveSeed % 10) / 100).toFixed(4)),
        ndviBias: Number((-0.1 + (liveSeed % 15) / 100).toFixed(4)),
        fetchedAt: new Date().toISOString(),
        earthEngineStatus: "connected"
      };
    } catch {
      return this.buildFallbackHints(date, boundsKey, "google_earth_probe_exception");
    }
  }

  pickSecondaryProvider(runtime, excludedProviderId) {
    return (runtime.providers || []).find(
      (provider) => provider.isLive && provider.providerId !== excludedProviderId
    );
  }

  // Multi-source ingestion hint selection:
  // 1) try active provider from priority list
  // 2) if no provider live, use deterministic fallback
  async fetchIngestionHints({ date = todayDate(), boundsKey = "default" } = {}) {
    const runtime = await this.resolveRuntimeMode({ force: true });

    if (runtime.useFallback) {
      return this.buildFallbackHints(date, boundsKey, runtime.fallbackReason, runtime);
    }

    const activeProvider = runtime.activeProvider;

    if (activeProvider.providerId === "google_earth") {
      const earthHints = await this.fetchEarthEngineHints(date, boundsKey);
      if (!String(earthHints.source || "").startsWith("simulated")) {
        return {
          ...earthHints,
          providerStatuses: runtime.providers
        };
      }

      const secondary = this.pickSecondaryProvider(runtime, "google_earth");
      if (secondary) {
        return {
          ...this.buildProviderHints(date, boundsKey, secondary),
          fallbackReason: earthHints.fallbackReason || earthHints.earthEngineStatus || "google_probe_failed",
          providerStatuses: runtime.providers
        };
      }

      return {
        ...earthHints,
        providerStatuses: runtime.providers
      };
    }

    return {
      ...this.buildProviderHints(date, boundsKey, activeProvider),
      providerStatuses: runtime.providers
    };
  }
}
