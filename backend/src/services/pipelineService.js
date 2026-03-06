import crypto from "node:crypto";
import fs from "node:fs/promises";
import path from "node:path";

const NRT_DRIVERS = [
  {
    id: "gpm_imerg_early",
    name: "GPM IMERG Early Run",
    variable: "Rainfall intensity & accumulation",
    cadence: "hourly",
    expectedLatencyHours: 4,
    provider: "Earth Engine"
  },
  {
    id: "smap_nrt",
    name: "SMAP Near Real-Time",
    variable: "Soil moisture",
    cadence: "daily",
    expectedLatencyHours: 24,
    provider: "Earth Engine"
  },
  {
    id: "viirs_nrt",
    name: "VIIRS NRT",
    variable: "Surface reflectance (NDVI source)",
    cadence: "daily",
    expectedLatencyHours: 12,
    provider: "Earth Engine"
  }
];

const CORRECTION_LAYERS = [
  {
    id: "sentinel1_sar",
    name: "Sentinel-1 SAR",
    purpose: "Flooding/wetness authoritative correction",
    cadence: "opportunistic",
    priority: "high"
  },
  {
    id: "sentinel2_optical",
    name: "Sentinel-2 Optical",
    purpose: "High-detail vegetation and water corrections",
    cadence: "5-day",
    priority: "medium"
  },
  {
    id: "landsat89",
    name: "Landsat 8/9",
    purpose: "Medium-detail correction",
    cadence: "16-day",
    priority: "medium"
  },
  {
    id: "hls",
    name: "Harmonized Landsat-Sentinel",
    purpose: "Cross-sensor continuity",
    cadence: "periodic",
    priority: "medium"
  }
];

const BASE_LAYERS = [
  {
    id: "esa_worldcover",
    name: "ESA WorldCover",
    purpose: "Land cover suitability",
    refresh: "annual"
  },
  {
    id: "jrc_surface_water",
    name: "JRC Global Surface Water",
    purpose: "Historical reliable water",
    refresh: "quarterly"
  },
  {
    id: "dem_slope",
    name: "DEM-derived slope",
    purpose: "Terrain movement constraints",
    refresh: "static"
  },
  {
    id: "settlement_proximity",
    name: "Settlement proximity",
    purpose: "Contextual modifier",
    refresh: "monthly"
  },
  {
    id: "conflict_priors",
    name: "Conflict/Avoidance priors",
    purpose: "Risk-aware penalties",
    refresh: "weekly"
  }
];

const FEATURE_KEYS = [
  "forageAvailability",
  "forageImprovementRate",
  "recentRainfall",
  "rainfallAnomaly",
  "soilMoisture",
  "surfaceWaterPresence",
  "floodingWaterlogging",
  "terrainSlope",
  "distanceToReliableWater",
  "landCoverSuitability",
  "proximityToSettlements",
  "historicalGrazingPressure",
  "seasonalTimingIndex",
  "vegetationTypeComposition",
  "conflictAvoidancePrior"
];

const DEFAULT_OPERATIONAL_BOUNDS = Object.freeze({
  minLat: 3,
  maxLat: 12,
  minLng: 24,
  maxLng: 36
});

const MIN_OPERATIONAL_SPAN_DEG = 0.4;
const MAX_OPERATIONAL_LAT_SPAN_DEG = 18;
const MAX_OPERATIONAL_LNG_SPAN_DEG = 20;
const MAX_PASTORAL_LATITUDE_ABS = 35;

function clamp(value, min = 0, max = 1) {
  return Math.max(min, Math.min(max, value));
}

function hashString(input) {
  let h = 2166136261;
  for (let i = 0; i < input.length; i += 1) {
    h ^= input.charCodeAt(i);
    h = Math.imul(h, 16777619);
  }
  return h >>> 0;
}

function seededUnit(seedText) {
  let x = hashString(seedText) || 1;
  x ^= x << 13;
  x ^= x >>> 17;
  x ^= x << 5;
  return (x >>> 0) / 4294967295;
}

function seededRange(seedText, min, max) {
  return min + (max - min) * seededUnit(seedText);
}

function pickWeighted(items, rand) {
  const total = items.reduce((sum, item) => sum + item.weight, 0);
  if (!total) return items[0]?.value;

  let cursor = rand * total;
  for (const item of items) {
    cursor -= item.weight;
    if (cursor <= 0) return item.value;
  }

  return items[items.length - 1]?.value;
}

function mean(values) {
  if (!values.length) return 0;
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

function stddev(values) {
  if (values.length < 2) return 0;
  const avg = mean(values);
  const variance = values.reduce((sum, value) => sum + (value - avg) ** 2, 0) / values.length;
  return Math.sqrt(variance);
}

function dayOfYear(dateString) {
  const date = new Date(`${dateString}T00:00:00.000Z`);
  const start = new Date(Date.UTC(date.getUTCFullYear(), 0, 1));
  const diffMs = date - start;
  return Math.floor(diffMs / (1000 * 60 * 60 * 24)) + 1;
}

function toDateString(value = new Date()) {
  const date = value instanceof Date ? value : new Date(value);
  return date.toISOString().slice(0, 10);
}

function shiftDate(dateToken, days) {
  const date = new Date(`${dateToken}T00:00:00.000Z`);
  date.setUTCDate(date.getUTCDate() + days);
  return toDateString(date);
}

function listDates(startDateToken, endDateToken, maxDays = 120) {
  const start = new Date(`${startDateToken}T00:00:00.000Z`);
  const end = new Date(`${endDateToken}T00:00:00.000Z`);
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime())) return [];
  if (start > end) return [];

  const dates = [];
  const cursor = new Date(start);
  while (cursor <= end && dates.length < maxDays) {
    dates.push(toDateString(cursor));
    cursor.setUTCDate(cursor.getUTCDate() + 1);
  }
  return dates;
}

function signedToUnit(signedValue) {
  return clamp((signedValue + 1) / 2, 0, 1);
}

function unitToSigned(unitValue) {
  return clamp(unitValue * 2 - 1, -1, 1);
}

function scoreToClass(score, uncertainty) {
  if (score >= 0.67 && uncertainty <= 0.55) return "High";
  if (score >= 0.4) return "Moderate";
  return "Low";
}

function signalBand(value) {
  if (value >= 0.67) return "Strong";
  if (value >= 0.4) return "Moderate";
  if (value > 0) return "Weak";
  return "Unavailable";
}

function uncertaintyBand(value) {
  if (value <= 0.35) return "High confidence";
  if (value <= 0.6) return "Medium confidence";
  return "Low confidence";
}

function haversineKm(aLat, aLng, bLat, bLng) {
  const R = 6371;
  const toRad = (deg) => (deg * Math.PI) / 180;
  const dLat = toRad(bLat - aLat);
  const dLng = toRad(bLng - aLng);
  const lat1 = toRad(aLat);
  const lat2 = toRad(bLat);

  const sinDLat = Math.sin(dLat / 2);
  const sinDLng = Math.sin(dLng / 2);
  const x = sinDLat ** 2 + Math.cos(lat1) * Math.cos(lat2) * sinDLng ** 2;
  const y = 2 * Math.atan2(Math.sqrt(x), Math.sqrt(1 - x));
  return R * y;
}

function rollingWindowLatency(source) {
  if (source.cadence === "hourly") return 4;
  if (source.cadence === "daily") return 24;
  if (source.cadence === "5-day") return 120;
  if (source.cadence === "16-day") return 384;
  if (source.cadence === "weekly") return 168;
  return 48;
}

function isFiniteCoordinate(lat, lng) {
  return Number.isFinite(lat) && Number.isFinite(lng) && Math.abs(lat) <= 90 && Math.abs(lng) <= 180;
}

function sanitizeBounds(bounds = {}, fallback = DEFAULT_OPERATIONAL_BOUNDS) {
  const rawMinLat = Number(bounds?.minLat);
  const rawMaxLat = Number(bounds?.maxLat);
  const rawMinLng = Number(bounds?.minLng);
  const rawMaxLng = Number(bounds?.maxLng);

  const minLat = Number.isFinite(rawMinLat) ? clamp(rawMinLat, -89.5, 89.5) : fallback.minLat;
  const maxLat = Number.isFinite(rawMaxLat) ? clamp(rawMaxLat, -89.5, 89.5) : fallback.maxLat;
  const minLng = Number.isFinite(rawMinLng) ? clamp(rawMinLng, -179.5, 179.5) : fallback.minLng;
  const maxLng = Number.isFinite(rawMaxLng) ? clamp(rawMaxLng, -179.5, 179.5) : fallback.maxLng;

  const orderedMinLat = Math.min(minLat, maxLat);
  const orderedMaxLat = Math.max(minLat, maxLat);
  const orderedMinLng = Math.min(minLng, maxLng);
  const orderedMaxLng = Math.max(minLng, maxLng);

  return {
    minLat: orderedMinLat,
    maxLat: orderedMaxLat,
    minLng: orderedMinLng,
    maxLng: orderedMaxLng
  };
}

function isOperationalBounds(bounds = {}) {
  const safe = sanitizeBounds(bounds);
  const latSpan = safe.maxLat - safe.minLat;
  const lngSpan = safe.maxLng - safe.minLng;
  return (
    latSpan >= MIN_OPERATIONAL_SPAN_DEG &&
    lngSpan >= MIN_OPERATIONAL_SPAN_DEG &&
    latSpan <= MAX_OPERATIONAL_LAT_SPAN_DEG &&
    lngSpan <= MAX_OPERATIONAL_LNG_SPAN_DEG &&
    Math.max(Math.abs(safe.minLat), Math.abs(safe.maxLat)) <= MAX_PASTORAL_LATITUDE_ABS
  );
}

function pointInsideBounds(lat, lng, bounds, padDeg = 0) {
  if (!isFiniteCoordinate(lat, lng)) return false;
  const safe = sanitizeBounds(bounds);
  return (
    lat >= safe.minLat - padDeg &&
    lat <= safe.maxLat + padDeg &&
    lng >= safe.minLng - padDeg &&
    lng <= safe.maxLng + padDeg
  );
}

function isOperationalCoordinate(lat, lng, bounds = DEFAULT_OPERATIONAL_BOUNDS) {
  if (!isFiniteCoordinate(lat, lng)) return false;
  if (Math.abs(lat) > MAX_PASTORAL_LATITUDE_ABS) return false;
  return pointInsideBounds(lat, lng, bounds, 1.2);
}

function getOrCreateRun(state, date) {
  let run = state.dailyRuns.find((item) => item.date === date);
  if (!run) {
    run = {
      date,
      createdAt: new Date().toISOString(),
      ingestion: null,
      featureVectors: [],
      gam: null,
      movement: null,
      validation: null,
      outputs: null,
      dataQuality: null,
      changeDetection: null,
      alerts: []
    };
    state.dailyRuns.push(run);
  } else {
    run.dataQuality = run.dataQuality || null;
    run.changeDetection = run.changeDetection || null;
    run.alerts = Array.isArray(run.alerts) ? run.alerts : [];
  }

  state.dailyRuns = state.dailyRuns
    .sort((a, b) => b.date.localeCompare(a.date))
    .slice(0, 370);

  return run;
}

function normalizeWeights(inputWeights, currentWeights) {
  const next = { ...currentWeights };
  for (const [key, value] of Object.entries(inputWeights || {})) {
    if (Object.prototype.hasOwnProperty.call(next, key) && Number.isFinite(Number(value))) {
      next[key] = clamp(Number(value), 0, 1);
    }
  }
  return next;
}

function summarizeAdmin(values) {
  const avgScore = mean(values.map((v) => v.score));
  const avgUncertainty = mean(values.map((v) => v.uncertainty));
  return {
    averageScore: Number(avgScore.toFixed(3)),
    averageUncertainty: Number(avgUncertainty.toFixed(3)),
    classification: scoreToClass(avgScore, avgUncertainty),
    confidence: uncertaintyBand(avgUncertainty)
  };
}

function pickDriverSummary(contributions) {
  if (!contributions.length) return "insufficient data";
  const sortedPositive = contributions
    .filter((item) => item.value > 0)
    .sort((a, b) => b.value - a.value)
    .slice(0, 2)
    .map((item) => item.driver);

  const sortedNegative = contributions
    .filter((item) => item.value < 0)
    .sort((a, b) => a.value - b.value)
    .slice(0, 1)
    .map((item) => item.driver);

  if (!sortedPositive.length && !sortedNegative.length) {
    return "neutral conditions";
  }

  if (!sortedNegative.length) {
    return `${sortedPositive.join(" + ")}`;
  }

  return `${sortedPositive.join(" + ")} with ${sortedNegative[0]} pressure`;
}

function quantile(values, q) {
  if (!values.length) return 0;
  const sorted = [...values].sort((a, b) => a - b);
  const pos = (sorted.length - 1) * q;
  const base = Math.floor(pos);
  const rest = pos - base;
  if (sorted[base + 1] !== undefined) {
    return sorted[base] + rest * (sorted[base + 1] - sorted[base]);
  }
  return sorted[base];
}

export class PipelineService {
  constructor(store, googleEarthService, config) {
    this.store = store;
    this.googleEarthService = googleEarthService;
    this.config = config;
  }

  activeOperationalBounds(masterGrid = null) {
    const candidate = sanitizeBounds(masterGrid?.bounds || DEFAULT_OPERATIONAL_BOUNDS);
    return isOperationalBounds(candidate) ? candidate : DEFAULT_OPERATIONAL_BOUNDS;
  }

  isMasterGridHealthy(masterGrid = null) {
    if (!masterGrid || !Array.isArray(masterGrid.cells) || !masterGrid.cells.length) return false;
    const bounds = sanitizeBounds(masterGrid.bounds || DEFAULT_OPERATIONAL_BOUNDS);
    if (!isOperationalBounds(bounds)) return false;

    let validPoints = 0;
    let operationalPoints = 0;
    for (const cell of masterGrid.cells) {
      const lat = Number(cell?.centroid?.lat);
      const lng = Number(cell?.centroid?.lng);
      if (!isFiniteCoordinate(lat, lng)) continue;
      validPoints += 1;
      if (isOperationalCoordinate(lat, lng, bounds)) operationalPoints += 1;
    }

    if (!validPoints) return false;
    const validRatio = validPoints / Math.max(1, masterGrid.cells.length);
    const operationalRatio = operationalPoints / validPoints;
    return validRatio >= 0.97 && operationalRatio >= 0.9;
  }

  runGridAlignmentRatio(run, gridCellIds, horizonDays = 7) {
    if (!run || !(gridCellIds instanceof Set) || !gridCellIds.size) return 0;
    const heatmap = run.movement?.simulations?.[horizonDays]?.probabilityHeatmap || [];
    if (!heatmap.length) return 0;
    let hits = 0;
    for (const item of heatmap) {
      if (gridCellIds.has(item?.cellId)) hits += 1;
    }
    return hits / heatmap.length;
  }

  summarizeDataQuality(run) {
    const missingRate = run.ingestion?.missingSignalRate ?? 0;
    const latency = run.ingestion?.sourceLatencyHours?.nearRealTime ?? 24;
    const fallbackActive = String(run.ingestion?.earthHints?.source || "").startsWith("simulated");
    const sample = run.featureVectors || [];
    const completeness = 1 - missingRate;

    let status = "good";
    if (missingRate > 0.14 || latency > 48) status = "poor";
    else if (missingRate > 0.08 || latency > 30) status = "watch";

    return {
      date: run.date,
      computedAt: new Date().toISOString(),
      status,
      completeness: Number(clamp(completeness, 0, 1).toFixed(4)),
      missingSignalRate: Number(clamp(missingRate, 0, 1).toFixed(4)),
      fallbackActive,
      earthHintSource: run.ingestion?.earthHints?.source || "unknown",
      nearRealTimeLatencyHours: latency,
      featureSampleSize: sample.length,
      note:
        status === "good"
          ? "Data quality is within expected operating thresholds."
          : status === "watch"
            ? "Monitor data freshness and signal gaps."
            : "Data quality degraded. Treat outputs with caution."
    };
  }

  compareCorridors(currentCorridors, previousCorridors) {
    const prevByKey = new Map(previousCorridors.map((item) => [`${item.fromCellId}|${item.toCellId}`, item]));
    const currByKey = new Map(currentCorridors.map((item) => [`${item.fromCellId}|${item.toCellId}`, item]));
    const changes = [];

    for (const corridor of currentCorridors) {
      const key = `${corridor.fromCellId}|${corridor.toCellId}`;
      const prev = prevByKey.get(key);
      if (!prev) {
        changes.push({
          type: "new",
          corridor,
          deltaProbability: corridor.probability
        });
        continue;
      }

      const delta = corridor.probability - (prev.probability || 0);
      if (delta >= 0.006) {
        changes.push({
          type: "intensified",
          corridor,
          previous: prev,
          deltaProbability: Number(delta.toFixed(5))
        });
      } else if (delta <= -0.006) {
        changes.push({
          type: "weakened",
          corridor,
          previous: prev,
          deltaProbability: Number(delta.toFixed(5))
        });
      }
    }

    for (const prev of previousCorridors) {
      const key = `${prev.fromCellId}|${prev.toCellId}`;
      if (!currByKey.has(key)) {
        changes.push({
          type: "resolved",
          corridor: prev,
          deltaProbability: Number((0 - (prev.probability || 0)).toFixed(5))
        });
      }
    }

    const summary = {
      new: changes.filter((item) => item.type === "new").length,
      intensified: changes.filter((item) => item.type === "intensified").length,
      weakened: changes.filter((item) => item.type === "weakened").length,
      resolved: changes.filter((item) => item.type === "resolved").length
    };

    return { changes, summary };
  }

  buildAlertFingerprint(type, date, details = {}) {
    return `${type}:${date}:${details.fromCellId || ""}:${details.toCellId || ""}:${details.reason || ""}`;
  }

  createAlert({
    date,
    type,
    severity = "medium",
    message,
    details = {}
  }) {
    return {
      id: crypto.randomUUID(),
      date,
      type,
      severity,
      message,
      details,
      status: "open",
      createdAt: new Date().toISOString(),
      fingerprint: this.buildAlertFingerprint(type, date, details)
    };
  }

  async registerDataSources({ force = false } = {}) {
    const providerRuntime = await this.googleEarthService.resolveRuntimeMode();
    const earthStatus = providerRuntime.connectivity;

    const state = await this.store.transact((draft) => {
      if (draft.dataSources.status === "registered" && !force) {
        return draft;
      }

      draft.dataSources.nrtDrivers = NRT_DRIVERS;
      draft.dataSources.correctionLayers = CORRECTION_LAYERS;
      draft.dataSources.baseLayers = BASE_LAYERS;
      draft.dataSources.status = "registered";
      draft.dataSources.registrationLog.push({
        at: new Date().toISOString(),
        earthStatus,
        providerRuntime: {
          activeProvider: providerRuntime.activeProvider,
          liveProviderCount: providerRuntime.liveProviderCount,
          providerCount: providerRuntime.providerCount,
          useFallback: providerRuntime.useFallback
        }
      });

      return draft;
    });

    return {
      status: state.dataSources.status,
      earthStatus,
      providerRuntime,
      nrtDrivers: state.dataSources.nrtDrivers,
      correctionLayers: state.dataSources.correctionLayers,
      baseLayers: state.dataSources.baseLayers
    };
  }

  async createMasterGrid({
    countryCode = this.config.defaultCountryCode,
    resolutionKm = this.config.defaultGridResolutionKm,
    maxCells = 800,
    bounds = DEFAULT_OPERATIONAL_BOUNDS
  } = {}) {
    const safeBounds = sanitizeBounds(bounds, DEFAULT_OPERATIONAL_BOUNDS);
    const bounded = isOperationalBounds(safeBounds) ? safeBounds : DEFAULT_OPERATIONAL_BOUNDS;
    const safeCountryCode = String(countryCode || this.config.defaultCountryCode || "UN-DEMO").trim() || "UN-DEMO";
    const clippedResolution = clamp(Number(resolutionKm), 2, 5);
    const latStep = clippedResolution / 111;
    const midLat = (bounded.minLat + bounded.maxLat) / 2;
    const lngStep = clippedResolution / (111 * Math.cos((midLat * Math.PI) / 180));

    const rows = Math.max(1, Math.ceil((bounded.maxLat - bounded.minLat) / latStep));
    const cols = Math.max(1, Math.ceil((bounded.maxLng - bounded.minLng) / lngStep));
    const expectedCells = rows * cols;
    const stride = Math.max(1, Math.ceil(Math.sqrt(expectedCells / Math.max(1, maxCells))));

    const cells = [];
    for (let r = 0; r < rows; r += stride) {
      for (let c = 0; c < cols; c += stride) {
        const lat = bounded.minLat + (r + 0.5) * latStep;
        const lng = bounded.minLng + (c + 0.5) * lngStep;
        if (lat >= bounded.maxLat || lng >= bounded.maxLng) {
          continue;
        }
        if (!isOperationalCoordinate(lat, lng, bounded)) {
          continue;
        }

        const countyIndex = Math.floor((r / rows) * 8) + 1;
        const payamIndex = Math.floor((c / cols) * 14) + 1;

        cells.push({
          id: `${safeCountryCode}-${r}-${c}`,
          row: r,
          col: c,
          centroid: {
            lat: Number(lat.toFixed(5)),
            lng: Number(lng.toFixed(5))
          },
          admin: {
            county: `County-${countyIndex}`,
            payam: `Payam-${payamIndex}`
          }
        });
      }
    }

    const state = await this.store.transact((draft) => {
      draft.masterGrid = {
        countryCode: safeCountryCode,
        resolutionKm: clippedResolution,
        bounds: bounded,
        rows,
        cols,
        stride,
        cellCount: cells.length,
        cells,
        createdAt: new Date().toISOString()
      };
      return draft;
    });

    return state.masterGrid;
  }

  async ensureBootstrapped() {
    const state = await this.store.read();
    if (state.dataSources.status !== "registered") {
      await this.registerDataSources();
    }
    if (!this.isMasterGridHealthy(state.masterGrid)) {
      await this.createMasterGrid({
        countryCode: this.config.defaultCountryCode,
        resolutionKm: this.config.defaultGridResolutionKm,
        maxCells: 800,
        bounds: DEFAULT_OPERATIONAL_BOUNDS
      });
    }
  }

  async ingestDaily({ date = toDateString() } = {}) {
    await this.ensureBootstrapped();

    const state = await this.store.read();
    const grid = state.masterGrid;
    const dateToken = toDateString(date);
    const doy = dayOfYear(dateToken);

    const hints = await this.googleEarthService.fetchIngestionHints({
      date: dateToken,
      boundsKey: `${grid.bounds.minLat},${grid.bounds.maxLat},${grid.bounds.minLng},${grid.bounds.maxLng}`
    });

    const featureVectors = grid.cells.map((cell) => {
      const spatialSeed = `${dateToken}:${cell.id}`;
      const season = 0.5 + 0.35 * Math.sin(((doy - 45) / 365) * Math.PI * 2);
      const northGradient = clamp((cell.centroid.lat - grid.bounds.minLat) / (grid.bounds.maxLat - grid.bounds.minLat), 0, 1);
      const eastGradient = clamp((cell.centroid.lng - grid.bounds.minLng) / (grid.bounds.maxLng - grid.bounds.minLng), 0, 1);

      const rainfall7 = clamp(0.2 + season * 0.5 + seededRange(`${spatialSeed}:rain7`, -0.15, 0.18), 0, 1);
      const rainfall1 = clamp(rainfall7 * seededRange(`${spatialSeed}:rain1`, 0.35, 1), 0, 1);
      const rainfall3 = clamp(rainfall7 * seededRange(`${spatialSeed}:rain3`, 0.55, 1), 0, 1);
      const rainfallAnomaly = clamp(unitToSigned(rainfall7) + hints.rainfallMultiplier - 1 + seededRange(`${spatialSeed}:anom`, -0.15, 0.15), -1, 1);
      const soilMoisture = clamp(rainfall7 * 0.6 + season * 0.2 + hints.soilMoistureBias + seededRange(`${spatialSeed}:soil`, -0.1, 0.1), 0, 1);
      const surfaceWaterPresence = clamp(soilMoisture * 0.55 + northGradient * 0.18 + seededRange(`${spatialSeed}:water`, -0.14, 0.14), 0, 1);
      const floodingWaterlogging = clamp(surfaceWaterPresence * 0.45 + hints.wetnessBias + seededRange(`${spatialSeed}:flood`, -0.1, 0.2), 0, 1);
      const terrainSlope = clamp(0.25 + seededRange(`${spatialSeed}:slope`, 0, 0.6) + eastGradient * 0.08, 0, 1);
      const distanceToReliableWater = clamp(1 - surfaceWaterPresence + seededRange(`${spatialSeed}:distwater`, -0.2, 0.2), 0, 1);

      const forageAvailability = clamp(0.25 + season * 0.45 + rainfall7 * 0.2 + hints.ndviBias + seededRange(`${spatialSeed}:ndvi`, -0.2, 0.15), 0, 1);
      const forageImprovementRate = clamp(unitToSigned(rainfall3 - 0.5) + seededRange(`${spatialSeed}:dndvi`, -0.25, 0.25), -1, 1);
      const landCoverSuitability = clamp(0.35 + (1 - terrainSlope) * 0.35 + seededRange(`${spatialSeed}:land`, -0.12, 0.18), 0, 1);
      const proximityToSettlements = clamp(0.2 + eastGradient * 0.6 + seededRange(`${spatialSeed}:sett`, -0.2, 0.2), 0, 1);
      const historicalGrazingPressure = clamp(0.25 + northGradient * 0.25 + seededRange(`${spatialSeed}:grazing`, -0.15, 0.2), 0, 1);
      const seasonalTimingIndex = clamp(season + seededRange(`${spatialSeed}:timing`, -0.15, 0.15), 0, 1);
      const vegetationTypeComposition = clamp(0.4 + (1 - eastGradient) * 0.25 + seededRange(`${spatialSeed}:vegtype`, -0.15, 0.15), 0, 1);
      const conflictAvoidancePrior = clamp(0.15 + seededRange(`${spatialSeed}:conflict`, 0, 0.5), 0, 1);

      const featureSet = {
        forageAvailability,
        forageImprovementRate,
        recentRainfall: rainfall7,
        rainfallAnomaly,
        soilMoisture,
        surfaceWaterPresence,
        floodingWaterlogging,
        terrainSlope,
        distanceToReliableWater,
        landCoverSuitability,
        proximityToSettlements,
        historicalGrazingPressure,
        seasonalTimingIndex,
        vegetationTypeComposition,
        conflictAvoidancePrior
      };

      const missingSignals = [];
      const fallbackSource = String(hints.source || "");
      const missingProbability = clamp(0.03 + (fallbackSource.startsWith("simulated") ? 0.03 : 0.015), 0, 0.08);
      const completedFeatureSet = { ...featureSet };

      for (const key of FEATURE_KEYS) {
        const missingSeed = seededUnit(`${spatialSeed}:missing:${key}`);
        if (missingSeed < missingProbability) {
          missingSignals.push(key);
          completedFeatureSet[key] = null;
        }
      }

      return {
        cellId: cell.id,
        row: cell.row,
        col: cell.col,
        centroid: cell.centroid,
        admin: cell.admin,
        rainfall: {
          oneDayTotal: Number((rainfall1 * 45).toFixed(2)),
          threeDayTotal: Number((rainfall3 * 110).toFixed(2)),
          sevenDayTotal: Number((rainfall7 * 250).toFixed(2))
        },
        ...completedFeatureSet,
        missingSignals
      };
    });

    const sourceLatencyHours = {
      nearRealTime: Math.max(...NRT_DRIVERS.map((driver) => rollingWindowLatency(driver))),
      correctionLayers: Math.max(...CORRECTION_LAYERS.map((driver) => rollingWindowLatency(driver))),
      baseLayers: Math.max(...BASE_LAYERS.map((driver) => rollingWindowLatency(driver)))
    };

    const nextState = await this.store.transact((draft) => {
      const run = getOrCreateRun(draft, dateToken);
      run.ingestion = {
        ingestedAt: new Date().toISOString(),
        earthHints: hints,
        sourceLatencyHours,
        records: featureVectors.length,
        missingSignalRate: Number(
          (
            featureVectors.reduce((sum, item) => sum + item.missingSignals.length, 0) /
            Math.max(1, featureVectors.length * FEATURE_KEYS.length)
          ).toFixed(4)
        )
      };
      run.featureVectors = featureVectors;
      return draft;
    });

    return nextState.dailyRuns.find((item) => item.date === dateToken);
  }

  scoreFeature(feature, weights) {
    const safe = (value, neutral = 0.5) => (value == null ? neutral : value);

    const normalizedForageImprovement = signedToUnit(safe(feature.forageImprovementRate, 0));
    const normalizedRainfallAnomaly = signedToUnit(safe(feature.rainfallAnomaly, 0));

    const settlementPreference = 1 - Math.abs(safe(feature.proximityToSettlements) - 0.45) * 1.7;
    const grazingPreference = 1 - Math.abs(safe(feature.historicalGrazingPressure) - 0.6) * 1.4;

    const contributions = [
      { driver: "forage availability", value: weights.forageAvailability * safe(feature.forageAvailability) },
      {
        driver: "forage improvement",
        value: weights.forageImprovementRate * normalizedForageImprovement
      },
      { driver: "recent rainfall", value: weights.recentRainfall * safe(feature.recentRainfall) },
      { driver: "rainfall anomaly", value: weights.rainfallAnomaly * normalizedRainfallAnomaly },
      { driver: "soil moisture", value: weights.soilMoisture * safe(feature.soilMoisture) },
      {
        driver: "surface water",
        value: weights.surfaceWaterPresence * safe(feature.surfaceWaterPresence)
      },
      {
        driver: "land cover suitability",
        value: weights.landCoverSuitability * safe(feature.landCoverSuitability)
      },
      {
        driver: "settlement proximity balance",
        value: weights.settlementProximityModifier * clamp(settlementPreference, 0, 1)
      },
      {
        driver: "historical grazing pressure",
        value: weights.historicalGrazingPressure * clamp(grazingPreference, 0, 1)
      },
      {
        driver: "seasonal timing",
        value: weights.seasonalTimingIndex * safe(feature.seasonalTimingIndex)
      },
      {
        driver: "vegetation type composition",
        value: weights.vegetationTypeComposition * safe(feature.vegetationTypeComposition)
      },
      {
        driver: "flooding constraints",
        value: -weights.floodingPenalty * safe(feature.floodingWaterlogging)
      },
      {
        driver: "terrain slope constraints",
        value: -weights.terrainSlopePenalty * safe(feature.terrainSlope)
      },
      {
        driver: "distance from reliable water",
        value: -weights.distanceToWaterPenalty * safe(feature.distanceToReliableWater)
      },
      {
        driver: "conflict avoidance",
        value: -weights.conflictAvoidancePenalty * safe(feature.conflictAvoidancePrior)
      }
    ];

    const rawScore = contributions.reduce((sum, item) => sum + item.value, 0);
    const score = clamp((rawScore + 0.25) / 0.95, 0, 1);

    const conflictSpread = stddev([
      safe(feature.forageAvailability),
      safe(feature.surfaceWaterPresence),
      safe(feature.recentRainfall),
      1 - safe(feature.distanceToReliableWater),
      1 - safe(feature.conflictAvoidancePrior)
    ]);
    const missingRate = feature.missingSignals.length / FEATURE_KEYS.length;
    const uncertainty = clamp(0.18 + missingRate * 0.6 + conflictSpread * 0.35, 0.05, 0.95);

    return {
      score,
      uncertainty,
      classification: scoreToClass(score, uncertainty),
      contributions,
      explanation: pickDriverSummary(contributions)
    };
  }

  async runGAM({ date = toDateString() } = {}) {
    const dateToken = toDateString(date);

    let state = await this.store.read();
    let run = state.dailyRuns.find((item) => item.date === dateToken);
    if (!run?.featureVectors?.length) {
      await this.ingestDaily({ date: dateToken });
      state = await this.store.read();
      run = state.dailyRuns.find((item) => item.date === dateToken);
    }

    const weights = state.model.weights;
    const cells = run.featureVectors.map((feature) => {
      const score = this.scoreFeature(feature, weights);
      return {
        cellId: feature.cellId,
        centroid: feature.centroid,
        admin: feature.admin,
        score: Number(score.score.toFixed(4)),
        uncertainty: Number(score.uncertainty.toFixed(4)),
        classification: score.classification,
        explanation: score.explanation,
        contributions: score.contributions.map((item) => ({
          driver: item.driver,
          value: Number(item.value.toFixed(4))
        }))
      };
    });

    const nextState = await this.store.transact((draft) => {
      const draftRun = getOrCreateRun(draft, dateToken);
      draftRun.gam = {
        generatedAt: new Date().toISOString(),
        modelVersion: draft.model.version,
        cells,
        summary: {
          averageScore: Number(mean(cells.map((cell) => cell.score)).toFixed(4)),
          averageUncertainty: Number(mean(cells.map((cell) => cell.uncertainty)).toFixed(4)),
          highCells: cells.filter((cell) => cell.classification === "High").length,
          moderateCells: cells.filter((cell) => cell.classification === "Moderate").length,
          lowCells: cells.filter((cell) => cell.classification === "Low").length
        }
      };
      return draft;
    });

    return nextState.dailyRuns.find((item) => item.date === dateToken).gam;
  }

  buildCellLookups(grid) {
    const byId = new Map();
    const byCoord = new Map();

    for (const cell of grid.cells) {
      byId.set(cell.id, cell);
      byCoord.set(`${cell.row}:${cell.col}`, cell);
    }

    const neighborsOf = (cell) => {
      const neighbors = [];
      for (let dr = -1; dr <= 1; dr += 1) {
        for (let dc = -1; dc <= 1; dc += 1) {
          if (dr === 0 && dc === 0) continue;
          const key = `${cell.row + dr * grid.stride}:${cell.col + dc * grid.stride}`;
          const neighbor = byCoord.get(key);
          if (neighbor) neighbors.push(neighbor);
        }
      }
      return neighbors;
    };

    return { byId, neighborsOf };
  }

  async buildGroundSignalLayer({ date = toDateString() } = {}) {
    const dateToken = toDateString(date);
    const state = await this.store.read();

    const cutoff = new Date(`${dateToken}T00:00:00.000Z`);
    cutoff.setUTCDate(cutoff.getUTCDate() - 7);

    const reports = state.communityReports.filter((report) => new Date(report.submittedAt) >= cutoff);

    const grouped = new Map();
    for (const report of reports) {
      const key = report.location.cellId;
      const bucket = grouped.get(key) || {
        cellId: key,
        reportCount: 0,
        verifiedCount: 0,
        weightedTrust: 0,
        directions: {}
      };

      bucket.reportCount += 1;
      bucket.verifiedCount += report.verified ? 1 : 0;
      bucket.weightedTrust += report.trustScore;
      const direction = report.directionOfTravel || "unknown";
      bucket.directions[direction] = (bucket.directions[direction] || 0) + report.trustScore;
      grouped.set(key, bucket);
    }

    return Array.from(grouped.values()).map((item) => {
      const dominantDirection = Object.entries(item.directions)
        .sort((a, b) => b[1] - a[1])[0]?.[0] || "unknown";
      return {
        cellId: item.cellId,
        reportCount: item.reportCount,
        verifiedCount: item.verifiedCount,
        trustWeightedScore: Number((item.weightedTrust / Math.max(1, item.reportCount)).toFixed(3)),
        dominantDirection
      };
    });
  }

  async runMovementForecast({ date = toDateString(), horizons = [7, 14], particleCount = 920 } = {}) {
    const dateToken = toDateString(date);

    let state = await this.store.read();
    let run = state.dailyRuns.find((item) => item.date === dateToken);

    if (!run?.gam?.cells?.length) {
      await this.runGAM({ date: dateToken });
      state = await this.store.read();
      run = state.dailyRuns.find((item) => item.date === dateToken);
    }

    const grid = state.masterGrid;
    const { byId, neighborsOf } = this.buildCellLookups(grid);

    const gamByCell = new Map(run.gam.cells.map((cell) => [cell.cellId, cell]));
    const featureByCell = new Map(run.featureVectors.map((item) => [item.cellId, item]));

    let costSurface = run.gam.cells.map((cell) => {
      const feature = featureByCell.get(cell.cellId);
      const flood = feature?.floodingWaterlogging ?? 0.5;
      const slope = feature?.terrainSlope ?? 0.5;
      const waterDistance = feature?.distanceToReliableWater ?? 0.5;
      const surfaceWater = feature?.surfaceWaterPresence ?? 0.5;
      const conflict = feature?.conflictAvoidancePrior ?? 0.5;
      const forage = feature?.forageAvailability ?? 0.5;
      const habitatScore = clamp(
        (1 - flood) * 0.34 +
        (1 - slope) * 0.28 +
        (1 - waterDistance) * 0.2 +
        forage * 0.12 +
        cell.score * 0.06,
        0,
        1
      );
      const severePenalty = flood >= 0.78 || slope >= 0.84;
      const waterStressDeadZone = waterDistance >= 0.93 && forage <= 0.34;
      const openWaterLikelihood = clamp(
        surfaceWater * 0.58 +
        flood * 0.26 +
        (1 - waterDistance) * 0.12 +
        (1 - forage) * 0.04 -
        slope * 0.08,
        0,
        1
      );
      const openWaterBody = openWaterLikelihood >= 0.86 && slope <= 0.34;
      const compoundTerrainFlood = flood >= 0.68 && slope >= 0.72;
      const lowHabitatWithPenalties = habitatScore <= 0.2 && (flood >= 0.62 || slope >= 0.74 || waterDistance >= 0.88);
      const veryLowModelSupport = cell.score <= 0.12 && habitatScore <= 0.24;
      const isUninhabitable =
        severePenalty ||
        waterStressDeadZone ||
        openWaterBody ||
        compoundTerrainFlood ||
        lowHabitatWithPenalties ||
        veryLowModelSupport;

      const baseCost = 1 - cell.score + flood * 0.5 + slope * 0.35 + waterDistance * 0.3 + conflict * 0.45;
      const cost = isUninhabitable
        ? clamp(2.45 + flood * 0.7 + slope * 0.45 + waterDistance * 0.3, 2.4, 4.2)
        : clamp(baseCost, 0.05, 2.3);

      return {
        cellId: cell.cellId,
        cost: Number(cost.toFixed(4)),
        blocked: isUninhabitable,
        habitatScore: Number(habitatScore.toFixed(3)),
        penalties: {
          flooding: Number(flood.toFixed(3)),
          steepSlope: Number(slope.toFixed(3)),
          distanceFromWater: Number(waterDistance.toFixed(3)),
          openWater: Number(openWaterLikelihood.toFixed(3)),
          conflict: Number(conflict.toFixed(3))
        }
      };
    });

    const preliminaryCostByCell = new Map(costSurface.map((item) => [item.cellId, item]));
    costSurface = costSurface.map((item) => {
      const cell = byId.get(item.cellId);
      const neighbors = cell ? neighborsOf(cell) : [];
      if (!neighbors.length) return item;

      const neighborCosts = neighbors
        .map((neighbor) => preliminaryCostByCell.get(neighbor.id))
        .filter(Boolean);
      if (!neighborCosts.length) return item;

      const neighborhoodOpenWater = clamp(
        mean(neighborCosts.map((neighbor) => Number(neighbor?.penalties?.openWater ?? 0.5))),
        0,
        1
      );
      const drySupportCount = neighborCosts.filter((neighbor) =>
        !neighbor.blocked &&
        Number(neighbor?.penalties?.openWater ?? 0) < 0.62 &&
        Number(neighbor?.habitatScore ?? 0) >= 0.34
      ).length;
      const drySupportRatio = drySupportCount / Math.max(1, neighborCosts.length);
      const openWaterScore = Number(item?.penalties?.openWater ?? 0);

      const isolatedOpenWaterCluster =
        openWaterScore >= 0.8 ||
        (openWaterScore >= 0.7 && drySupportRatio <= 0.2) ||
        (openWaterScore >= 0.62 && neighborhoodOpenWater >= 0.75 && drySupportRatio <= 0.32) ||
        (openWaterScore >= 0.58 && neighborhoodOpenWater >= 0.8 && drySupportRatio <= 0.28) ||
        (neighborhoodOpenWater >= 0.84 && drySupportRatio <= 0.42);
      const blocked = Boolean(item.blocked || isolatedOpenWaterCluster);
      const adjustedCost = blocked
        ? clamp(Math.max(Number(item.cost || 0), 2.95 + openWaterScore * 1.05), 2.8, 4.45)
        : Number(item.cost || 0);

      return {
        ...item,
        blocked,
        cost: Number(adjustedCost.toFixed(4)),
        penalties: {
          ...item.penalties,
          neighborhoodOpenWater: Number(neighborhoodOpenWater.toFixed(3))
        }
      };
    });

    const costByCell = new Map(costSurface.map((item) => [item.cellId, item]));

    const groundSignals = await this.buildGroundSignalLayer({ date: dateToken });
    const groundByCell = new Map(groundSignals.map((item) => [item.cellId, item]));
    const signalAnchors = groundSignals
      .sort((a, b) => b.trustWeightedScore - a.trustWeightedScore)
      .map((item) => item.cellId)
      .filter((id) => byId.has(id) && !costByCell.get(id)?.blocked);

    const gamAnchors = [...run.gam.cells]
      .sort((a, b) => b.score - a.score)
      .filter((item) => !costByCell.get(item.cellId)?.blocked)
      .slice(0, 32)
      .map((item) => item.cellId);

    let startAnchors = Array.from(new Set([...signalAnchors, ...gamAnchors])).slice(0, 42);
    if (!startAnchors.length) {
      startAnchors = [...run.gam.cells]
        .sort((a, b) => b.score - a.score)
        .slice(0, 24)
        .map((item) => item.cellId);
    }

    const sortedRuns = [...state.dailyRuns].sort((a, b) => a.date.localeCompare(b.date));
    const runIndex = sortedRuns.findIndex((item) => item.date === dateToken);
    const previousRun = runIndex > 0 ? sortedRuns[runIndex - 1] : null;
    const previousHeatByCell = new Map(
      (previousRun?.movement?.simulations?.[7]?.probabilityHeatmap || []).map((item) => [item.cellId, Number(item.probability || 0)])
    );
    const recentValidations = sortedRuns
      .filter((item) => item.date < dateToken && item.validation && Number(item.validation.sampleSize || 0) >= 4)
      .slice(-21)
      .map((item) => item.validation);
    const avgValidationOverlap = recentValidations.length
      ? mean(recentValidations.map((item) => Number(item.spatialOverlap || 0)))
      : 0.56;
    const avgValidationBrier = recentValidations.length
      ? mean(recentValidations.map((item) => Number(item.probabilityCalibrationBrier || 0)))
      : 0.29;
    const avgValidationDistance = recentValidations.length
      ? mean(recentValidations.map((item) => Number(item.meanDistanceToPredictedKm || 0)))
      : 58;
    const validationReliability = clamp(
      avgValidationOverlap * 0.58 + (1 - avgValidationBrier) * 0.3 + (1 - clamp(avgValidationDistance / 220, 0, 1)) * 0.12,
      0.35,
      0.98
    );
    const calibrationFactor = clamp(
      0.76 + avgValidationOverlap * 0.34 + (1 - avgValidationBrier) * 0.24 - clamp(avgValidationDistance / 260, 0, 1) * 0.16,
      0.68,
      1.12
    );
    const missingSignalRate = clamp(Number(run.ingestion?.missingSignalRate || 0), 0, 1);
    const nearRealTimeLatency = Number(run.ingestion?.sourceLatencyHours?.nearRealTime || 24);
    const freshnessFactor = clamp(1 - missingSignalRate * 0.45 - Math.max(0, nearRealTimeLatency - 24) / 240, 0.62, 1.04);

    const herdSizePerParticle = 18;

    const horizonOutputs = {};
    for (const horizonDays of horizons) {
      const visits = new Map();
      const transitions = new Map();

      for (let particle = 0; particle < particleCount; particle += 1) {
        const startIndex = Math.floor(
          seededUnit(`${dateToken}:start:${particle}:${horizonDays}`) * Math.max(1, startAnchors.length)
        );
        let currentId = startAnchors[startIndex] || gamAnchors[0];

        for (let step = 0; step < horizonDays; step += 1) {
          const currentCell = byId.get(currentId);
          if (!currentCell) continue;

          visits.set(currentId, (visits.get(currentId) || 0) + 1);

          const candidates = [currentCell, ...neighborsOf(currentCell)].filter((candidate) => !costByCell.get(candidate.id)?.blocked);
          if (!candidates.length) {
            continue;
          }

          const weightedCandidates = candidates.map((candidate) => {
            const gamCell = gamByCell.get(candidate.id);
            const costCell = costByCell.get(candidate.id);
            const ground = groundByCell.get(candidate.id);
            const score = gamCell?.score ?? 0.4;
            const uncertaintyPenalty = gamCell?.uncertainty ?? 0.4;
            const cost = costCell?.cost ?? 1;
            const habitatScore = Number(costCell?.habitatScore ?? 0.45);
            const momentum = clamp(previousHeatByCell.get(candidate.id) || 0, 0, 0.08);
            const communitySignal = clamp(
              (Number(ground?.trustWeightedScore || 0) * 0.78) +
              (Math.min(4, Number(ground?.verifiedCount || 0)) * 0.055),
              0,
              1
            );
            const trackerAgreement = clamp(
              score * 0.4 +
              habitatScore * 0.24 +
              (1 - uncertaintyPenalty) * 0.15 +
              communitySignal * 0.13 +
              clamp(momentum * 12, 0, 1) * 0.08,
              0,
              1
            );
            const disagreementPenalty = clamp(1 - trackerAgreement, 0, 1);

            const weight = clamp(
              (1.4 - cost) +
              score * 0.82 +
              habitatScore * 0.24 +
              momentum * 5.1 +
              communitySignal * 0.28 -
              uncertaintyPenalty * 0.22 -
              disagreementPenalty * 0.34,
              0.02,
              2.8
            );
            return {
              value: candidate.id,
              weight
            };
          });

          const moveRand = seededUnit(`${dateToken}:move:${particle}:${horizonDays}:${step}`);
          const nextId = pickWeighted(weightedCandidates, moveRand) || currentId;

          const edgeKey = `${currentId}|${nextId}`;
          transitions.set(edgeKey, (transitions.get(edgeKey) || 0) + 1);
          currentId = nextId;
        }
      }

      const normalizer = particleCount * horizonDays;
      const activeCellIds = run.gam.cells
        .map((cell) => cell.cellId)
        .filter((cellId) => !costByCell.get(cellId)?.blocked);

      const particleProbabilityByCell = new Map(
        activeCellIds.map((cellId) => [cellId, (visits.get(cellId) || 0) / Math.max(1, normalizer)])
      );

      const ecologicalRawByCell = new Map();
      const communitySeedByCell = new Map();
      activeCellIds.forEach((cellId) => {
        const ground = groundByCell.get(cellId);
        const seed = clamp(
          (Number(ground?.trustWeightedScore || 0) * 0.78) +
          (Math.min(5, Number(ground?.reportCount || 0)) * 0.05) +
          (Math.min(4, Number(ground?.verifiedCount || 0)) * 0.06),
          0,
          1
        );
        communitySeedByCell.set(cellId, seed);
      });

      activeCellIds.forEach((cellId) => {
        const cell = byId.get(cellId);
        const gamCell = gamByCell.get(cellId);
        const costCell = costByCell.get(cellId);
        const neighbors = (cell ? neighborsOf(cell) : []).filter((n) => !costByCell.get(n.id)?.blocked);
        const neighborGam = mean(neighbors.map((n) => Number(gamByCell.get(n.id)?.score || 0.4)));
        const flood = Number(costCell?.penalties?.flooding || 0.5);
        const slope = Number(costCell?.penalties?.steepSlope || 0.5);
        const waterDistance = Number(costCell?.penalties?.distanceFromWater || 0.5);
        const habitatScore = Number(costCell?.habitatScore || 0.45);
        const communitySeed = Number(communitySeedByCell.get(cellId) || 0);

        const ecologicalRaw = clamp(
          Number(gamCell?.score || 0.45) * 0.41 +
          habitatScore * 0.27 +
          (1 - flood) * 0.11 +
          (1 - slope) * 0.08 +
          (1 - waterDistance) * 0.08 +
          neighborGam * 0.03 +
          communitySeed * 0.02,
          0,
          1
        );
        ecologicalRawByCell.set(cellId, ecologicalRaw);
      });

      const communityRawByCell = new Map();
      const hasCommunityEvidence = groundSignals.length > 0;
      activeCellIds.forEach((cellId) => {
        const cell = byId.get(cellId);
        const costCell = costByCell.get(cellId);
        const neighbors = (cell ? neighborsOf(cell) : []).filter((n) => !costByCell.get(n.id)?.blocked);
        const seed = Number(communitySeedByCell.get(cellId) || 0);
        const neighborSeed = mean(neighbors.map((n) => Number(communitySeedByCell.get(n.id) || 0)));
        const temporal = clamp((previousHeatByCell.get(cellId) || 0) / 0.045, 0, 1);
        const habitatGate = 0.55 + Number(costCell?.habitatScore || 0.45) * 0.45;
        const raw = hasCommunityEvidence
          ? clamp(seed * 0.56 + neighborSeed * 0.22 + temporal * 0.22, 0, 1) * habitatGate
          : clamp(temporal * 0.7 + Number(particleProbabilityByCell.get(cellId) || 0) * 8, 0, 1) * habitatGate;
        communityRawByCell.set(cellId, clamp(raw, 0, 1));
      });

      const normalizeDistribution = (rawByCell, fallbackByCell = null) => {
        const total = Array.from(rawByCell.values()).reduce((sum, value) => sum + Number(value || 0), 0);
        if (total > 0) {
          return new Map(
            Array.from(rawByCell.entries()).map(([cellId, value]) => [cellId, Number(value || 0) / total])
          );
        }
        if (fallbackByCell) return new Map(fallbackByCell);
        return new Map(activeCellIds.map((cellId) => [cellId, 0]));
      };

      const ecologicalProbabilityByCell = normalizeDistribution(ecologicalRawByCell, particleProbabilityByCell);
      const communityProbabilityByCell = normalizeDistribution(communityRawByCell, particleProbabilityByCell);

      let fusionWeights = hasCommunityEvidence
        ? { particle: 0.5, ecological: 0.3, community: 0.2 }
        : { particle: 0.58, ecological: 0.42, community: 0 };
      if (missingSignalRate > 0.14) {
        fusionWeights = {
          particle: fusionWeights.particle + 0.05,
          ecological: Math.max(0.08, fusionWeights.ecological - 0.05),
          community: fusionWeights.community
        };
      }
      if (validationReliability < 0.55) {
        fusionWeights = {
          particle: Math.max(0.3, fusionWeights.particle - 0.02),
          ecological: fusionWeights.ecological + 0.03,
          community: Math.max(0, fusionWeights.community - 0.01)
        };
      }
      const weightTotal = Math.max(0.0001, fusionWeights.particle + fusionWeights.ecological + fusionWeights.community);
      fusionWeights = {
        particle: fusionWeights.particle / weightTotal,
        ecological: fusionWeights.ecological / weightTotal,
        community: fusionWeights.community / weightTotal
      };

      const fusedProbabilityByCell = new Map(
        activeCellIds.map((cellId) => {
          const particleP = Number(particleProbabilityByCell.get(cellId) || 0);
          const ecologicalP = Number(ecologicalProbabilityByCell.get(cellId) || 0);
          const communityP = Number(communityProbabilityByCell.get(cellId) || 0);
          return [
            cellId,
            (particleP * fusionWeights.particle) +
              (ecologicalP * fusionWeights.ecological) +
              (communityP * fusionWeights.community)
          ];
        })
      );

      const heatmap = activeCellIds
        .map((cellId) => {
          const cell = byId.get(cellId);
          const costCell = costByCell.get(cellId);
          const gamCell = gamByCell.get(cellId);
          const particleP = Number(particleProbabilityByCell.get(cellId) || 0);
          const ecologicalP = Number(ecologicalProbabilityByCell.get(cellId) || 0);
          const communityP = Number(communityProbabilityByCell.get(cellId) || 0);
          const rawProbability = Number(fusedProbabilityByCell.get(cellId) || 0);
          const calibratedProbability = clamp(rawProbability * calibrationFactor * freshnessFactor, 0, 1);
          const visitCount = Number(visits.get(cellId) || 0);
          const estimatedCattle = Math.max(0, Math.round((visitCount / Math.max(1, horizonDays)) * herdSizePerParticle));
          const habitatScore = clamp(Number(costCell?.habitatScore ?? 0), 0, 1);
          const activeTrackerValues = [particleP, ecologicalP, communityP].filter((value, idx) => idx < 2 || fusionWeights.community > 0);
          const trackerDisagreement = clamp(stddev(activeTrackerValues) / 0.12, 0, 1);
          const trackerAgreement = clamp(1 - trackerDisagreement, 0, 1);
          const projectionUncertainty = clamp(
            (1 - trackerAgreement) * 0.55 +
            Number(gamCell?.uncertainty || 0.5) * 0.27 +
            (1 - validationReliability) * 0.18,
            0.04,
            0.96
          );
          const falseProjectionRisk = clamp(
            (1 - trackerAgreement) * 0.52 +
            projectionUncertainty * 0.34 +
            (1 - habitatScore) * 0.14,
            0,
            1
          );
          return {
            cellId,
            probability: Number(calibratedProbability.toFixed(5)),
            centroid: cell.centroid,
            admin: cell.admin,
            blocked: false,
            habitatScore: Number(habitatScore.toFixed(3)),
            particleTrackerProbability: Number(particleP.toFixed(5)),
            ecologicalTrackerProbability: Number(ecologicalP.toFixed(5)),
            communityTrackerProbability: Number(communityP.toFixed(5)),
            trackerAgreement: Number(trackerAgreement.toFixed(3)),
            projectionUncertainty: Number(projectionUncertainty.toFixed(3)),
            falseProjectionRisk: Number(falseProjectionRisk.toFixed(3)),
            estimatedCattle,
            estimatedHerds: Math.max(0, Math.round(estimatedCattle / 34))
          };
        })
        .sort((a, b) => b.probability - a.probability);
      const heatByCell = new Map(heatmap.map((item) => [item.cellId, item]));

      const transitionTotal = Array.from(transitions.values()).reduce((sum, value) => sum + value, 0);
      const topCorridors = Array.from(transitions.entries())
        .map(([edgeKey, count]) => {
          const [fromCellId, toCellId] = edgeKey.split("|");
          const fromRisk = costByCell.get(fromCellId);
          const toRisk = costByCell.get(toCellId);
          if (fromRisk?.blocked || toRisk?.blocked) return null;

          const baseProbability = count / Math.max(1, transitionTotal);
          const penalty = clamp(
            ((toRisk?.penalties?.flooding ?? 0.4) * 0.38) +
            ((toRisk?.penalties?.steepSlope ?? 0.4) * 0.28) +
            ((toRisk?.penalties?.distanceFromWater ?? 0.4) * 0.2) +
            ((toRisk?.penalties?.conflict ?? 0.35) * 0.14),
            0,
            0.92
          );
          const fromMeta = heatByCell.get(fromCellId);
          const toMeta = heatByCell.get(toCellId);
          const particleCorridorProbability = baseProbability;
          const ecologicalCorridorProbability = clamp(
            (
              Number(fromMeta?.ecologicalTrackerProbability || 0) * 0.36 +
              Number(toMeta?.ecologicalTrackerProbability || 0) * 0.64
            ) * (1 - penalty * 0.36),
            0,
            1
          );
          const communityCorridorProbability = clamp(
            (
              Number(fromMeta?.communityTrackerProbability || 0) * 0.32 +
              Number(toMeta?.communityTrackerProbability || 0) * 0.68
            ) * (1 - penalty * 0.22),
            0,
            1
          );
          const trackerAgreement = clamp(
            (Number(fromMeta?.trackerAgreement || 0.45) * 0.42) +
            (Number(toMeta?.trackerAgreement || 0.45) * 0.34) +
            (1 - clamp(stddev([particleCorridorProbability, ecologicalCorridorProbability, communityCorridorProbability]) / 0.14, 0, 1)) * 0.08 +
            ((1 - penalty) * 0.16),
            0,
            1
          );
          const projectionUncertainty = clamp(
            (Number(fromMeta?.projectionUncertainty || 0.5) * 0.42) +
            (Number(toMeta?.projectionUncertainty || 0.5) * 0.42) +
            (penalty * 0.16),
            0.04,
            0.96
          );
          const falseProjectionRisk = clamp(
            (1 - trackerAgreement) * 0.52 +
            projectionUncertainty * 0.34 +
            penalty * 0.14,
            0,
            1
          );
          const endpointReliability = clamp(
            (Number(fromMeta?.habitatScore || 0.2) * 0.2) +
            (Number(toMeta?.habitatScore || 0.2) * 0.25) +
            trackerAgreement * 0.35 +
            (1 - falseProjectionRisk) * 0.2,
            0,
            1
          );
          if (endpointReliability < 0.24 || falseProjectionRisk >= 0.9) {
            return null;
          }
          const fusedBaseProbability = clamp(
            particleCorridorProbability * fusionWeights.particle +
              ecologicalCorridorProbability * fusionWeights.ecological +
              communityCorridorProbability * fusionWeights.community,
            0,
            1
          );
          const probability = clamp(
            fusedBaseProbability * (1 - penalty * 0.6) * (0.91 + trackerAgreement * 0.19) * (1 - falseProjectionRisk * 0.32) * calibrationFactor,
            0,
            1
          );
          const destination = gamByCell.get(toCellId);
          const estimatedCattleFlow = Math.max(
            0,
            Math.round((probability * normalizer / Math.max(1, horizonDays)) * herdSizePerParticle * (0.72 + trackerAgreement * 0.42) * (1 - falseProjectionRisk * 0.38))
          );

          let classification = "Low";
          if (probability >= 0.022 && trackerAgreement >= 0.56 && falseProjectionRisk < 0.52) classification = "High";
          else if (probability >= 0.009 && trackerAgreement >= 0.4 && falseProjectionRisk < 0.7) classification = "Moderate";

          return {
            fromCellId,
            toCellId,
            probability: Number(probability.toFixed(5)),
            classification,
            confidence: uncertaintyBand(projectionUncertainty),
            estimatedCattleFlow,
            estimatedHerds: Math.max(0, Math.round(estimatedCattleFlow / 34)),
            destinationRiskPenalty: Number(penalty.toFixed(3)),
            particleTrackerProbability: Number(particleCorridorProbability.toFixed(5)),
            ecologicalTrackerProbability: Number(ecologicalCorridorProbability.toFixed(5)),
            communityTrackerProbability: Number(communityCorridorProbability.toFixed(5)),
            trackerAgreement: Number(trackerAgreement.toFixed(3)),
            projectionUncertainty: Number(projectionUncertainty.toFixed(3)),
            falseProjectionRisk: Number(falseProjectionRisk.toFixed(3)),
            projectionStatus:
              falseProjectionRisk <= 0.38 ? "Reliable" : falseProjectionRisk <= 0.62 ? "Watch" : "Uncertain",
            driverSummary: destination?.explanation || "mixed drivers"
          };
        })
        .filter(Boolean)
        .sort((a, b) => b.probability - a.probability)
        .slice(0, 36);

      const topShare = topCorridors.slice(0, 5).reduce((sum, corridor) => sum + corridor.probability, 0);
      const confidenceBand = topShare > 0.28 ? "High" : topShare > 0.18 ? "Medium" : "Low";
      const estimatedCattleTotal = heatmap.reduce((sum, cell) => sum + (cell.estimatedCattle || 0), 0);
      const averageTrackerAgreement = mean(heatmap.map((cell) => Number(cell.trackerAgreement || 0)));
      const averageProjectionUncertainty = mean(heatmap.map((cell) => Number(cell.projectionUncertainty || 0)));
      const falseProjectionHotspots = heatmap.filter((cell) => Number(cell.falseProjectionRisk || 0) >= 0.68).length;
      const expectedAccuracy = clamp(
        validationReliability * 0.48 +
        averageTrackerAgreement * 0.34 +
        (1 - averageProjectionUncertainty) * 0.18,
        0.5,
        0.93
      );

      horizonOutputs[horizonDays] = {
        horizonDays,
        generatedAt: new Date().toISOString(),
        particleCount,
        probabilityHeatmap: heatmap,
        dominantCorridors: topCorridors,
        confidenceBand,
        blockedCellCount: costSurface.filter((cell) => cell.blocked).length,
        estimatedCattleTotal,
        averageTrackerAgreement: Number(averageTrackerAgreement.toFixed(3)),
        averageProjectionUncertainty: Number(averageProjectionUncertainty.toFixed(3)),
        falseProjectionHotspots,
        validationReliability: Number(validationReliability.toFixed(3)),
        calibrationFactor: Number(calibrationFactor.toFixed(3)),
        expectedAccuracy: Number(expectedAccuracy.toFixed(3)),
        fusionWeights: {
          particle: Number(fusionWeights.particle.toFixed(3)),
          ecological: Number(fusionWeights.ecological.toFixed(3)),
          community: Number(fusionWeights.community.toFixed(3))
        },
        trackerModels: [
          "particle_flow_tracker",
          "ecological_suitability_tracker",
          "community_momentum_tracker",
          "ensemble_fusion"
        ]
      };
    }

    const nextState = await this.store.transact((draft) => {
      const draftRun = getOrCreateRun(draft, dateToken);
      draftRun.movement = {
        generatedAt: new Date().toISOString(),
        costSurface,
        simulations: horizonOutputs,
        anchorCount: startAnchors.length
      };

      const defaultHorizon = horizonOutputs[7] ? 7 : Number(Object.keys(horizonOutputs)[0] || 7);
      draft.outputs.latestSimulation = draftRun.movement;
      draft.outputs.activeCorridors = draftRun.movement.simulations[defaultHorizon]?.dominantCorridors || [];
      draft.outputs.lastUpdated = new Date().toISOString();

      return draft;
    });

    return nextState.dailyRuns.find((item) => item.date === dateToken).movement;
  }

  async addCommunityReport({
    reporterId = "anonymous",
    lat,
    lng,
    cellId,
    directionOfTravel = "unknown",
    grazingStatus = "unknown",
    waterStatus = "unknown",
    notes = ""
  }) {
    await this.ensureBootstrapped();
    const state = await this.store.read();

    const grid = state.masterGrid;
    const locationCell =
      (cellId && grid.cells.find((cell) => cell.id === cellId)) ||
      (Number.isFinite(Number(lat)) && Number.isFinite(Number(lng))
        ? grid.cells
            .map((cell) => ({
              cell,
              distance: haversineKm(Number(lat), Number(lng), cell.centroid.lat, cell.centroid.lng)
            }))
            .sort((a, b) => a.distance - b.distance)[0]?.cell
        : null);

    if (!locationCell) {
      throw new Error("Report must include a valid grid cell ID or latitude/longitude inside the active grid.");
    }

    const trustProfile = state.reporterTrust[reporterId] || {
      score: 0.5,
      confirmations: 0,
      audits: 0
    };

    const directionPenalty = directionOfTravel.toLowerCase() === "dramatic" ? 0.08 : 0;
    const trustScore = clamp(trustProfile.score - directionPenalty, 0.1, 1);

    const report = {
      id: crypto.randomUUID(),
      reporterId,
      submittedAt: new Date().toISOString(),
      location: {
        cellId: locationCell.id,
        lat: Number((lat ?? locationCell.centroid.lat).toFixed(5)),
        lng: Number((lng ?? locationCell.centroid.lng).toFixed(5))
      },
      directionOfTravel,
      grazingStatus,
      waterStatus,
      notes,
      verified: false,
      reviewState: "pending",
      reviewNotes: "",
      trustScore: Number(trustScore.toFixed(3))
    };

    const nextState = await this.store.transact((draft) => {
      draft.communityReports.unshift(report);
      draft.communityReports = draft.communityReports.slice(0, 3000);

      draft.reporterTrust[reporterId] = {
        ...trustProfile,
        score: Number(trustProfile.score.toFixed(3))
      };

      return draft;
    });

    return nextState.communityReports.find((item) => item.id === report.id);
  }

  async verifyCommunityReport(reportId, { approved = true, reviewedBy = "system", reviewNotes = "" } = {}) {
    const nextState = await this.store.transact((draft) => {
      const report = draft.communityReports.find((item) => item.id === reportId);
      if (!report) {
        throw new Error("Report not found.");
      }

      report.verified = approved;
      report.reviewState = approved ? "approved" : "rejected";
      report.reviewedAt = new Date().toISOString();
      report.reviewedBy = reviewedBy;
      report.reviewNotes = reviewNotes;

      const profile = draft.reporterTrust[report.reporterId] || {
        score: 0.5,
        confirmations: 0,
        audits: 0
      };

      profile.audits += 1;
      if (approved) {
        profile.confirmations += 1;
        profile.score = clamp(profile.score + 0.04, 0.05, 1);
      } else {
        profile.score = clamp(profile.score - 0.07, 0.05, 1);
      }

      draft.reporterTrust[report.reporterId] = {
        ...profile,
        score: Number(profile.score.toFixed(3))
      };

      return draft;
    });

    return nextState.communityReports.find((item) => item.id === reportId);
  }

  async runValidation({ date = toDateString(), horizonDays = 7 } = {}) {
    const dateToken = toDateString(date);

    let state = await this.store.read();
    let run = state.dailyRuns.find((item) => item.date === dateToken);
    if (!run?.movement?.simulations?.[horizonDays]) {
      await this.runMovementForecast({ date: dateToken });
      state = await this.store.read();
      run = state.dailyRuns.find((item) => item.date === dateToken);
    }

    const simulation = run.movement.simulations[horizonDays];
    const probabilityMap = simulation.probabilityHeatmap;
    const threshold = quantile(
      probabilityMap.map((cell) => cell.probability),
      0.75
    );

    const highZone = probabilityMap.filter((cell) => cell.probability >= threshold);
    const highZoneIds = new Set(highZone.map((cell) => cell.cellId));

    const cutoff = new Date(`${dateToken}T00:00:00.000Z`);
    cutoff.setUTCDate(cutoff.getUTCDate() - 7);
    const verifiedReports = state.communityReports.filter(
      (report) => report.verified && new Date(report.submittedAt) >= cutoff
    );

    const overlapHits = verifiedReports.filter((report) => highZoneIds.has(report.location.cellId));
    const overlapRatio = verifiedReports.length
      ? overlapHits.length / verifiedReports.length
      : 0;

    const pointMap = new Map(probabilityMap.map((cell) => [cell.cellId, cell]));
    const highPoints = highZone.map((cell) => ({
      lat: cell.centroid.lat,
      lng: cell.centroid.lng
    }));

    const distances = verifiedReports.map((report) => {
      const reportPoint = pointMap.get(report.location.cellId) || {
        centroid: {
          lat: report.location.lat,
          lng: report.location.lng
        }
      };

      if (!highPoints.length) return 0;

      const nearest = highPoints.reduce((minDistance, point) => {
        const d = haversineKm(reportPoint.centroid.lat, reportPoint.centroid.lng, point.lat, point.lng);
        return Math.min(minDistance, d);
      }, Infinity);
      return nearest;
    });

    const brierPairs = verifiedReports.map((report) => {
      const p = pointMap.get(report.location.cellId)?.probability || 0;
      return (p - 1) ** 2;
    });

    const validation = {
      computedAt: new Date().toISOString(),
      horizonDays,
      sampleSize: verifiedReports.length,
      spatialOverlap: Number(overlapRatio.toFixed(4)),
      meanDistanceToPredictedKm: Number(mean(distances).toFixed(2)),
      seasonalTimingAccuracy: Number(clamp(overlapRatio * 0.7 + 0.2, 0, 1).toFixed(4)),
      probabilityCalibrationBrier: Number(mean(brierPairs).toFixed(4))
    };

    const nextState = await this.store.transact((draft) => {
      const draftRun = getOrCreateRun(draft, dateToken);
      draftRun.validation = validation;
      draft.outputs.validation = validation;
      draft.outputs.lastUpdated = new Date().toISOString();
      return draft;
    });

    return nextState.dailyRuns.find((item) => item.date === dateToken).validation;
  }

  buildSignalsStatus(run, communityLayer) {
    const features = run.featureVectors;
    const vegetation = mean(features.map((f) => f.forageAvailability ?? 0));
    const water = mean(features.map((f) => (f.surfaceWaterPresence ?? 0.5) - (f.distanceToReliableWater ?? 0.5) * 0.5));
    const terrainFeasibility = mean(features.map((f) => 1 - (f.terrainSlope ?? 0.5) * 0.7 - (f.floodingWaterlogging ?? 0.5) * 0.3));
    const precipitation = mean(features.map((f) => signedToUnit(f.rainfallAnomaly ?? 0)));
    const community = communityLayer.length
      ? mean(communityLayer.map((item) => item.trustWeightedScore))
      : 0;

    return {
      generatedAt: new Date().toISOString(),
      vegetation: {
        value: Number(clamp(vegetation).toFixed(3)),
        status: signalBand(clamp(vegetation))
      },
      waterAccessibility: {
        value: Number(clamp(water).toFixed(3)),
        status: signalBand(clamp(water))
      },
      terrainFeasibility: {
        value: Number(clamp(terrainFeasibility).toFixed(3)),
        status: signalBand(clamp(terrainFeasibility))
      },
      precipitationTrend: {
        value: Number(clamp(precipitation).toFixed(3)),
        status: signalBand(clamp(precipitation))
      },
      communityReports: {
        value: Number(clamp(community).toFixed(3)),
        status: signalBand(clamp(community)),
        reportCount: communityLayer.reduce((sum, item) => sum + item.reportCount, 0),
        verifiedCount: communityLayer.reduce((sum, item) => sum + item.verifiedCount, 0)
      },
      dataLatencyHours: run.ingestion?.sourceLatencyHours || null
    };
  }

  async buildDecisionSupportOutputs({ date = toDateString(), adminLevel = "county" } = {}) {
    const dateToken = toDateString(date);
    let state = await this.store.read();

    let run = state.dailyRuns.find((item) => item.date === dateToken);
    if (!run?.movement?.simulations?.[7]) {
      await this.runDailyPipeline({ date: dateToken, skipBootstrap: true });
      state = await this.store.read();
      run = state.dailyRuns.find((item) => item.date === dateToken);
    }

    const communityLayer = await this.buildGroundSignalLayer({ date: dateToken });
    const signalsStatus = this.buildSignalsStatus(run, communityLayer);

    const groups = new Map();
    for (const gamCell of run.gam.cells) {
      const key = gamCell.admin[adminLevel] || gamCell.admin.county;
      const bucket = groups.get(key) || [];
      bucket.push(gamCell);
      groups.set(key, bucket);
    }

    const adminSummaries = Array.from(groups.entries()).map(([name, cells]) => ({
      adminLevel,
      name,
      ...summarizeAdmin(cells),
      topDrivers: pickDriverSummary(cells[0]?.contributions || [])
    }));

    const corridorLayer = run.movement.simulations[7].dominantCorridors.map((corridor) => ({
      ...corridor,
      confidenceIndicator: corridor.confidence,
      dataLatencyHours: run.ingestion?.sourceLatencyHours?.nearRealTime ?? 24,
      explanation: corridor.driverSummary
    }));

    const outputs = {
      date: dateToken,
      generatedAt: new Date().toISOString(),
      grazingAttractivenessMap: run.gam.cells,
      floodAndAccessConstraints: run.movement.costSurface,
      probabilisticMovementCorridors: corridorLayer,
      adminSummaries,
      confidence: uncertaintyBand(mean(run.gam.cells.map((cell) => cell.uncertainty))),
      dataLatency: run.ingestion?.sourceLatencyHours || null,
      explanation: "Recent rainfall increase + forage gradients + flood/conflict avoidance"
    };

    await this.store.transact((draft) => {
      const draftRun = getOrCreateRun(draft, dateToken);
      draftRun.outputs = outputs;
      draft.outputs.signalsStatus = signalsStatus;
      draft.outputs.activeCorridors = corridorLayer;
      draft.outputs.adminSummaries = adminSummaries;
      draft.outputs.lastUpdated = new Date().toISOString();
      return draft;
    });

    return outputs;
  }

  async runDailyPipeline({ date = toDateString(), skipBootstrap = false } = {}) {
    const dateToken = toDateString(date);

    if (!skipBootstrap) {
      await this.ensureBootstrapped();
    }

    await this.ingestDaily({ date: dateToken });
    await this.runGAM({ date: dateToken });
    await this.runMovementForecast({ date: dateToken });
    await this.runValidation({ date: dateToken });
    const outputs = await this.buildDecisionSupportOutputs({ date: dateToken, adminLevel: "county" });
    const state = await this.store.read();
    const run = state.dailyRuns.find((item) => item.date === dateToken);
    const dataQuality = this.summarizeDataQuality(run);

    await this.store.transact((draft) => {
      const draftRun = getOrCreateRun(draft, dateToken);
      draftRun.dataQuality = dataQuality;
      draft.dataQualityHistory = [dataQuality, ...(draft.dataQualityHistory || [])]
        .sort((a, b) => b.date.localeCompare(a.date))
        .slice(0, 370);
      draft.outputs.dataQuality = dataQuality;
      return draft;
    });

    const changeDetection = await this.runForecastChangeDetection({ date: dateToken, horizonDays: 7 });
    const alerts = await this.runAlertEngine({ date: dateToken, horizonDays: 7 });

    return {
      date: dateToken,
      completedAt: new Date().toISOString(),
      signalsStatus: outputs ? "ready" : "missing",
      corridorCount: outputs.probabilisticMovementCorridors.length,
      adminSummaryCount: outputs.adminSummaries.length,
      dataQualityStatus: dataQuality.status,
      detectedChanges: changeDetection.summary,
      newAlerts: alerts.length
    };
  }

  async getSignalsStatus({ date } = {}) {
    const state = await this.store.read();
    if (state.outputs.signalsStatus && !date) return state.outputs.signalsStatus;

    const targetDate = date ? toDateString(date) : state.dailyRuns[0]?.date;
    if (!targetDate) return null;

    const run = state.dailyRuns.find((item) => item.date === targetDate);
    if (!run) return null;

    const communityLayer = await this.buildGroundSignalLayer({ date: targetDate });
    return this.buildSignalsStatus(run, communityLayer);
  }

  async getActiveCorridors({ horizonDays = 7, date } = {}) {
    const state = await this.store.read();
    const targetDate = date ? toDateString(date) : state.dailyRuns[0]?.date;
    if (!targetDate) return [];

    const run = state.dailyRuns.find((item) => item.date === targetDate);
    if (!run?.movement?.simulations?.[horizonDays]) return [];

    return run.movement.simulations[horizonDays].dominantCorridors;
  }

  async getCommunityReports({ verified } = {}) {
    const state = await this.store.read();
    if (verified === undefined) return state.communityReports;
    const expected = String(verified).toLowerCase() === "true";
    return state.communityReports.filter((report) => report.verified === expected);
  }

  async getReviewQueue({ limit = 100 } = {}) {
    const state = await this.store.read();
    return state.communityReports
      .filter((report) => (report.reviewState || "pending") === "pending")
      .sort((a, b) => {
        if ((b.trustScore || 0) !== (a.trustScore || 0)) return (b.trustScore || 0) - (a.trustScore || 0);
        return String(a.submittedAt).localeCompare(String(b.submittedAt));
      })
      .slice(0, Math.max(1, limit));
  }

  async reviewCommunityReport(reportId, { decision = "approve", reviewedBy = "analyst", notes = "" } = {}) {
    const approved = String(decision).toLowerCase() === "approve";
    return this.verifyCommunityReport(reportId, {
      approved,
      reviewedBy,
      reviewNotes: notes
    });
  }

  async runForecastChangeDetection({ date = toDateString(), horizonDays = 7 } = {}) {
    const dateToken = toDateString(date);
    let state = await this.store.read();
    let run = state.dailyRuns.find((item) => item.date === dateToken);

    if (!run?.movement?.simulations?.[horizonDays]) {
      await this.runMovementForecast({ date: dateToken, horizons: [horizonDays] });
      state = await this.store.read();
      run = state.dailyRuns.find((item) => item.date === dateToken);
    }

    const sortedRuns = [...state.dailyRuns].sort((a, b) => a.date.localeCompare(b.date));
    const index = sortedRuns.findIndex((item) => item.date === dateToken);
    const previousRun = index > 0 ? sortedRuns[index - 1] : null;
    const previousCorridors = previousRun?.movement?.simulations?.[horizonDays]?.dominantCorridors || [];
    const currentCorridors = run?.movement?.simulations?.[horizonDays]?.dominantCorridors || [];

    const diff = this.compareCorridors(currentCorridors, previousCorridors);
    const payload = {
      computedAt: new Date().toISOString(),
      date: dateToken,
      horizonDays,
      previousDate: previousRun?.date || null,
      ...diff
    };

    await this.store.transact((draft) => {
      const draftRun = getOrCreateRun(draft, dateToken);
      draftRun.changeDetection = payload;
      draft.outputs.latestChangeDetection = payload;
      draft.outputs.lastUpdated = new Date().toISOString();
      return draft;
    });

    return payload;
  }

  async runAlertEngine({ date = toDateString(), horizonDays = 7 } = {}) {
    const dateToken = toDateString(date);
    let state = await this.store.read();
    let run = state.dailyRuns.find((item) => item.date === dateToken);
    if (!run?.outputs || !run?.movement?.simulations?.[horizonDays]) {
      await this.runDailyPipeline({ date: dateToken });
      state = await this.store.read();
      run = state.dailyRuns.find((item) => item.date === dateToken);
    }

    const changeDetection = run.changeDetection || (await this.runForecastChangeDetection({ date: dateToken, horizonDays }));
    const featuresByCell = new Map(run.featureVectors.map((item) => [item.cellId, item]));
    const existingOpenFingerprints = new Set(
      (state.alerts || []).filter((alert) => alert.status === "open").map((alert) => alert.fingerprint)
    );

    const alerts = [];
    for (const corridor of run.movement.simulations[horizonDays].dominantCorridors) {
      if (corridor.classification !== "High") continue;
      const destinationFeature = featuresByCell.get(corridor.toCellId);
      if (!destinationFeature) continue;

      if ((destinationFeature.conflictAvoidancePrior ?? 0) >= 0.7) {
        alerts.push(
          this.createAlert({
            date: dateToken,
            type: "risk_zone_intersection",
            severity: "high",
            message: `High-likelihood corridor intersects elevated conflict avoidance prior at ${corridor.toCellId}.`,
            details: { ...corridor, reason: "conflict_prior" }
          })
        );
      }

      if ((destinationFeature.floodingWaterlogging ?? 0) >= 0.75) {
        alerts.push(
          this.createAlert({
            date: dateToken,
            type: "flood_access_risk",
            severity: "medium",
            message: `High-likelihood corridor may face flooding/access constraints near ${corridor.toCellId}.`,
            details: { ...corridor, reason: "flooding" }
          })
        );
      }
    }

    for (const change of changeDetection.changes || []) {
      if (change.type === "intensified" && (change.deltaProbability || 0) >= 0.01) {
        alerts.push(
          this.createAlert({
            date: dateToken,
            type: "corridor_spike",
            severity: "medium",
            message: `Corridor ${change.corridor.fromCellId} -> ${change.corridor.toCellId} intensified sharply.`,
            details: {
              ...change.corridor,
              deltaProbability: change.deltaProbability,
              reason: "intensified"
            }
          })
        );
      }
    }

    const dataQuality = run.dataQuality || this.summarizeDataQuality(run);
    if (dataQuality.status === "poor") {
      alerts.push(
        this.createAlert({
          date: dateToken,
          type: "data_quality_degraded",
          severity: "high",
          message: "Data quality degraded: high missing signal rate and/or excessive latency.",
          details: { reason: "data_quality", dataQuality }
        })
      );
    }

    const dedupedAlerts = alerts.filter((alert) => !existingOpenFingerprints.has(alert.fingerprint));

    await this.store.transact((draft) => {
      const draftRun = getOrCreateRun(draft, dateToken);
      draftRun.alerts = dedupedAlerts;
      draft.alerts = [...dedupedAlerts, ...(draft.alerts || [])].slice(0, 2000);
      draft.outputs.latestAlerts = dedupedAlerts;
      draft.outputs.lastUpdated = new Date().toISOString();
      return draft;
    });

    return dedupedAlerts;
  }

  async getAlerts({ status, severity, type, limit = 200 } = {}) {
    const state = await this.store.read();
    return (state.alerts || [])
      .filter((alert) => (status ? alert.status === status : true))
      .filter((alert) => (severity ? alert.severity === severity : true))
      .filter((alert) => (type ? alert.type === type : true))
      .slice(0, Math.max(1, limit));
  }

  async acknowledgeAlert(alertId, { actor = "analyst", note = "" } = {}) {
    const nextState = await this.store.transact((draft) => {
      const alert = (draft.alerts || []).find((item) => item.id === alertId);
      if (!alert) throw new Error("Alert not found.");
      alert.status = "acknowledged";
      alert.acknowledgedAt = new Date().toISOString();
      alert.acknowledgedBy = actor;
      alert.acknowledgeNote = note;
      return draft;
    });

    return nextState.alerts.find((item) => item.id === alertId);
  }

  async getDataQualityMonitor({ limit = 30 } = {}) {
    const state = await this.store.read();
    const history = (state.dataQualityHistory || []).slice(0, Math.max(1, limit));
    const fallbackDays = history.filter((item) => item.fallbackActive).length;
    return {
      generatedAt: new Date().toISOString(),
      latest: history[0] || null,
      history,
      summary: {
        sampleDays: history.length,
        fallbackDays,
        averageMissingSignalRate: Number(mean(history.map((item) => item.missingSignalRate || 0)).toFixed(4)),
        averageLatencyHours: Number(mean(history.map((item) => item.nearRealTimeLatencyHours || 0)).toFixed(2))
      }
    };
  }

  csvEscape(value) {
    const text = String(value ?? "");
    if (text.includes(",") || text.includes("\"") || text.includes("\n")) {
      return `"${text.replaceAll("\"", "\"\"")}"`;
    }
    return text;
  }

  async buildExportPackage({ date = toDateString(), formats = ["json", "csv", "geojson"] } = {}) {
    const dateToken = toDateString(date);
    const outputs = await this.buildDecisionSupportOutputs({ date: dateToken, adminLevel: "county" });
    const state = await this.store.read();
    const gridById = new Map((state.masterGrid?.cells || []).map((cell) => [cell.id, cell]));

    await fs.mkdir(this.config.dataExportsDir, { recursive: true });
    const stamp = new Date().toISOString().replaceAll(":", "").replaceAll(".", "");
    const prefix = `cattle-export-${dateToken}-${stamp}`;
    const generated = [];

    if (formats.includes("json")) {
      const filename = `${prefix}.json`;
      const filePath = path.resolve(this.config.dataExportsDir, filename);
      await fs.writeFile(filePath, JSON.stringify(outputs, null, 2), "utf8");
      generated.push({ format: "json", filename, filePath });
    }

    if (formats.includes("csv")) {
      const corridorCsvHeader = ["fromCellId", "toCellId", "classification", "probability", "confidenceIndicator", "explanation"];
      const corridorCsvRows = outputs.probabilisticMovementCorridors.map((corridor) => [
        corridor.fromCellId,
        corridor.toCellId,
        corridor.classification,
        corridor.probability,
        corridor.confidenceIndicator,
        corridor.explanation
      ]);
      const corridorCsv = [corridorCsvHeader, ...corridorCsvRows]
        .map((row) => row.map((item) => this.csvEscape(item)).join(","))
        .join("\n");

      const adminHeader = ["adminLevel", "name", "classification", "averageScore", "averageUncertainty", "topDrivers"];
      const adminRows = outputs.adminSummaries.map((item) => [
        item.adminLevel,
        item.name,
        item.classification,
        item.averageScore,
        item.averageUncertainty,
        item.topDrivers
      ]);
      const adminCsv = [adminHeader, ...adminRows]
        .map((row) => row.map((item) => this.csvEscape(item)).join(","))
        .join("\n");

      const corridorFilename = `${prefix}-corridors.csv`;
      const adminFilename = `${prefix}-admin.csv`;
      const corridorPath = path.resolve(this.config.dataExportsDir, corridorFilename);
      const adminPath = path.resolve(this.config.dataExportsDir, adminFilename);

      await fs.writeFile(corridorPath, corridorCsv, "utf8");
      await fs.writeFile(adminPath, adminCsv, "utf8");
      generated.push({ format: "csv", filename: corridorFilename, filePath: corridorPath });
      generated.push({ format: "csv", filename: adminFilename, filePath: adminPath });
    }

    if (formats.includes("geojson")) {
      const features = outputs.probabilisticMovementCorridors.map((corridor) => {
        const from = gridById.get(corridor.fromCellId)?.centroid;
        const to = gridById.get(corridor.toCellId)?.centroid;
        const geometry = from && to
          ? {
              type: "LineString",
              coordinates: [
                [from.lng, from.lat],
                [to.lng, to.lat]
              ]
            }
          : null;

        return {
          type: "Feature",
          properties: corridor,
          geometry
        };
      });

      const geojson = {
        type: "FeatureCollection",
        features
      };

      const filename = `${prefix}.geojson`;
      const filePath = path.resolve(this.config.dataExportsDir, filename);
      await fs.writeFile(filePath, JSON.stringify(geojson, null, 2), "utf8");
      generated.push({ format: "geojson", filename, filePath });
    }

    const record = {
      id: crypto.randomUUID(),
      date: dateToken,
      generatedAt: new Date().toISOString(),
      formats,
      files: generated.map((item) => ({
        ...item,
        relativePath: path.relative(process.cwd(), item.filePath)
      }))
    };

    await this.store.transact((draft) => {
      draft.exports = [record, ...(draft.exports || [])].slice(0, 300);
      return draft;
    });

    return record;
  }

  async runBacktest({
    startDate = shiftDate(toDateString(), -14),
    endDate = toDateString(),
    horizonDays = 7,
    maxDays = 60,
    actor = "system"
  } = {}) {
    const dates = listDates(toDateString(startDate), toDateString(endDate), maxDays);
    if (!dates.length) {
      throw new Error("Invalid backtest range.");
    }

    const runs = [];
    for (const dateToken of dates) {
      await this.runDailyPipeline({ date: dateToken });
      const validation = await this.runValidation({ date: dateToken, horizonDays });
      runs.push({
        date: dateToken,
        overlap: validation.spatialOverlap,
        meanDistanceKm: validation.meanDistanceToPredictedKm,
        brier: validation.probabilityCalibrationBrier,
        sampleSize: validation.sampleSize
      });
    }

    const summary = {
      days: runs.length,
      averageOverlap: Number(mean(runs.map((item) => item.overlap || 0)).toFixed(4)),
      averageDistanceKm: Number(mean(runs.map((item) => item.meanDistanceKm || 0)).toFixed(2)),
      averageBrier: Number(mean(runs.map((item) => item.brier || 0)).toFixed(4)),
      totalSamples: runs.reduce((sum, item) => sum + (item.sampleSize || 0), 0)
    };

    const payload = {
      id: crypto.randomUUID(),
      requestedAt: new Date().toISOString(),
      actor,
      startDate: dates[0],
      endDate: dates[dates.length - 1],
      horizonDays,
      summary,
      runs
    };

    await this.store.transact((draft) => {
      draft.backtests = [payload, ...(draft.backtests || [])].slice(0, 100);
      return draft;
    });

    return payload;
  }

  async getBacktests({ limit = 20 } = {}) {
    const state = await this.store.read();
    return (state.backtests || []).slice(0, Math.max(1, limit));
  }

  async getMapLayers({ date = toDateString(), horizonDays = 7 } = {}) {
    await this.ensureBootstrapped();
    const dateToken = toDateString(date);
    let state = await this.store.read();
    let run = state.dailyRuns.find((item) => item.date === dateToken);
    const gridCellIds = new Set((state.masterGrid?.cells || []).map((cell) => cell.id));
    let alignmentRatio = this.runGridAlignmentRatio(run, gridCellIds, horizonDays);
    const needsRefresh =
      !run?.movement?.simulations?.[horizonDays] ||
      alignmentRatio < 0.7;

    if (needsRefresh) {
      await this.runDailyPipeline({ date: dateToken });
      state = await this.store.read();
      run = state.dailyRuns.find((item) => item.date === dateToken);
      const refreshedGridCellIds = new Set((state.masterGrid?.cells || []).map((cell) => cell.id));
      alignmentRatio = this.runGridAlignmentRatio(run, refreshedGridCellIds, horizonDays);
    }
    if (!run) throw new Error(`No run found for ${dateToken}`);

    const outputs = await this.buildDecisionSupportOutputs({ date: dateToken });

    const probabilityMap = run.movement?.simulations?.[horizonDays]?.probabilityHeatmap || [];
    const probabilityByCell = new Map(probabilityMap.map((item) => [item.cellId, item]));
    const groundLayer = await this.buildGroundSignalLayer({ date: dateToken });
    const groundByCell = new Map(groundLayer.map((item) => [item.cellId, item]));
    const costByCell = new Map((outputs.floodAndAccessConstraints || []).map((item) => [item.cellId, item]));
    const operationalBounds = this.activeOperationalBounds(state.masterGrid);
    const gridCells = (state.masterGrid?.cells || []).filter((cell) => {
      const lat = Number(cell?.centroid?.lat);
      const lng = Number(cell?.centroid?.lng);
      return isOperationalCoordinate(lat, lng, operationalBounds);
    });
    const validCellIds = new Set(gridCells.map((cell) => cell.id));

    const cells = gridCells.map((cell) => {
      const attractiveness = outputs.grazingAttractivenessMap.find((item) => item.cellId === cell.id);
      const community = groundByCell.get(cell.id);
      const cost = costByCell.get(cell.id);
      const probabilityMeta = probabilityByCell.get(cell.id);
      return {
        cellId: cell.id,
        row: cell.row,
        col: cell.col,
        centroid: cell.centroid,
        admin: cell.admin,
        attractivenessScore: attractiveness?.score ?? null,
        uncertainty: attractiveness?.uncertainty ?? null,
        probability7d: probabilityMeta?.probability ?? 0,
        movementCost: cost?.cost ?? null,
        habitatScore: cost?.habitatScore ?? null,
        uninhabitable: Boolean(cost?.blocked),
        floodPenalty: cost?.penalties?.flooding ?? null,
        conflictPenalty: cost?.penalties?.conflict ?? null,
        terrainPenalty: cost?.penalties?.steepSlope ?? null,
        waterDistancePenalty: cost?.penalties?.distanceFromWater ?? null,
        openWaterPenalty: cost?.penalties?.openWater ?? null,
        openWaterNeighborhoodPenalty: cost?.penalties?.neighborhoodOpenWater ?? null,
        communityTrust: community?.trustWeightedScore ?? 0,
        communityReports: community?.reportCount ?? 0,
        estimatedCattle: probabilityMeta?.estimatedCattle ?? 0,
        estimatedHerds: probabilityMeta?.estimatedHerds ?? 0,
        particleTrackerProbability: probabilityMeta?.particleTrackerProbability ?? null,
        ecologicalTrackerProbability: probabilityMeta?.ecologicalTrackerProbability ?? null,
        communityTrackerProbability: probabilityMeta?.communityTrackerProbability ?? null,
        trackerAgreement: probabilityMeta?.trackerAgreement ?? null,
        projectionUncertainty: probabilityMeta?.projectionUncertainty ?? null,
        falseProjectionRisk: probabilityMeta?.falseProjectionRisk ?? null
      };
    });
    const corridors = (outputs.probabilisticMovementCorridors || []).filter((corridor) =>
      validCellIds.has(corridor?.fromCellId) &&
      validCellIds.has(corridor?.toCellId)
    );

    const providerRuntime = await this.googleEarthService.resolveRuntimeMode();
    const externalSources = this.googleEarthService.getMapSourceCatalog();

    return {
      date: dateToken,
      generatedAt: new Date().toISOString(),
      sources: {
        activeProvider: providerRuntime.activeProvider,
        useFallback: providerRuntime.useFallback,
        providers: providerRuntime.providers,
        externalSources
      },
      layers: {
        cells,
        corridors,
        adminSummaries: outputs.adminSummaries
      },
      metrics: {
        estimatedCattleTotal: Number(run.movement?.simulations?.[horizonDays]?.estimatedCattleTotal || 0),
        blockedCellCount: Number(run.movement?.simulations?.[horizonDays]?.blockedCellCount || 0),
        averageTrackerAgreement: Number(run.movement?.simulations?.[horizonDays]?.averageTrackerAgreement || 0),
        averageProjectionUncertainty: Number(run.movement?.simulations?.[horizonDays]?.averageProjectionUncertainty || 0),
        falseProjectionHotspots: Number(run.movement?.simulations?.[horizonDays]?.falseProjectionHotspots || 0),
        validationReliability: Number(run.movement?.simulations?.[horizonDays]?.validationReliability || 0),
        calibrationFactor: Number(run.movement?.simulations?.[horizonDays]?.calibrationFactor || 0),
        expectedAccuracy: Number(run.movement?.simulations?.[horizonDays]?.expectedAccuracy || 0),
        fusionWeights: run.movement?.simulations?.[horizonDays]?.fusionWeights || null,
        trackerModels: run.movement?.simulations?.[horizonDays]?.trackerModels || [],
        filteredOutCellCount: Math.max(0, Number(state.masterGrid?.cells?.length || 0) - cells.length),
        gridAlignmentRatio: Number(alignmentRatio.toFixed(4))
      }
    };
  }

  async recordAudit({
    actor = "anonymous",
    role = "anonymous",
    action = "unknown",
    method = "GET",
    path: requestPath = "",
    metadata = {}
  } = {}) {
    const event = {
      id: crypto.randomUUID(),
      at: new Date().toISOString(),
      actor,
      role,
      action,
      method,
      path: requestPath,
      metadata
    };

    await this.store.transact((draft) => {
      draft.auditTrail = [event, ...(draft.auditTrail || [])].slice(0, 3000);
      return draft;
    });

    return event;
  }

  async getAuditTrail({ limit = 200 } = {}) {
    const state = await this.store.read();
    return (state.auditTrail || []).slice(0, Math.max(1, limit));
  }

  async updateModelWeights({ weights, reason = "Manual feedback adjustment" } = {}) {
    const nextState = await this.store.transact((draft) => {
      const updatedWeights = normalizeWeights(weights, draft.model.weights);
      const changed = Object.keys(updatedWeights).some((key) => updatedWeights[key] !== draft.model.weights[key]);

      if (!changed) {
        return draft;
      }

      draft.model.version += 1;
      draft.model.weights = updatedWeights;
      draft.model.history.unshift({
        version: draft.model.version,
        changedAt: new Date().toISOString(),
        reason,
        weights: updatedWeights
      });
      draft.model.history = draft.model.history.slice(0, 30);
      return draft;
    });

    return nextState.model;
  }

  async getStateSnapshot() {
    return this.store.read();
  }
}
