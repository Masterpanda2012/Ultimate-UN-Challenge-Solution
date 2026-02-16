import fs from "node:fs/promises";
import path from "node:path";

const DEFAULT_WEIGHTS = {
  forageAvailability: 0.12,
  forageImprovementRate: 0.08,
  recentRainfall: 0.09,
  rainfallAnomaly: 0.08,
  soilMoisture: 0.08,
  surfaceWaterPresence: 0.12,
  floodingPenalty: 0.09,
  terrainSlopePenalty: 0.07,
  distanceToWaterPenalty: 0.08,
  landCoverSuitability: 0.05,
  settlementProximityModifier: 0.03,
  historicalGrazingPressure: 0.03,
  seasonalTimingIndex: 0.03,
  vegetationTypeComposition: 0.03,
  conflictAvoidancePenalty: 0.10
};

function initialState() {
  return {
    schemaVersion: 2,
    createdAt: new Date().toISOString(),
    dataSources: {
      nrtDrivers: [],
      correctionLayers: [],
      baseLayers: [],
      registrationLog: [],
      status: "not_registered"
    },
    masterGrid: null,
    dailyRuns: [],
    communityReports: [],
    reporterTrust: {},
    alerts: [],
    backtests: [],
    exports: [],
    dataQualityHistory: [],
    auditTrail: [],
    model: {
      version: 1,
      weights: DEFAULT_WEIGHTS,
      history: [
        {
          version: 1,
          changedAt: new Date().toISOString(),
          reason: "Initial model weights",
          weights: DEFAULT_WEIGHTS
        }
      ]
    },
    outputs: {
      signalsStatus: null,
      activeCorridors: [],
      latestSimulation: null,
      validation: null,
      dataQuality: null,
      latestChangeDetection: null,
      latestAlerts: [],
      adminSummaries: [],
      lastUpdated: null
    }
  };
}

function normalizeState(state) {
  const normalized = { ...state };

  normalized.schemaVersion = 2;
  normalized.dataSources = normalized.dataSources || {
    nrtDrivers: [],
    correctionLayers: [],
    baseLayers: [],
    registrationLog: [],
    status: "not_registered"
  };
  normalized.dailyRuns = Array.isArray(normalized.dailyRuns) ? normalized.dailyRuns : [];
  normalized.communityReports = Array.isArray(normalized.communityReports) ? normalized.communityReports : [];
  normalized.reporterTrust = normalized.reporterTrust || {};
  normalized.alerts = Array.isArray(normalized.alerts) ? normalized.alerts : [];
  normalized.backtests = Array.isArray(normalized.backtests) ? normalized.backtests : [];
  normalized.exports = Array.isArray(normalized.exports) ? normalized.exports : [];
  normalized.dataQualityHistory = Array.isArray(normalized.dataQualityHistory) ? normalized.dataQualityHistory : [];
  normalized.auditTrail = Array.isArray(normalized.auditTrail) ? normalized.auditTrail : [];
  normalized.model = normalized.model || {
    version: 1,
    weights: DEFAULT_WEIGHTS,
    history: []
  };
  normalized.outputs = normalized.outputs || {};
  normalized.outputs.signalsStatus = normalized.outputs.signalsStatus || null;
  normalized.outputs.activeCorridors = Array.isArray(normalized.outputs.activeCorridors)
    ? normalized.outputs.activeCorridors
    : [];
  normalized.outputs.latestSimulation = normalized.outputs.latestSimulation || null;
  normalized.outputs.validation = normalized.outputs.validation || null;
  normalized.outputs.dataQuality = normalized.outputs.dataQuality || null;
  normalized.outputs.latestChangeDetection = normalized.outputs.latestChangeDetection || null;
  normalized.outputs.latestAlerts = Array.isArray(normalized.outputs.latestAlerts)
    ? normalized.outputs.latestAlerts
    : [];
  normalized.outputs.adminSummaries = Array.isArray(normalized.outputs.adminSummaries)
    ? normalized.outputs.adminSummaries
    : [];
  normalized.outputs.lastUpdated = normalized.outputs.lastUpdated || null;

  normalized.dailyRuns = normalized.dailyRuns.map((run) => ({
    ...run,
    featureVectors: Array.isArray(run.featureVectors) ? run.featureVectors : [],
    changeDetection: run.changeDetection || null,
    alerts: Array.isArray(run.alerts) ? run.alerts : [],
    dataQuality: run.dataQuality || null
  }));

  normalized.communityReports = normalized.communityReports.map((report) => ({
    reviewState: report.reviewState || (report.verified ? "approved" : "pending"),
    reviewNotes: report.reviewNotes || "",
    ...report
  }));

  return normalized;
}

export class StateStore {
  #dataFilePath;
  #cache;
  #writeQueue;

  constructor(dataFilePath) {
    this.#dataFilePath = dataFilePath;
    this.#cache = null;
    this.#writeQueue = Promise.resolve();
  }

  async #ensureFile() {
    const dirPath = path.dirname(this.#dataFilePath);
    await fs.mkdir(dirPath, { recursive: true });
    try {
      await fs.access(this.#dataFilePath);
    } catch {
      const startingState = initialState();
      await fs.writeFile(this.#dataFilePath, JSON.stringify(startingState, null, 2), "utf8");
    }
  }

  async read() {
    if (this.#cache) {
      return structuredClone(this.#cache);
    }

    await this.#ensureFile();
    const raw = await fs.readFile(this.#dataFilePath, "utf8");
    this.#cache = normalizeState(JSON.parse(raw));
    return structuredClone(this.#cache);
  }

  async write(nextState) {
    this.#writeQueue = this.#writeQueue.then(async () => {
      this.#cache = normalizeState(structuredClone(nextState));
      await fs.writeFile(this.#dataFilePath, JSON.stringify(this.#cache, null, 2), "utf8");
    });
    await this.#writeQueue;
    return structuredClone(this.#cache);
  }

  async transact(mutator) {
    const current = await this.read();
    const draft = structuredClone(current);
    const changed = (await mutator(draft)) ?? draft;
    return this.write(changed);
  }
}

export { initialState, DEFAULT_WEIGHTS };
