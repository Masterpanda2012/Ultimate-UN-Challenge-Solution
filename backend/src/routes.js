import path from "node:path";
import express from "express";

function parseBoolean(value) {
  if (value === undefined) return undefined;
  return ["1", "true", "yes", "on"].includes(String(value).toLowerCase());
}

function toNumber(value, fallback) {
  const n = Number(value);
  return Number.isFinite(n) ? n : fallback;
}

function parseCsv(value) {
  if (!value) return [];
  return String(value)
    .split(",")
    .map((part) => part.trim())
    .filter(Boolean);
}

function resolveAuth(req, config) {
  const adminToken = config.adminApiToken || "";
  const analystToken = config.analystApiToken || "";
  const authHeader = req.headers.authorization || "";
  const bearer = authHeader.startsWith("Bearer ") ? authHeader.slice(7).trim() : "";
  const token = String(req.headers["x-api-token"] || bearer || "").trim();
  const actor = String(req.headers["x-actor-id"] || "").trim() || "anonymous";

  // Open mode when tokens are not configured.
  if (!adminToken && !analystToken) {
    return {
      role: "admin",
      actor,
      isOpenMode: true,
      tokenProvided: Boolean(token)
    };
  }

  if (adminToken && token === adminToken) {
    return {
      role: "admin",
      actor,
      isOpenMode: false,
      tokenProvided: true
    };
  }

  if (analystToken && token === analystToken) {
    return {
      role: "analyst",
      actor,
      isOpenMode: false,
      tokenProvided: true
    };
  }

  return {
    role: "anonymous",
    actor,
    isOpenMode: false,
    tokenProvided: Boolean(token)
  };
}

function requireRole(roles, config) {
  const allowed = Array.isArray(roles) ? roles : [roles];
  return (req, res, next) => {
    const auth = resolveAuth(req, config);
    req.auth = auth;

    if (allowed.includes(auth.role)) {
      next();
      return;
    }

    res.status(401).json({
      error: "Unauthorized for this endpoint.",
      requiredRoles: allowed,
      role: auth.role
    });
  };
}

export function createApiRouter({ pipelineService, googleEarthService, config }) {
  const router = express.Router();

  async function audit(req, action, metadata = {}) {
    try {
      const auth = req.auth || resolveAuth(req, config);
      await pipelineService.recordAudit({
        actor: auth.actor,
        role: auth.role,
        action,
        method: req.method,
        path: req.path,
        metadata
      });
    } catch {
      // no-op on audit failures
    }
  }

  router.use((req, _res, next) => {
    req.auth = resolveAuth(req, config);
    next();
  });

  router.get("/auth/me", (req, res) => {
    res.json(req.auth);
  });

  router.get("/health", async (_req, res) => {
    const state = await pipelineService.getStateSnapshot();
    const providerRuntime = await googleEarthService.resolveRuntimeMode();
    const earthStatus = providerRuntime.connectivity;
    res.json({
      status: "ok",
      timestamp: new Date().toISOString(),
      dataSourcesRegistered: state.dataSources.status === "registered",
      masterGridReady: Boolean(state.masterGrid),
      latestRunDate: state.dailyRuns[0]?.date || null,
      earthEngine: {
        status: earthStatus.status,
        runtimeMode: earthStatus.runtimeMode,
        isLive: earthStatus.isLive,
        fallbackActive: earthStatus.fallbackActive,
        liveEarthCallsEnabled: earthStatus.liveEarthCallsEnabled,
        message: earthStatus.message || null,
        checkedAt: earthStatus.checkedAt
      },
      providers: {
        activeProvider: providerRuntime.activeProvider,
        useFallback: providerRuntime.useFallback,
        liveProviderCount: providerRuntime.liveProviderCount,
        providerCount: providerRuntime.providerCount
      }
    });
  });

  router.get("/providers/google-earth/status", async (req, res) => {
    const force = parseBoolean(req.query.force) ?? false;
    const status = await googleEarthService.checkConnectivity({ force });
    res.json(status);
  });

  router.get("/providers/status", async (req, res) => {
    const force = parseBoolean(req.query.force) ?? false;
    const runtime = await googleEarthService.resolveRuntimeMode({ force });
    res.json({
      checkedAt: new Date().toISOString(),
      activeProvider: runtime.activeProvider,
      useFallback: runtime.useFallback,
      liveProviderCount: runtime.liveProviderCount,
      providerCount: runtime.providerCount,
      providers: runtime.providers,
      mapSourceCatalog: googleEarthService.getMapSourceCatalog()
    });
  });

  // STEP 1 - Data source registration
  router.post("/setup/data-sources/register", requireRole(["admin", "analyst"], config), async (req, res, next) => {
    try {
      const payload = await pipelineService.registerDataSources({
        force: parseBoolean(req.body?.force)
      });
      await audit(req, "setup.data_sources.register", { force: Boolean(req.body?.force) });
      res.json(payload);
    } catch (error) {
      next(error);
    }
  });

  router.get("/setup/data-sources", async (_req, res) => {
    const state = await pipelineService.getStateSnapshot();
    res.json(state.dataSources);
  });

  // STEP 2 - Master grid definition
  router.post("/setup/master-grid", requireRole(["admin", "analyst"], config), async (req, res, next) => {
    try {
      const grid = await pipelineService.createMasterGrid({
        countryCode: req.body?.countryCode,
        resolutionKm: toNumber(req.body?.resolutionKm, undefined),
        maxCells: toNumber(req.body?.maxCells, undefined),
        bounds: req.body?.bounds
      });
      await audit(req, "setup.master_grid.create", {
        countryCode: req.body?.countryCode,
        resolutionKm: req.body?.resolutionKm
      });
      res.json(grid);
    } catch (error) {
      next(error);
    }
  });

  router.get("/setup/master-grid", async (_req, res) => {
    const state = await pipelineService.getStateSnapshot();
    res.json(state.masterGrid);
  });

  // STEP 3/4 - Ingestion and feature vector assembly
  router.post("/ingestion/daily", requireRole(["admin", "analyst"], config), async (req, res, next) => {
    try {
      const run = await pipelineService.ingestDaily({ date: req.body?.date });
      await audit(req, "ingestion.daily", { date: req.body?.date });
      res.json(run.ingestion);
    } catch (error) {
      next(error);
    }
  });

  router.get("/features/daily", async (req, res, next) => {
    try {
      const state = await pipelineService.getStateSnapshot();
      const date = req.query.date || state.dailyRuns[0]?.date;
      if (!date) {
        res.status(404).json({ error: "No daily runs available." });
        return;
      }
      const run = state.dailyRuns.find((item) => item.date === date);
      if (!run) {
        res.status(404).json({ error: `No run found for ${date}` });
        return;
      }
      res.json({ date, count: run.featureVectors.length, featureVectors: run.featureVectors });
    } catch (error) {
      next(error);
    }
  });

  // STEP 5 - GAM
  router.post("/gam/run", requireRole(["admin", "analyst"], config), async (req, res, next) => {
    try {
      const gam = await pipelineService.runGAM({ date: req.body?.date });
      await audit(req, "model.gam.run", { date: req.body?.date });
      res.json(gam);
    } catch (error) {
      next(error);
    }
  });

  // STEP 6 - Movement and corridors
  router.post("/forecast/run", requireRole(["admin", "analyst"], config), async (req, res, next) => {
    try {
      const movement = await pipelineService.runMovementForecast({
        date: req.body?.date,
        horizons: Array.isArray(req.body?.horizons) ? req.body.horizons : [7, 14],
        particleCount: toNumber(req.body?.particleCount, 920)
      });
      await audit(req, "forecast.run", { date: req.body?.date, horizons: req.body?.horizons || [7, 14] });
      res.json(movement);
    } catch (error) {
      next(error);
    }
  });

  // STEP 7 - Community reporting and trust
  router.post("/community/reports", async (req, res, next) => {
    try {
      const report = await pipelineService.addCommunityReport(req.body || {});
      await audit(req, "community.report.submit", { reportId: report.id, reporterId: report.reporterId });
      res.status(201).json(report);
    } catch (error) {
      next(error);
    }
  });

  router.get("/community/reports", async (req, res, next) => {
    try {
      const reports = await pipelineService.getCommunityReports({
        verified: req.query.verified
      });
      res.json({ count: reports.length, reports });
    } catch (error) {
      next(error);
    }
  });

  router.get("/community/review-queue", requireRole(["admin", "analyst"], config), async (req, res, next) => {
    try {
      const queue = await pipelineService.getReviewQueue({
        limit: toNumber(req.query.limit, 100)
      });
      res.json({ count: queue.length, queue });
    } catch (error) {
      next(error);
    }
  });

  router.post("/community/reviews/:id", requireRole(["admin", "analyst"], config), async (req, res, next) => {
    try {
      const reviewed = await pipelineService.reviewCommunityReport(req.params.id, {
        decision: req.body?.decision || "approve",
        reviewedBy: req.auth.actor,
        notes: req.body?.notes || ""
      });
      await audit(req, "community.review", {
        reportId: req.params.id,
        decision: req.body?.decision || "approve"
      });
      res.json(reviewed);
    } catch (error) {
      next(error);
    }
  });

  router.post("/community/reports/:id/verify", requireRole(["admin", "analyst"], config), async (req, res, next) => {
    try {
      const report = await pipelineService.verifyCommunityReport(req.params.id, {
        approved: parseBoolean(req.body?.approved) ?? true,
        reviewedBy: req.body?.reviewedBy || req.auth.actor,
        reviewNotes: req.body?.reviewNotes || ""
      });
      await audit(req, "community.report.verify", { reportId: req.params.id, approved: parseBoolean(req.body?.approved) ?? true });
      res.json(report);
    } catch (error) {
      next(error);
    }
  });

  // STEP 8 - Validation
  router.post("/validation/run", requireRole(["admin", "analyst"], config), async (req, res, next) => {
    try {
      const validation = await pipelineService.runValidation({
        date: req.body?.date,
        horizonDays: toNumber(req.body?.horizonDays, 7)
      });
      await audit(req, "validation.run", { date: req.body?.date, horizonDays: toNumber(req.body?.horizonDays, 7) });
      res.json(validation);
    } catch (error) {
      next(error);
    }
  });

  router.get("/validation/latest", async (_req, res) => {
    const state = await pipelineService.getStateSnapshot();
    res.json(state.outputs.validation || null);
  });

  // STEP 9 - UN-facing decision support outputs
  router.get("/signals/status", async (req, res, next) => {
    try {
      const payload = await pipelineService.getSignalsStatus({ date: req.query.date });
      if (!payload) {
        res.status(404).json({ error: "No signal status is available yet." });
        return;
      }
      res.json(payload);
    } catch (error) {
      next(error);
    }
  });

  router.get("/corridors/active", async (req, res, next) => {
    try {
      const corridors = await pipelineService.getActiveCorridors({
        date: req.query.date,
        horizonDays: toNumber(req.query.horizonDays, 7)
      });
      res.json({ count: corridors.length, corridors });
    } catch (error) {
      next(error);
    }
  });

  router.get("/outputs/daily", async (req, res, next) => {
    try {
      const output = await pipelineService.buildDecisionSupportOutputs({
        date: req.query.date,
        adminLevel: req.query.adminLevel || "county"
      });
      res.json(output);
    } catch (error) {
      next(error);
    }
  });

  router.get("/map/layers", async (req, res, next) => {
    try {
      const payload = await pipelineService.getMapLayers({
        date: req.query.date,
        horizonDays: toNumber(req.query.horizonDays, 7)
      });
      res.json(payload);
    } catch (error) {
      next(error);
    }
  });

  // STEP 10 - Feedback loop
  router.patch("/model/weights", requireRole(["admin", "analyst"], config), async (req, res, next) => {
    try {
      const model = await pipelineService.updateModelWeights({
        weights: req.body?.weights || {},
        reason: req.body?.reason
      });
      await audit(req, "model.weights.update", { reason: req.body?.reason || "manual" });
      res.json(model);
    } catch (error) {
      next(error);
    }
  });

  // Operational add-ons
  router.get("/monitor/data-quality", async (req, res, next) => {
    try {
      const monitor = await pipelineService.getDataQualityMonitor({
        limit: toNumber(req.query.limit, 30)
      });
      res.json(monitor);
    } catch (error) {
      next(error);
    }
  });

  router.get("/forecast/changes", async (req, res, next) => {
    try {
      const changes = await pipelineService.runForecastChangeDetection({
        date: req.query.date,
        horizonDays: toNumber(req.query.horizonDays, 7)
      });
      res.json(changes);
    } catch (error) {
      next(error);
    }
  });

  router.get("/alerts", async (req, res, next) => {
    try {
      const alerts = await pipelineService.getAlerts({
        status: req.query.status,
        severity: req.query.severity,
        type: req.query.type,
        limit: toNumber(req.query.limit, 200)
      });
      res.json({ count: alerts.length, alerts });
    } catch (error) {
      next(error);
    }
  });

  router.post("/alerts/:id/ack", requireRole(["admin", "analyst"], config), async (req, res, next) => {
    try {
      const alert = await pipelineService.acknowledgeAlert(req.params.id, {
        actor: req.auth.actor,
        note: req.body?.note || ""
      });
      await audit(req, "alert.acknowledge", { alertId: req.params.id });
      res.json(alert);
    } catch (error) {
      next(error);
    }
  });

  router.post("/backtest/run", requireRole(["admin", "analyst"], config), async (req, res, next) => {
    try {
      const result = await pipelineService.runBacktest({
        startDate: req.body?.startDate,
        endDate: req.body?.endDate,
        horizonDays: toNumber(req.body?.horizonDays, 7),
        maxDays: toNumber(req.body?.maxDays, 60),
        actor: req.auth.actor
      });
      await audit(req, "backtest.run", {
        startDate: req.body?.startDate,
        endDate: req.body?.endDate,
        horizonDays: toNumber(req.body?.horizonDays, 7)
      });
      res.json(result);
    } catch (error) {
      next(error);
    }
  });

  router.get("/backtests", requireRole(["admin", "analyst"], config), async (req, res, next) => {
    try {
      const items = await pipelineService.getBacktests({ limit: toNumber(req.query.limit, 20) });
      res.json({ count: items.length, backtests: items });
    } catch (error) {
      next(error);
    }
  });

  router.post("/exports/daily", requireRole(["admin", "analyst"], config), async (req, res, next) => {
    try {
      const formats = Array.isArray(req.body?.formats)
        ? req.body.formats
        : parseCsv(req.body?.formats || "json,csv,geojson");

      const exportRecord = await pipelineService.buildExportPackage({
        date: req.body?.date,
        formats: formats.length ? formats : ["json", "csv", "geojson"]
      });

      const files = exportRecord.files.map((file) => ({
        ...file,
        downloadUrl: `/exports/files/${encodeURIComponent(file.filename)}`
      }));

      await audit(req, "export.daily", { date: req.body?.date, formats: exportRecord.formats });
      res.json({ ...exportRecord, files });
    } catch (error) {
      next(error);
    }
  });

  router.get("/exports/history", requireRole(["admin", "analyst"], config), async (_req, res, next) => {
    try {
      const state = await pipelineService.getStateSnapshot();
      res.json({ count: (state.exports || []).length, exports: state.exports || [] });
    } catch (error) {
      next(error);
    }
  });

  router.get("/exports/files/:filename", requireRole(["admin", "analyst"], config), async (req, res, next) => {
    try {
      const filename = path.basename(String(req.params.filename));
      const filePath = path.resolve(config.dataExportsDir, filename);
      if (!filePath.startsWith(path.resolve(config.dataExportsDir))) {
        res.status(400).json({ error: "Invalid export filename." });
        return;
      }
      res.download(filePath);
    } catch (error) {
      next(error);
    }
  });

  router.get("/audit/trail", requireRole(["admin"], config), async (req, res, next) => {
    try {
      const events = await pipelineService.getAuditTrail({ limit: toNumber(req.query.limit, 200) });
      res.json({ count: events.length, events });
    } catch (error) {
      next(error);
    }
  });

  // Full orchestration endpoint
  router.post("/pipeline/run", requireRole(["admin", "analyst"], config), async (req, res, next) => {
    try {
      const result = await pipelineService.runDailyPipeline({ date: req.body?.date });
      await audit(req, "pipeline.run", { date: req.body?.date });
      res.json(result);
    } catch (error) {
      next(error);
    }
  });

  router.get("/state", async (_req, res) => {
    const state = await pipelineService.getStateSnapshot();
    res.json(state);
  });

  return router;
}
