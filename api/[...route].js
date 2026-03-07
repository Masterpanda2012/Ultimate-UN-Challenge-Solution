import fs from "node:fs/promises";
import cors from "cors";
import express from "express";

import { config } from "../backend/src/config.js";
import { createApiRouter } from "../backend/src/routes.js";
import { GoogleEarthService } from "../backend/src/services/googleEarthService.js";
import { PipelineService } from "../backend/src/services/pipelineService.js";
import { StateStore } from "../backend/src/state/store.js";

let appPromise = null;

async function buildApp() {
  const app = express();
  app.use(cors());
  app.use(express.json({ limit: "1mb" }));

  const store = new StateStore(config.dataFilePath);
  const googleEarthService = new GoogleEarthService(config);
  const pipelineService = new PipelineService(store, googleEarthService, config);
  const apiRouter = createApiRouter({ pipelineService, googleEarthService, config });

  // Support both /api/* and bare routes in case upstream rewrites strip /api.
  app.use("/api", apiRouter);
  app.use(apiRouter);

  app.use((error, _req, res, _next) => {
    const statusCode = error.message?.includes("not found") ? 404 : 400;
    res.status(statusCode).json({
      error: error.message || "Unexpected error",
      timestamp: new Date().toISOString()
    });
  });

  await fs.mkdir(config.dataExportsDir, { recursive: true });
  if (config.autoBootstrapOnStart) {
    await pipelineService.ensureBootstrapped();
    await pipelineService.runDailyPipeline({ date: new Date() });
  }

  return app;
}

export default async function handler(req, res) {
  if (!appPromise) {
    appPromise = buildApp().catch((error) => {
      appPromise = null;
      throw error;
    });
  }
  const app = await appPromise;
  return app(req, res);
}
