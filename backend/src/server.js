import path from "node:path";
import fs from "node:fs/promises";
import { fileURLToPath } from "node:url";
import cors from "cors";
import express from "express";

import { config } from "./config.js";
import { createApiRouter } from "./routes.js";
import { GoogleEarthService } from "./services/googleEarthService.js";
import { PipelineService } from "./services/pipelineService.js";
import { StateStore } from "./state/store.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const workspaceRoot = path.resolve(__dirname, "../..");

const app = express();
app.use(cors());
app.use(express.json({ limit: "1mb" }));

const store = new StateStore(config.dataFilePath);
const googleEarthService = new GoogleEarthService(config);
const pipelineService = new PipelineService(store, googleEarthService, config);
const apiRouter = createApiRouter({ pipelineService, googleEarthService, config });

app.use("/api", apiRouter);
app.use(apiRouter);

app.get("/prototype", (_req, res) => {
  res.sendFile(path.resolve(workspaceRoot, "cattle-forecasting-prototype (1).html"));
});

app.use(express.static(workspaceRoot));

app.use((error, _req, res, _next) => {
  const statusCode = error.message?.includes("not found") ? 404 : 400;
  res.status(statusCode).json({
    error: error.message || "Unexpected error",
    timestamp: new Date().toISOString()
  });
});

async function start() {
  await fs.mkdir(config.dataExportsDir, { recursive: true });

  if (config.autoBootstrapOnStart) {
    await pipelineService.ensureBootstrapped();
    await pipelineService.runDailyPipeline({ date: new Date() });
  }

  const intervalMs = Math.max(1, config.pipelineIntervalHours) * 60 * 60 * 1000;
  setInterval(async () => {
    try {
      await pipelineService.runDailyPipeline({ date: new Date() });
      // eslint-disable-next-line no-console
      console.log(`[pipeline] completed @ ${new Date().toISOString()}`);
    } catch (error) {
      // eslint-disable-next-line no-console
      console.error(`[pipeline] failed: ${error.message}`);
    }
  }, intervalMs);

  app.listen(config.port, () => {
    // eslint-disable-next-line no-console
    console.log(`Backend listening on http://localhost:${config.port}`);
    // eslint-disable-next-line no-console
    console.log(`Google Earth key configured: ${Boolean(config.googleEarthApiKey)}`);
  });
}

start().catch((error) => {
  // eslint-disable-next-line no-console
  console.error(`Failed to start server: ${error.message}`);
  process.exit(1);
});
