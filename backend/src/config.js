import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";

dotenv.config();

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function toNumber(value, fallback) {
  const num = Number(value);
  return Number.isFinite(num) ? num : fallback;
}

function toBool(value, fallback = false) {
  if (value == null) return fallback;
  const normalized = String(value).trim().toLowerCase();
  if (["1", "true", "yes", "on"].includes(normalized)) return true;
  if (["0", "false", "no", "off"].includes(normalized)) return false;
  return fallback;
}

export const config = {
  port: toNumber(process.env.PORT, 8080),
  nodeEnv: process.env.NODE_ENV || "development",
  dataFilePath: process.env.DATA_FILE_PATH || path.resolve(__dirname, "../data/state.json"),
  dataExportsDir: process.env.DATA_EXPORTS_DIR || path.resolve(__dirname, "../data/exports"),
  googleEarthApiKey: process.env.GOOGLE_EARTH_API_KEY || "",
  earthEngineProject: process.env.EARTH_ENGINE_PROJECT || "earthengine-public",
  enableLiveEarthCalls: toBool(process.env.ENABLE_LIVE_EARTH_CALLS, false),
  adminApiToken: process.env.ADMIN_API_TOKEN || "",
  analystApiToken: process.env.ANALYST_API_TOKEN || "",
  pipelineIntervalHours: toNumber(process.env.PIPELINE_INTERVAL_HOURS, 24),
  defaultGridResolutionKm: toNumber(process.env.DEFAULT_GRID_RESOLUTION_KM, 5),
  defaultCountryCode: process.env.DEFAULT_COUNTRY_CODE || "UN-DEMO",
  autoBootstrapOnStart: toBool(process.env.AUTO_BOOTSTRAP_ON_START, true)
};

export function maskApiKey(apiKey) {
  if (!apiKey) return "not-set";
  if (apiKey.length <= 8) return "****";
  return `${apiKey.slice(0, 4)}...${apiKey.slice(-4)}`;
}
