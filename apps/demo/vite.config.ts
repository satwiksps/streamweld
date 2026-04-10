import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { sites } from "@openai/sites-vite-plugin";
import { cloudflare } from "@cloudflare/vite-plugin";
import hostingConfig from "./.openai/hosting.json" with { type: "json" };

const localD1DatabaseID = "00000000-0000-4000-8000-000000000000";

export default defineConfig({
  server: {
    watch: { ignored: ["**/public/og.png"] },
  },
  plugins: [
    react(),
    tailwindcss(),
    sites(),
    cloudflare({
      config: {
        name: "server",
        main: "worker/index.ts",
        compatibility_date: "2026-05-22",
        d1_databases: hostingConfig.d1 === null
          ? []
          : [{
              binding: hostingConfig.d1,
              database_name: "streamweld-demo",
              database_id: localD1DatabaseID,
            }],
        assets: {
          not_found_handling: "single-page-application",
          run_worker_first: ["/api/*", "/v1/*"],
        },
      },
    }),
  ],
});
