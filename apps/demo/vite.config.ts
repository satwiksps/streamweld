import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { sites } from "@openai/sites-vite-plugin";
import { cloudflare } from "@cloudflare/vite-plugin";

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
        assets: {
          not_found_handling: "single-page-application",
          run_worker_first: ["/api/*", "/v1/*"],
        },
      },
    }),
  ],
});
