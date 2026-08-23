import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";
import { sites } from "@openai/sites-vite-plugin";
import { cloudflare } from "@cloudflare/vite-plugin";
export default defineConfig(({ mode }) => ({
  server: {
    watch: { ignored: ["**/public/og.png"] },
  },
  plugins: mode === "vercel"
    ? [react(), tailwindcss()]
    : [
        react(),
        tailwindcss(),
        sites(),
        cloudflare({
          config: {
            name: "streamweld-website",
            main: "worker/index.ts",
            compatibility_date: "2026-05-22",
            assets: {
              binding: "ASSETS",
              not_found_handling: "404-page",
            },
          },
        }),
      ],
}));
