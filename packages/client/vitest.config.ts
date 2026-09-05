import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // Native runtime suites execute with their own runners against dist/.
    include: ["src/**/*.test.ts"],
  },
});
