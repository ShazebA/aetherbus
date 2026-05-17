import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The Vite build emits to ./dist; FastAPI mounts it at "/" and "/assets" when
// present. Dev: `vite` proxies /api → FastAPI on :7798 so the React app and
// the Python backend can run side-by-side during development.
export default defineConfig({
  plugins: [react()],
  build: {
    outDir: "dist",
    emptyOutDir: true,
    sourcemap: false,
  },
  server: {
    port: 5173,
    proxy: {
      "/api": {
        target: "http://localhost:7798",
        ws: true,
        changeOrigin: true,
      },
    },
  },
});
