import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// The API is proxied rather than called cross-origin, so the console needs no
// CORS handling in the backend and the same build works behind any host.
export default defineConfig({
  plugins: [react()],
  server: {
    // 5273, not Vite's default 5173: that port is commonly already taken by
    // another project's dev server, and silently landing on 5174 makes every
    // proxied request go somewhere unexpected.
    port: 5273,
    strictPort: true,
    proxy: {
      "/workflows": "http://127.0.0.1:8000",
      "/runs": "http://127.0.0.1:8000",
      "/health": "http://127.0.0.1:8000",
    },
  },
});
