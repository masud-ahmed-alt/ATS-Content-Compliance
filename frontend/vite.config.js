import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],

  // ==================== Optimization ====================
  build: {
    // Output optimization
    outDir: "dist",
    sourcemap: false, // disable sourcemaps in production
    minify: "terser",
    terserOptions: {
      compress: {
        drop_console: true,
        drop_debugger: true,
      },
    },

    // Code splitting for better caching
    rollupOptions: {
      output: {
        manualChunks: {
          // Vendor chunks
          vendor: ["react", "react-dom", "react-router-dom"],
          bootstrap: ["bootstrap"],
          utils: ["axios", "xlsx"],

          // Feature chunks
          pages: [
            "./src/pages/Dashboard.jsx",
            "./src/pages/Report.jsx",
            "./src/pages/Events.jsx",
          ],
          components: [
            "./src/components/ProgressTracker.jsx",
            "./src/components/ProductGallery.jsx",
          ],
        },
        // Better filenames for caching
        entryFileNames: "js/[name]-[hash:8].js",
        chunkFileNames: "js/[name]-[hash:8].js",
        assetFileNames: (assetInfo) => {
          const info = assetInfo.name.split(".");
          const ext = info[info.length - 1];
          if (/png|jpe?g|gif|svg/.test(ext)) {
            return `img/[name]-[hash:8][extname]`;
          }
          if (/woff|woff2|eot|ttf|otf/.test(ext)) {
            return `fonts/[name]-[hash:8][extname]`;
          }
          if (ext === "css") {
            return `css/[name]-[hash:8][extname]`;
          }
          return `misc/[name]-[hash:8][extname]`;
        },
      },
    },

    // Performance hints
    chunkSizeWarningLimit: 500,

    // Rollup config
    commonjsOptions: {
      transformMixedEsModules: true,
    },
  },

  // ==================== Development ====================
  server: {
    port: 5173,
    strictPort: false,
    cors: true,
  },
});
