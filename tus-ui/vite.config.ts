// @ts-expect-error untyped for some reason but fine
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    host: "0.0.0.0", // 添加此行，允许局域网访问
    port: 3000, // 可选：指定端口号
    strictPort: true, // 可选：如果端口被占用则退出
    proxy: {
      "/tus": {
        // target: 'http://10.2.25.3:8080',
        target: "http://localhost:8080",
        changeOrigin: true,
      },
    },
  },
});
