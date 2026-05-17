import type { Config } from "tailwindcss";

const config: Config = {
  content: ["./index.html", "./src/**/*.{ts,tsx}"],
  theme: {
    extend: {
      colors: {
        // Match the dark palette from the M5a Jinja viewer so the visual jump
        // between dev fallback and the SPA is small.
        bg: "#0b0d12",
        surface: "#11141b",
        border: "#1a1e27",
        fg: "#e6e8ec",
        muted: "#8a8f9a",
        accent: "#4cc9f0",
        ok: "#34c759",
      },
      fontFamily: {
        mono: [
          "ui-monospace",
          "SFMono-Regular",
          "Menlo",
          "Monaco",
          "Consolas",
          "monospace",
        ],
      },
    },
  },
  plugins: [],
};

export default config;
