// Captures the README screenshots of the console.
//
// Chrome's `--headless --screenshot` was the obvious tool and it does not work
// here: `--virtual-time-budget` runs the page clock fast and then shoots, but the
// thing being captured is a live SSE stream over real sockets, so the frame lands
// mid-run every time. This drives Chrome over the DevTools Protocol instead and
// waits in wall-clock time.
//
// No dependencies: Node 22 has a WebSocket client built in, and the console's
// `?run=`/`&then=` params do the clicking.
//
//   node scripts/capture_console.mjs [consoleUrl] [outDir]

import { spawn } from "node:child_process";
import { mkdir, writeFile } from "node:fs/promises";
import { setTimeout as sleep } from "node:timers/promises";

const CHROME = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
const CONSOLE_URL = process.argv[2] ?? "http://localhost:5273";
const OUT = process.argv[3] ?? "docs";
const PORT = 9333;
const WIDTH = 1440;
const HEIGHT = 860;

// `node scripts/capture_console.mjs --frames` instead captures a burst during one
// run, which the GIF is assembled from.
const FRAME_MODE = process.argv.includes("--frames");
const FRAME_COUNT = 26;
const FRAME_EVERY_MS = 170;

const SHOTS = [
  { name: "console-parallel", path: "/?run=showcase", settleMs: 2500 },
  { name: "console-paused", path: "/?run=approval", settleMs: 2500 },
  { name: "console-resumed", path: "/?run=approval&then=resume", settleMs: 4000 },
  { name: "console-recovery", path: "/?run=recovery", settleMs: 5000 },
];

class Session {
  constructor(ws) {
    this.ws = ws;
    this.id = 0;
    this.pending = new Map();
    ws.addEventListener("message", (event) => {
      const message = JSON.parse(event.data);
      const resolve = this.pending.get(message.id);
      if (resolve) {
        this.pending.delete(message.id);
        resolve(message.result ?? {});
      }
    });
  }

  send(method, params = {}) {
    const id = ++this.id;
    this.ws.send(JSON.stringify({ id, method, params }));
    return new Promise((resolve) => this.pending.set(id, resolve));
  }
}

async function devtools(path) {
  const response = await fetch(`http://127.0.0.1:${PORT}${path}`);
  if (!response.ok) throw new Error(`${path} -> ${response.status}`);
  return response.json();
}

async function waitForChrome(attempts = 40) {
  for (let attempt = 0; attempt < attempts; attempt += 1) {
    try {
      await devtools("/json/version");
      return;
    } catch {
      await sleep(250);
    }
  }
  throw new Error("Chrome never opened its debugging port");
}

const chrome = spawn(CHROME, [
  "--headless=new",
  `--remote-debugging-port=${PORT}`,
  `--window-size=${WIDTH},${HEIGHT}`,
  "--hide-scrollbars",
  "--disable-gpu",
  "--no-first-run",
  "--user-data-dir=/tmp/flowforge-capture-profile",
  "about:blank",
]);
chrome.on("error", (exc) => {
  console.error("could not start Chrome:", exc.message);
  process.exit(1);
});

try {
  await waitForChrome();
  await mkdir(OUT, { recursive: true });
  await mkdir(`${OUT}/frames`, { recursive: true });

  // One tab, reused: `/json/new` needs a PUT in current Chrome, and navigating
  // an existing target is simpler than negotiating that.
  const targets = await devtools("/json/list");
  const target = targets.find((t) => t.type === "page");
  if (!target) throw new Error("Chrome exposed no page target");
  const ws = new WebSocket(target.webSocketDebuggerUrl);
  await new Promise((resolve, reject) => {
    ws.addEventListener("open", resolve, { once: true });
    ws.addEventListener("error", reject, { once: true });
  });
  const session = new Session(ws);
  await session.send("Page.enable");

  if (FRAME_MODE) {
    await session.send("Emulation.setDeviceMetricsOverride", {
      width: WIDTH,
      height: HEIGHT,
      deviceScaleFactor: 1, // a GIF does not need 2x, and 2x quadruples its size
      mobile: false,
    });
    await session.send("Page.navigate", {
      url: `${CONSOLE_URL}/?run=showcase&then=resume`,
    });
    for (let frame = 0; frame < FRAME_COUNT; frame += 1) {
      await sleep(FRAME_EVERY_MS);
      const { data } = await session.send("Page.captureScreenshot", { format: "png" });
      const name = `frame-${String(frame).padStart(2, "0")}`;
      await writeFile(`${OUT}/frames/${name}.png`, Buffer.from(data, "base64"));
    }
    console.log(`  ${FRAME_COUNT} frames -> ${OUT}/frames/`);
    ws.close();
    chrome.kill();
    process.exit(0);
  }

  for (const shot of SHOTS) {
    await session.send("Emulation.setDeviceMetricsOverride", {
      width: WIDTH,
      height: HEIGHT,
      deviceScaleFactor: 2, // readable on a high-DPI screen and in a README
      mobile: false,
    });
    await session.send("Page.navigate", { url: CONSOLE_URL + shot.path });
    // Wall clock, not virtual: the run has to actually finish streaming.
    await sleep(shot.settleMs);

    const { data } = await session.send("Page.captureScreenshot", {
      format: "png",
      captureBeyondViewport: false,
    });
    await writeFile(`${OUT}/${shot.name}.png`, Buffer.from(data, "base64"));
    console.log(`  ${OUT}/${shot.name}.png`);
  }
  ws.close();
} finally {
  chrome.kill();
}
