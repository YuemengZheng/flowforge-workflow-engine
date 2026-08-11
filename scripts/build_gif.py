"""Assemble the console GIF from the frames ``capture_console.mjs --frames`` took.

Two passes, because a naive GIF of a dark UI comes out either huge or banded:

* **Deduplicate.** A run finishes long before the last frame, so the tail is the
  same picture repeated. Identical consecutive frames are collapsed and the last
  distinct one is held instead, which is both smaller and reads better.
* **One shared palette, sampled from every frame.** Quantising each frame
  separately makes the background shift frame to frame — a visible flicker on a
  dark theme. Taking the palette from a single frame is worse: the first attempt
  used the last frame, which is all greens, so the amber of a *running* node got
  mapped to the nearest green-ish colour and came out muddy red. The palette is
  built from a montage of all frames, so every state's colour survives.

Pillow is a documentation dependency, not a runtime one: nothing in ``flowforge``
imports it.

    python3 scripts/build_gif.py docs/frames docs/console.gif
"""

from __future__ import annotations

import sys
from pathlib import Path

from PIL import Image

FRAME_MS = 170
HOLD_MS = 1800  # pause on the finished state before looping
MAX_WIDTH = 1100


def load(directory: Path) -> list[Image.Image]:
    paths = sorted(directory.glob("frame-*.png"))
    if not paths:
        raise SystemExit(f"no frames in {directory}")
    frames = []
    for path in paths:
        image = Image.open(path).convert("RGB")
        if image.width > MAX_WIDTH:
            ratio = MAX_WIDTH / image.width
            image = image.resize(
                (MAX_WIDTH, round(image.height * ratio)), Image.LANCZOS
            )
        frames.append(image)
    return frames


def dedupe(frames: list[Image.Image]) -> tuple[list[Image.Image], list[int]]:
    """Collapse identical consecutive frames, keeping their total duration."""
    kept: list[Image.Image] = []
    durations: list[int] = []
    for frame in frames:
        if kept and frame.tobytes() == kept[-1].tobytes():
            durations[-1] += FRAME_MS
        else:
            kept.append(frame)
            durations.append(FRAME_MS)
    durations[-1] = max(durations[-1], HOLD_MS)
    return kept, durations


def shared_palette(frames: list[Image.Image]) -> Image.Image:
    """A palette that has seen every frame, so no state's colour is dropped."""
    width, height = frames[0].size
    montage = Image.new("RGB", (width, height * len(frames)))
    for index, frame in enumerate(frames):
        montage.paste(frame, (0, index * height))
    return montage.quantize(colors=192, method=Image.MEDIANCUT)


def main() -> int:
    source = Path(sys.argv[1] if len(sys.argv) > 1 else "docs/frames")
    target = Path(sys.argv[2] if len(sys.argv) > 2 else "docs/console.gif")

    frames, durations = dedupe(load(source))
    palette = shared_palette(frames)
    quantised = [frame.quantize(palette=palette, dither=Image.NONE) for frame in frames]

    quantised[0].save(
        target,
        save_all=True,
        append_images=quantised[1:],
        duration=durations,
        loop=0,
        optimize=True,
        disposal=1,
    )
    size_kb = target.stat().st_size / 1024
    print(f"{target}: {len(quantised)} frames, {size_kb:.0f} KiB")
    if size_kb > 3000:
        print("  warning: over 3 MiB — GitHub will be slow to load this")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
