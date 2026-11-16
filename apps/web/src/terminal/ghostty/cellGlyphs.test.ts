import { describe, expect, it } from "vite-plus/test";

import { drawCellGlyph } from "./cellGlyphs";
import type { GhosttyCell, GhosttySnapshot } from "./core";
import { renderGhosttySnapshot } from "./renderer";

function recordingContext(devicePixelRatio?: number) {
  const rects: number[][] = [];
  const texts: unknown[][] = [];
  const context = {
    canvas: { width: 400, height: 80 },
    globalAlpha: 1,
    getTransform: () => ({ a: devicePixelRatio ?? 1, d: devicePixelRatio ?? 1 }),
    beginPath: () => {},
    clip: () => {},
    fillRect: (...args: number[]) => rects.push(args),
    fillText: (...args: unknown[]) => texts.push(args),
    rect: () => {},
    resetTransform: () => {},
    restore: () => {},
    save: () => {},
    set fillStyle(_value: string) {},
    set font(_value: string) {},
    set textBaseline(_value: string) {},
  } as unknown as CanvasRenderingContext2D;
  return { context, rects, texts };
}

const cell = (text: string): GhosttyCell => ({
  text,
  wide: 0,
  foreground: { r: 255, g: 255, b: 255 },
  background: { r: 0, g: 0, b: 0 },
  bold: false,
  italic: false,
  invisible: false,
  strikethrough: false,
  overline: false,
  underline: false,
  selected: false,
});

describe("drawCellGlyph", () => {
  it("fills block quadrants to the cell edges, not the font's glyph box", () => {
    const { context, rects } = recordingContext();
    expect(drawCellGlyph(context, "▛", 10, 20, 8, 16)).toBe(true);
    expect(rects).toEqual([
      [10, 20, 4, 8],
      [14, 20, 4, 8],
      [10, 28, 4, 8],
    ]);
  });

  it("draws a full block over the whole line height so rows tile without gaps", () => {
    const { context, rects } = recordingContext();
    drawCellGlyph(context, "█", 0, 16, 8, 16);
    drawCellGlyph(context, "█", 0, 32, 8, 16);
    expect(rects).toEqual([
      [0, 16, 8, 16],
      [0, 32, 8, 16],
    ]);
  });

  it("runs box-drawing lines edge to edge through the cell center", () => {
    const { context, rects } = recordingContext();
    drawCellGlyph(context, "─", 0, 0, 8, 16);
    drawCellGlyph(context, "│", 8, 0, 8, 16);
    expect(rects).toEqual([
      [0, 8, 5, 1],
      [4, 8, 4, 1],
      [12, 0, 1, 9],
      [12, 8, 1, 8],
    ]);
  });

  it("leaves ordinary text to the font", () => {
    const { context, rects } = recordingContext();
    expect(drawCellGlyph(context, "a", 0, 0, 8, 16)).toBe(false);
    expect(rects).toEqual([]);
  });
});

describe("renderGhosttySnapshot with cell glyphs", () => {
  it("clears each row on the same device pixels its neighbour's blocks end on", () => {
    // 125% display scaling: unsnapped, row 1's clear would start half a pixel
    // inside row 0's last block row and leave a seam through the logo.
    const { context, rects } = recordingContext(1.25);
    const row = { cells: [cell("█")], text: "█", isWrapContinuation: false, wrapsToNext: false };
    const snapshot: GhosttySnapshot = {
      cols: 1,
      rows: 2,
      foreground: { r: 255, g: 255, b: 255 },
      background: { r: 0, g: 0, b: 0 },
      cursor: { r: 255, g: 255, b: 255 },
      cursorX: -1,
      cursorY: -1,
      cursorVisible: false,
      cursorBlinking: false,
      cursorStyle: 1,
      dirtyRows: new Set([0, 1]),
      rowData: [row, row],
    };

    renderGhosttySnapshot({
      context,
      snapshot,
      metrics: { width: 8, height: 18, baseline: 13 },
      fontSize: 13,
      fontFamily: "monospace",
      padding: 4,
      forceFull: false,
      cursorOn: false,
    });

    const [, firstBlock, secondClear] = rects;
    expect(firstBlock![1]! + firstBlock![3]!).toBeCloseTo(secondClear![1]!, 10);
  });

  it("keeps text after block art on its own cells", () => {
    const { context, texts } = recordingContext();
    const cells = [cell("▐"), cell("▛"), cell("a"), cell("b")];
    const snapshot: GhosttySnapshot = {
      cols: 4,
      rows: 1,
      foreground: { r: 255, g: 255, b: 255 },
      background: { r: 0, g: 0, b: 0 },
      cursor: { r: 255, g: 255, b: 255 },
      cursorX: -1,
      cursorY: -1,
      cursorVisible: false,
      cursorBlinking: false,
      cursorStyle: 1,
      dirtyRows: new Set([0]),
      rowData: [{ cells, text: "▐▛ab", isWrapContinuation: false, wrapsToNext: false }],
    };

    renderGhosttySnapshot({
      context,
      snapshot,
      metrics: { width: 8, height: 16, baseline: 11 },
      fontSize: 12,
      fontFamily: "monospace",
      padding: 4,
      forceFull: false,
      cursorOn: false,
    });

    // The block glyphs never reach fillText, so the text run starts exactly at
    // column 2 instead of after whatever advance a fallback font gives them.
    expect(texts).toEqual([["ab", 20, 15, 16]]);
  });
});
