/**
 * Box-drawing lines and block elements painted as cell geometry: font glyphs
 * rarely fill the cell, so fallback advances shift text runs and short glyphs
 * leave gaps in borders and block art.
 */

// Stroke weight toward up, right, down, left: 0 none, 1 light, 2 heavy.
const LINE_SEGMENTS: Readonly<Record<string, string>> = {
  "─": "0101",
  "━": "0202",
  "│": "1010",
  "┃": "2020",
  "┌": "0110",
  "┐": "0011",
  "└": "1100",
  "┘": "1001",
  "┏": "0220",
  "┓": "0022",
  "┗": "2200",
  "┛": "2002",
  "├": "1110",
  "┤": "1011",
  "┬": "0111",
  "┴": "1101",
  "┼": "1111",
  "┣": "2220",
  "┫": "2022",
  "┳": "0222",
  "┻": "2202",
  "╋": "2222",
  "╴": "0001",
  "╵": "1000",
  "╶": "0100",
  "╷": "0010",
  "╸": "0002",
  "╹": "2000",
  "╺": "0200",
  "╻": "0020",
};

// Rounded corners as the horizontal and vertical edge their arc joins.
const ROUNDED_CORNERS: Readonly<Record<string, readonly [1 | -1, 1 | -1]>> = {
  "╭": [1, 1],
  "╮": [-1, 1],
  "╯": [-1, -1],
  "╰": [1, -1],
};

const BLOCK_FIRST = 0x2580;
const BLOCK_LAST = 0x259f;
// Quadrant bits: upper left 1, upper right 2, lower left 4, lower right 8.
const QUADRANTS: Readonly<Record<number, number>> = {
  0x2596: 4,
  0x2597: 8,
  0x2598: 1,
  0x2599: 13,
  0x259a: 9,
  0x259b: 7,
  0x259c: 11,
  0x259d: 2,
  0x259e: 6,
  0x259f: 14,
};

function blockCodePoint(text: string): number | null {
  if (text.length !== 1) return null;
  const code = text.charCodeAt(0);
  return code >= BLOCK_FIRST && code <= BLOCK_LAST ? code : null;
}

/**
 * Rounds CSS pixels to the canvas's device pixel grid. Row clears and cell
 * glyphs must share it, or a clear at a fractional DPR erases part of the row
 * above and leaves a seam through block art.
 */
function deviceSnap(context: CanvasRenderingContext2D): (value: number) => number {
  const scale = typeof context.getTransform === "function" ? context.getTransform().a || 1 : 1;
  return (value) => Math.round(value * scale) / scale;
}

export function isCellGlyph(text: string): boolean {
  return (
    blockCodePoint(text) !== null ||
    LINE_SEGMENTS[text] !== undefined ||
    ROUNDED_CORNERS[text] !== undefined
  );
}

/**
 * Paints `text` over one cell with the current fill style. Returns false when
 * the glyph is not one this module draws, leaving it to the font.
 */
export function drawCellGlyph(
  context: CanvasRenderingContext2D,
  text: string,
  left: number,
  top: number,
  width: number,
  height: number,
): boolean {
  // Snap edges to device pixels so adjacent cells meet without seams.
  const snap = deviceSnap(context);
  const x0 = snap(left);
  const x1 = snap(left + width);
  const y0 = snap(top);
  const y1 = snap(top + height);
  const fill = (fromX: number, fromY: number, toX: number, toY: number) => {
    const ax = snap(x0 + (x1 - x0) * fromX);
    const ay = snap(y0 + (y1 - y0) * fromY);
    const bx = snap(x0 + (x1 - x0) * toX);
    const by = snap(y0 + (y1 - y0) * toY);
    if (bx > ax && by > ay) context.fillRect(ax, ay, bx - ax, by - ay);
  };

  const code = blockCodePoint(text);
  if (code !== null) {
    const quadrants = QUADRANTS[code];
    if (quadrants !== undefined) {
      if (quadrants & 1) fill(0, 0, 0.5, 0.5);
      if (quadrants & 2) fill(0.5, 0, 1, 0.5);
      if (quadrants & 4) fill(0, 0.5, 0.5, 1);
      if (quadrants & 8) fill(0.5, 0.5, 1, 1);
    } else if (code === 0x2580) {
      fill(0, 0, 1, 0.5);
    } else if (code <= 0x2588) {
      fill(0, 1 - (code - 0x2580) / 8, 1, 1);
    } else if (code <= 0x258f) {
      fill(0, 0, (0x2590 - code) / 8, 1);
    } else if (code === 0x2590) {
      fill(0.5, 0, 1, 1);
    } else if (code <= 0x2593) {
      const alpha = context.globalAlpha;
      context.globalAlpha = alpha * ((code - 0x2590) / 4);
      fill(0, 0, 1, 1);
      context.globalAlpha = alpha;
    } else if (code === 0x2594) {
      fill(0, 0, 1, 1 / 8);
    } else {
      fill(7 / 8, 0, 1, 1);
    }
    return true;
  }

  const light = Math.max(1, Math.round(width / 8));
  // A line's leading edge is snapped, not its center, so odd widths stay crisp.
  const lineX = (size: number) => snap(left + width / 2 - size / 2);
  const lineY = (size: number) => snap(top + height / 2 - size / 2);
  const segments = LINE_SEGMENTS[text];
  if (segments !== undefined) {
    const [up, right, down, toLeft] = [...segments].map(Number);
    const stroke = (weight: number) => (weight === 2 ? light * 2 : light);
    if (up) {
      const size = stroke(up);
      context.fillRect(lineX(size), y0, size, lineY(size) + size - y0);
    }
    if (down) {
      const size = stroke(down);
      const from = lineY(size);
      context.fillRect(lineX(size), from, size, y1 - from);
    }
    if (toLeft) {
      const size = stroke(toLeft);
      context.fillRect(x0, lineY(size), lineX(size) + size - x0, size);
    }
    if (right) {
      const size = stroke(right);
      const from = lineX(size);
      context.fillRect(from, lineY(size), x1 - from, size);
    }
    return true;
  }

  const corner = ROUNDED_CORNERS[text];
  if (corner !== undefined) {
    const [horizontal, vertical] = corner;
    // Stroke along the same center as a straight light line so the arc meets
    // the neighbouring cell's segment exactly.
    const centerX = lineX(light) + light / 2;
    const centerY = lineY(light) + light / 2;
    const edgeX = horizontal === 1 ? x1 : x0;
    const edgeY = vertical === 1 ? y1 : y0;
    const radius = Math.min(Math.abs(edgeX - centerX), Math.abs(edgeY - centerY));
    context.save();
    context.strokeStyle = context.fillStyle;
    context.lineWidth = light;
    context.beginPath();
    context.moveTo(edgeX, centerY);
    context.arcTo(centerX, centerY, centerX, edgeY, radius);
    context.lineTo(centerX, edgeY);
    context.stroke();
    context.restore();
    return true;
  }
  return false;
}
