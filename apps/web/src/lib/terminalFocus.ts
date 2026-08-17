export type TerminalFocusOwner = "drawer" | "right-panel" | "workspace";

export function getTerminalFocusOwner(): TerminalFocusOwner | null {
  const activeElement = document.activeElement;
  if (!(activeElement instanceof HTMLElement)) return null;
  if (!activeElement.isConnected) return null;
  const owner = activeElement.closest<HTMLElement>("[data-terminal-owner]")?.dataset.terminalOwner;
  if (owner === "drawer" || owner === "right-panel" || owner === "workspace") return owner;
  return null;
}

export function isTerminalFocused(): boolean {
  return getTerminalFocusOwner() !== null;
}
