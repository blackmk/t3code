import { renderToStaticMarkup } from "react-dom/server";
import { describe, expect, it } from "vite-plus/test";

import { PanelLayoutControls } from "./PanelLayoutControls";

function renderControls({
  terminalAvailable = true,
  terminalOpen = false,
  terminalWorkspaceOpen = false,
}: {
  terminalAvailable?: boolean;
  terminalOpen?: boolean;
  terminalWorkspaceOpen?: boolean;
} = {}) {
  return renderToStaticMarkup(
    <PanelLayoutControls
      terminalAvailable={terminalAvailable}
      terminalOpen={terminalOpen}
      terminalWorkspaceOpen={terminalWorkspaceOpen}
      terminalShortcutLabel="Ctrl+J"
      terminalWorkspaceShortcutLabel="Ctrl+Shift+J"
      rightPanelAvailable
      rightPanelOpen={false}
      rightPanelShortcutLabel={null}
      liveAgentCount={0}
      onToggleTerminal={() => {}}
      onToggleTerminalWorkspace={() => {}}
      onToggleRightPanel={() => {}}
    />,
  );
}

describe("PanelLayoutControls terminal view toggle", () => {
  it("offers the terminal view while chat is visible", () => {
    const markup = renderControls();

    expect(markup).toContain('aria-label="Toggle terminal drawer"');
    expect(markup).toContain('aria-label="Switch to terminal view"');
    expect(markup).toContain("lucide-terminal");
  });

  it("offers the reverse path back to chat while the terminal is visible", () => {
    const markup = renderControls({ terminalWorkspaceOpen: true });

    expect(markup).toContain('aria-label="Switch to chat view"');
    expect(markup).toContain('aria-pressed="true"');
    expect(markup).toContain("lucide-message-square");
  });

  it("tracks drawer and workspace pressed states independently", () => {
    const markup = renderControls({ terminalOpen: true });

    expect(markup).toMatch(/aria-pressed="true"[^>]*aria-label="Toggle terminal drawer"/);
    expect(markup).toMatch(/aria-pressed="false"[^>]*aria-label="Switch to terminal view"/);
  });

  it("keeps the chat return path enabled when the project becomes unavailable", () => {
    const markup = renderControls({ terminalAvailable: false, terminalWorkspaceOpen: true });
    const drawerButton = markup.match(/<button[^>]*aria-label="Toggle terminal drawer"[^>]*>/)?.[0];
    const workspaceButton = markup.match(/<button[^>]*aria-label="Switch to chat view"[^>]*>/)?.[0];

    expect(drawerButton).toContain('disabled=""');
    expect(workspaceButton).toContain('aria-pressed="true"');
    expect(workspaceButton).not.toContain('disabled=""');
  });
});
