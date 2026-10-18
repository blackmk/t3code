import { scopedThreadKey } from "@t3tools/client-runtime/environment";
import type { ScopedThreadRef } from "@t3tools/contracts";
import { create } from "zustand";
import { createJSONStorage, persist } from "zustand/middleware";
import { resolveStorage } from "../lib/storage";

export const useAgentTerminalViewStore = create<{
  readonly terminalThreads: Readonly<Record<string, true>>;
  readonly setTerminal: (thread: ScopedThreadRef, terminal: boolean) => void;
}>()(
  persist(
    (set) => ({
      terminalThreads: {},
      setTerminal: (thread, terminal) =>
        set((state) => {
          const terminalThreads = { ...state.terminalThreads };
          if (terminal) terminalThreads[scopedThreadKey(thread)] = true;
          else delete terminalThreads[scopedThreadKey(thread)];
          return { terminalThreads };
        }),
    }),
    {
      name: "t3:agent-terminal-views",
      storage: createJSONStorage(() => resolveStorage(window.localStorage)),
    },
  ),
);

export function requestAgentTerminalToggle(): void {
  window.dispatchEvent(new Event("t3:toggle-agent-terminal"));
}
