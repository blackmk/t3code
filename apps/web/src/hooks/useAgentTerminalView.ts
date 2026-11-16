import { scopedThreadKey } from "@t3tools/client-runtime/environment";
import {
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import { AGENT_TERMINAL_ID, type ScopedThreadRef } from "@t3tools/contracts";
import { useEffect, useRef, useState } from "react";
import { terminalEnvironment } from "../state/terminal";
import { useAtomCommand } from "../state/use-atom-command";
import { useAgentTerminalViewStore } from "../state/agentTerminalView";
import { toastManager } from "../components/ui/toast";
import { useStableCallback } from "./useStableCallback";

export function useAgentTerminalView(options: {
  readonly threadRef: ScopedThreadRef | null;
  readonly available: boolean;
  readonly prepare: () => Promise<void>;
}) {
  const { threadRef, available, prepare } = options;
  const key = threadRef ? scopedThreadKey(threadRef) : null;
  const terminal = useAgentTerminalViewStore(
    (state) => key !== null && state.terminalThreads[key] === true,
  );
  const setTerminal = useAgentTerminalViewStore((state) => state.setTerminal);
  const close = useAtomCommand(terminalEnvironment.close, { reportFailure: false });
  const pending = useRef(false);
  const [pendingKey, setPendingKey] = useState<string | null>(null);
  // The terminal view opens the session itself once it has measured its grid.
  const toggle = useStableCallback(async () => {
    const target = threadRef;
    if (!target || !available || pending.current) return;
    pending.current = true;
    setPendingKey(scopedThreadKey(target));
    try {
      if (!terminal) {
        await prepare();
      } else {
        const result = await close({
          environmentId: target.environmentId,
          input: { threadId: target.threadId, terminalId: AGENT_TERMINAL_ID, returnToChat: true },
        });
        if (result._tag === "Failure") {
          if (isAtomCommandInterrupted(result)) return;
          throw squashAtomCommandFailure(result);
        }
      }
      setTerminal(target, !terminal);
    } catch (error) {
      reportSwitchFailure(error);
    } finally {
      pending.current = false;
      setPendingKey(null);
    }
  });
  const openFailed = useStableCallback((error: unknown) => {
    if (threadRef) setTerminal(threadRef, false);
    reportSwitchFailure(error);
  });
  // oxlint-disable-next-line t3code/no-unreasoned-state-sync -- listens for the window-level toggle request while mounted
  useEffect(() => {
    const handler = () => {
      void toggle();
    };
    window.addEventListener("t3:toggle-agent-terminal", handler);
    return () => window.removeEventListener("t3:toggle-agent-terminal", handler);
  }, [toggle]);
  return {
    terminal: available && terminal,
    pending: pendingKey === key && key !== null,
    toggle,
    openFailed,
  };
}

function reportSwitchFailure(error: unknown): void {
  toastManager.add({
    type: "error",
    title: "Could not switch conversation view",
    description: error instanceof Error ? error.message : "Check the connection and retry.",
  });
}
