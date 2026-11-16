import {
  AGENT_TERMINAL_ID,
  type ScopedThreadRef,
  type ResolvedKeybindingsConfig,
} from "@t3tools/contracts";
import {
  isAtomCommandInterrupted,
  squashAtomCommandFailure,
} from "@t3tools/client-runtime/state/runtime";
import { useEffectEvent, useRef, useState } from "react";
import * as Schema from "effect/Schema";
import { TerminalViewport } from "./ThreadTerminalDrawer";
import { Button } from "./ui/button";
import { toastManager } from "./ui/toast";
import { terminalEnvironment } from "../state/terminal";
import { useAtomCommand } from "../state/use-atom-command";
import { useLocalStorage } from "../hooks/useLocalStorage";
import { TYPOGRAPHY_ADVANCED_STORAGE_KEY } from "../appearanceFonts";

/**
 * Hosts the native CLI for a thread. The CLI draws its first frame for the
 * size it is spawned at, so the session is opened only after the viewport has
 * measured its grid; spawning at a default size leaves that frame wrapped.
 */
export function AgentTerminalView({
  threadRef,
  cwd,
  keybindings,
  busy,
  onOpenFailed,
}: {
  threadRef: ScopedThreadRef;
  cwd: string;
  keybindings: ResolvedKeybindingsConfig;
  busy: boolean;
  onOpenFailed: (error: unknown) => void;
}) {
  const [advancedTypography] = useLocalStorage(
    TYPOGRAPHY_ADVANCED_STORAGE_KEY,
    false,
    Schema.Boolean,
  );
  const [exited, setExited] = useState(false);
  const [generation, setGeneration] = useState(0);
  const [restarting, setRestarting] = useState(false);
  const [attachReady, setAttachReady] = useState(false);
  const gridRef = useRef<{ cols: number; rows: number } | null>(null);
  const openRequestedRef = useRef(false);
  const open = useAtomCommand(terminalEnvironment.open, { reportFailure: false });
  const resize = useAtomCommand(terminalEnvironment.resize, { reportFailure: false });
  const stop = useAtomCommand(terminalEnvironment.close, { reportFailure: false });
  const handleGridResize = useEffectEvent(async (cols: number, rows: number) => {
    gridRef.current = { cols, rows };
    if (openRequestedRef.current) return;
    openRequestedRef.current = true;
    const result = await open({
      environmentId: threadRef.environmentId,
      input: { threadId: threadRef.threadId, terminalId: AGENT_TERMINAL_ID, cwd, cols, rows },
    });
    if (result._tag === "Failure") {
      if (!isAtomCommandInterrupted(result)) onOpenFailed(squashAtomCommandFailure(result));
      return;
    }
    // A resize while the open was in flight found no session to apply to.
    const latest = gridRef.current;
    if (latest && (latest.cols !== cols || latest.rows !== rows)) {
      await resize({
        environmentId: threadRef.environmentId,
        input: { threadId: threadRef.threadId, terminalId: AGENT_TERMINAL_ID, ...latest },
      });
    }
    setAttachReady(true);
  });
  return (
    <div
      className="flex min-h-0 flex-1 flex-col"
      data-terminal-owner="agent"
      data-agent-terminal-view
    >
      <div className="flex shrink-0 items-center justify-between border-b px-4 py-2 text-xs text-muted-foreground">
        <span>
          {exited
            ? "Terminal exited. Restart to resume this conversation."
            : "Native agent terminal. Answer permission requests here."}
        </span>
        <div className="flex items-center gap-2">
          {busy && !exited ? (
            <Button
              variant="ghost"
              size="sm"
              disabled={restarting}
              onClick={async () => {
                setRestarting(true);
                try {
                  const result = await stop({
                    environmentId: threadRef.environmentId,
                    input: { threadId: threadRef.threadId, terminalId: AGENT_TERMINAL_ID },
                  });
                  if (result._tag === "Failure") {
                    if (!isAtomCommandInterrupted(result)) throw squashAtomCommandFailure(result);
                    return;
                  }
                  setExited(true);
                } catch (error) {
                  toastManager.add({
                    type: "error",
                    title: "Could not stop terminal",
                    description:
                      error instanceof Error ? error.message : "Check the connection and retry.",
                  });
                } finally {
                  setRestarting(false);
                }
              }}
            >
              Stop
            </Button>
          ) : null}
          <Button
            variant="ghost"
            size="sm"
            disabled={busy || restarting}
            onClick={async () => {
              setRestarting(true);
              try {
                // Handing the conversation back first ends the old CLI and its native session.
                const stopped = await stop({
                  environmentId: threadRef.environmentId,
                  input: {
                    threadId: threadRef.threadId,
                    terminalId: AGENT_TERMINAL_ID,
                    returnToChat: true,
                  },
                });
                const result =
                  stopped._tag === "Failure"
                    ? stopped
                    : await open({
                        environmentId: threadRef.environmentId,
                        input: {
                          threadId: threadRef.threadId,
                          terminalId: AGENT_TERMINAL_ID,
                          cwd,
                          ...(gridRef.current ?? { cols: 120, rows: 30 }),
                        },
                      });
                if (result._tag === "Failure") {
                  if (!isAtomCommandInterrupted(result)) throw squashAtomCommandFailure(result);
                  return;
                }
                setExited(false);
                setGeneration((value) => value + 1);
              } catch (error) {
                toastManager.add({
                  type: "error",
                  title: "Could not restart terminal",
                  description:
                    error instanceof Error ? error.message : "Check the connection and retry.",
                });
              } finally {
                setRestarting(false);
              }
            }}
          >
            Restart
          </Button>
        </div>
      </div>
      <div className="min-h-0 flex-1">
        <TerminalViewport
          key={generation}
          advancedTypography={advancedTypography}
          threadRef={threadRef}
          threadId={threadRef.threadId}
          terminalId={AGENT_TERMINAL_ID}
          terminalLabel="Agent terminal"
          cwd={cwd}
          onSessionExited={() => setExited(true)}
          autoFocus
          visible
          keybindings={keybindings}
          nativeAgent
          attachReady={attachReady}
          onGridResize={(cols, rows) => void handleGridResize(cols, rows)}
        />
      </div>
    </div>
  );
}
