// @effect-diagnostics nodeBuiltinImport:off
// The authenticated hook endpoint is a loopback server owned by this native process scope.
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import { NativeTerminalError, ProviderDriverKind } from "@t3tools/contracts";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Option from "effect/Option";
import * as NodeHttp from "node:http";
import * as NodeCrypto from "node:crypto";
import { record } from "./NativeCodex.ts";
import type { NativeRuntime } from "./NativeRuntime.ts";
import {
  ProviderAdapterProtocolError,
  type ProviderAdapterV2TurnInput,
} from "@t3tools/provider-core/server/ProviderAdapter";

const NATIVE_CLAUDE_HOOK_CLIENT = String.raw`
const chunks = [];
let bytes = 0;
process.stdin.on('data', chunk => {
  bytes += chunk.length;
  if (bytes > 16777216) process.exit(2);
  chunks.push(chunk);
});
process.stdin.on('end', async () => {
  try {
    const response = await fetch(process.argv[2], {
      method: 'POST',
      headers: { 'content-type': 'application/json', authorization: 'Bearer ' + process.argv[3] },
      body: Buffer.concat(chunks),
      signal: AbortSignal.timeout(90000)
    });
    if (!response.ok) throw new Error('T3 terminal connection failed. Restart the terminal.');
    process.stdout.write(await response.text());
  } catch (error) {
    process.stderr.write(error.message);
    process.exitCode = 2;
  }
});
`;
const string = (value: unknown) => (typeof value === "string" ? value : "");
const isNativeTerminalError = Schema.is(NativeTerminalError);
// "Task" is the tool's name in older Claude Code releases.
const CLAUDE_SUBAGENT_TOOLS = new Set(["Agent", "Task"]);
// Where "block" refuses an action. On Stop, SubagentStop, or PostToolUse it
// would instead keep the agent working, so failures there stay silent.
const REFUSABLE_EVENTS = new Set(["UserPromptSubmit", "PreToolUse", "PreModelSwitch"]);

function refuse(event: string, reason: string): Record<string, unknown> {
  return REFUSABLE_EVENTS.has(event) ? { decision: "block", reason } : {};
}

// Claude Code hands background work to the model as a prompt: finished tasks as
// task notifications, and what its agents or peer sessions send as agent
// messages, which can follow a one-line header.
const BACKGROUND_PROMPT = /^\s*(?:[^\n<]*:\n)?<(?:task-notification|agent-message)[\s>]/;
const BACKGROUND_ENTRY = /<(?:task-notification|agent-message)[\s>]/;

/**
 * Whether the conversation's latest exchange ended in an Esc cancellation,
 * which Claude Code records in the transcript but reports through no hook.
 */
function lastTurnInterrupted(entries: ReadonlyArray<Record<string, unknown>>): boolean {
  for (const entry of entries) {
    if (entry.type !== "user" && entry.type !== "assistant") continue;
    const content = json(record(entry.message).content ?? "");
    if (entry.type === "user" && BACKGROUND_ENTRY.test(content)) continue;
    return entry.type === "user" && content.includes("[Request interrupted by user");
  }
  return false;
}

/** The text a finished Agent tool call returned, or null when it has none. */
function claudeAgentResultText(response: unknown): string | null {
  if (typeof response === "string") return response.trim() ? response : null;
  const content = record(response).content;
  const text = Array.isArray(content)
    ? content
        .map((part) => string(record(part).text))
        .filter(Boolean)
        .join("\n\n")
    : "";
  return text || null;
}
const json = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
const error = (cause: unknown) =>
  new ProviderAdapterProtocolError({
    driver: ProviderDriverKind.make("claudeAgent"),
    detail: "Native Claude hook failed.",
    cause,
  });
const shellQuote = (value: string) => `'${value.replaceAll("'", "'\\''")}'`;
const powerShellQuote = (value: string) => `'${value.replaceAll("'", "''")}'`;

function nativeClaudeHookCommand(input: {
  platform: NodeJS.Platform;
  executable: string;
  script: string;
  url: string;
  token: string;
}): string {
  const args = [input.executable, input.script, input.url, input.token];
  return input.platform === "win32"
    ? `$env:ELECTRON_RUN_AS_NODE='1'; & ${args.map(powerShellQuote).join(" ")}; exit $LASTEXITCODE`
    : `ELECTRON_RUN_AS_NODE=1 ${args.map(shellQuote).join(" ")}`;
}

const readBody = async (request: NodeHttp.IncomingMessage) => {
  const chunks: Buffer[] = [];
  let size = 0;
  for await (const chunk of request) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    size += buffer.byteLength;
    if (size > 16 * 1024 * 1024) throw new Error("Native hook input is too large.");
    chunks.push(buffer);
  }
  return Buffer.concat(chunks).toString("utf8");
};

/** Command hooks fail the prompt when V2 cannot capture its workspace baseline. */
export const makeNativeClaude = Effect.fn("terminal.makeNativeClaude")(function* (options: {
  readonly stateDir: string;
  readonly nativeThreadId: string;
  readonly platform: NodeJS.Platform;
  readonly executable: string;
  readonly submit: (text: string) => Effect.Effect<ProviderAdapterV2TurnInput, NativeTerminalError>;
  readonly getRuntime: () => NativeRuntime | undefined;
}) {
  const fs = yield* FileSystem.FileSystem;
  const scope = yield* Effect.scope;
  const path = yield* Path.Path;
  const directory = path.join(options.stateDir, "native-terminal", NodeCrypto.randomUUID());
  yield* fs.makeDirectory(directory, { recursive: true });
  yield* Effect.addFinalizer(() =>
    fs.remove(directory, { recursive: true, force: true }).pipe(Effect.ignore),
  );
  const script = path.join(directory, "hook.cjs");
  yield* fs.writeFileString(script, NATIVE_CLAUDE_HOOK_CLIENT);
  const token = NodeCrypto.randomUUID();
  // Background agent id -> the Agent tool call that launched it.
  const backgroundAgents = new Map<string, string>();
  // A fast agent's stop can beat its own launch acknowledgment; agent id -> answer.
  const earlyStops = new Map<string, string>();
  /** Transcript entries from the end of the file, newest first. */
  const readTranscriptTail = (transcript: string) =>
    Effect.scoped(
      Effect.gen(function* () {
        const file = yield* fs.open(transcript, { flag: "r" });
        const info = yield* file.stat;
        yield* file.seek(info.size > 262144n ? info.size - 262144n : 0n, "start");
        const bytes = yield* file.readAlloc(262144);
        if (Option.isNone(bytes)) return [];
        const entries: Array<Record<string, unknown>> = [];
        for (const line of new TextDecoder().decode(bytes.value).split("\n").toReversed()) {
          entries.push(
            yield* decodeJson(line).pipe(
              Effect.map(record),
              Effect.orElseSucceed(() => ({}) as Record<string, unknown>),
            ),
          );
        }
        return entries;
      }),
    ).pipe(Effect.orElseSucceed(() => [] as Array<Record<string, unknown>>));
  const context = yield* Effect.context<never>();
  const server = yield* Effect.acquireRelease(
    Effect.callback<ReturnType<typeof NodeHttp.createServer>, ProviderAdapterProtocolError>(
      (resume) => {
        const http = NodeHttp.createServer((request, response) => {
          if (request.method !== "POST" || request.headers.authorization !== `Bearer ${token}`) {
            response.writeHead(403).end();
            return;
          }
          let event = "";
          Effect.runForkWith(context)(
            Effect.gen(function* () {
              const raw = yield* Effect.tryPromise({ try: () => readBody(request), catch: error });
              const payload = record(yield* decodeJson(raw).pipe(Effect.mapError(error)));
              event = string(payload.hook_event_name);
              if (payload.session_id !== options.nativeThreadId) {
                return refuse(
                  event,
                  "This terminal is bound to its T3 conversation. Restart the terminal to return to it.",
                );
              }
              const runtime = options.getRuntime();
              if (!runtime) return refuse(event, "The T3 terminal is not ready. Restart it.");
              if (event === "UserPromptSubmit") {
                const prompt = string(payload.prompt);
                // Background prompts also arrive mid-turn; they continue the
                // running turn rather than start one, unless that turn was
                // cancelled with Esc, which fires no hook.
                if (
                  runtime.active &&
                  BACKGROUND_PROMPT.test(prompt) &&
                  !lastTurnInterrupted(yield* readTranscriptTail(string(payload.transcript_path)))
                )
                  return {};
                // A native cancellation has no Stop hook. A new prompt proves the CLI is idle.
                if (runtime.active) yield* runtime.finish("interrupted");
                const prepared = yield* options.submit(prompt);
                return prepared.message.text !== prompt
                  ? {
                      hookSpecificOutput: {
                        hookEventName: event,
                        additionalContext: prepared.message.text,
                      },
                    }
                  : {};
              }
              const toolId = string(payload.tool_use_id);
              const name = string(payload.tool_name) || "Tool";
              // Hooks fired inside a subagent carry its agent_id. That work
              // belongs to the subagent, not the parent conversation.
              const insideSubagent = string(payload.agent_id) !== "";
              if (event === "PreToolUse") {
                yield* runtime.approval("permission", "Claude terminal request", true);
                if (insideSubagent) {
                  // Reported through the subagent's own result.
                } else if (CLAUDE_SUBAGENT_TOOLS.has(name)) {
                  const input = record(payload.tool_input);
                  yield* runtime.subagent(toolId, {
                    status: "running",
                    prompt: string(input.prompt),
                    title: string(input.description) || null,
                  });
                } else {
                  yield* runtime.tool(toolId, name, payload.tool_input, false);
                }
              } else if (event === "PostToolUse" || event === "PostToolUseFailure") {
                yield* runtime.approval("permission", "Claude terminal request", true);
                if (insideSubagent) {
                  // Reported through the subagent's own result.
                } else if (CLAUDE_SUBAGENT_TOOLS.has(name)) {
                  const response = record(payload.tool_response);
                  const agentId = string(response.agentId);
                  if (event === "PostToolUseFailure") {
                    yield* runtime.subagent(toolId, {
                      status: "failed",
                      result: string(payload.error) || null,
                    });
                  } else if (response.status === "async_launched" && !earlyStops.has(agentId)) {
                    // A background agent reports completion through SubagentStop.
                    if (agentId) backgroundAgents.set(agentId, toolId);
                  } else {
                    const early = earlyStops.get(agentId);
                    earlyStops.delete(agentId);
                    yield* runtime.subagent(toolId, {
                      status: "completed",
                      result: claudeAgentResultText(payload.tool_response) ?? (early || null),
                    });
                  }
                } else {
                  yield* runtime.tool(
                    toolId,
                    name,
                    payload.tool_response ?? payload.error,
                    true,
                    event === "PostToolUseFailure",
                  );
                }
              } else if (event === "SubagentStop") {
                const agentId = string(payload.agent_id);
                const result = string(payload.last_assistant_message);
                const subagentToolId = backgroundAgents.get(agentId);
                if (subagentToolId) {
                  backgroundAgents.delete(agentId);
                  yield* runtime.subagent(subagentToolId, {
                    status: "completed",
                    ...(result ? { result } : {}),
                  });
                } else if (agentId) {
                  // Foreground agents stop before their PostToolUse, which then
                  // consumes this entry either way.
                  earlyStops.set(agentId, result);
                }
              } else if (
                event === "PermissionRequest" ||
                (event === "Notification" && payload.notification_type === "permission_prompt")
              ) {
                yield* runtime.approval(
                  "permission",
                  string(payload.message) || `Claude needs permission for ${name}`,
                  false,
                );
              } else if (event === "Stop") {
                // Claude's SDK resumes checkpoints by assistant message UUID, not turn id.
                const transcript = string(payload.transcript_path);
                const assistant = transcript
                  ? (yield* readTranscriptTail(transcript)).find(
                      (entry) => entry.type === "assistant" && typeof entry.uuid === "string",
                    )
                  : undefined;
                if (assistant) yield* runtime.setNativeTurn(String(assistant.uuid), "weak");
                const id = `${runtime.active?.input.runId}:assistant`;
                yield* runtime.assistant(id, string(payload.last_assistant_message), true);
                yield* runtime.finish();
              } else if (event === "StopFailure") {
                yield* runtime.finish(
                  "failed",
                  string(payload.last_assistant_message) || "Claude could not complete this turn.",
                );
              } else if (event === "PreCompact" && !runtime.active) {
                yield* options.submit("/compact");
              } else if (event === "PostCompact") {
                if (runtime.active?.input.message.text === "/compact") yield* runtime.finish();
              } else if (event === "PreModelSwitch") {
                return {
                  decision: "block",
                  reason: "Return to Chat to change the model, then reopen Terminal.",
                };
              } else if (event === "SessionEnd") {
                yield* runtime.finish("interrupted");
              }
              return {};
            }).pipe(
              Effect.catch((cause) =>
                Effect.succeed(
                  refuse(
                    event,
                    isNativeTerminalError(cause)
                      ? cause.detail
                      : "T3 could not prepare this turn. Check the thread status and retry.",
                  ),
                ),
              ),
              Effect.tap((result) =>
                Effect.sync(() => {
                  response.writeHead(200, { "content-type": "application/json" });
                  response.end(json(result));
                }),
              ),
              Effect.forkIn(scope),
              Effect.asVoid,
            ),
          );
        });
        http.once("error", (cause) => resume(Effect.fail(error(cause))));
        http.listen(0, "127.0.0.1", () => resume(Effect.succeed(http)));
      },
    ),
    (http) =>
      Effect.sync(() => {
        http.closeAllConnections();
        http.close();
      }),
  );
  const address = server.address();
  if (!address || typeof address === "string")
    return yield* error("Native Claude hook server has no address.");
  const command = nativeClaudeHookCommand({
    ...options,
    script,
    url: `http://127.0.0.1:${address.port}`,
    token,
  });
  const hooks = Object.fromEntries(
    [
      "SessionStart",
      "UserPromptSubmit",
      "PreToolUse",
      "PostToolUse",
      "PostToolUseFailure",
      "PermissionRequest",
      "Notification",
      "Stop",
      "StopFailure",
      "SubagentStop",
      "SessionEnd",
      "PreCompact",
      "PostCompact",
      "PreModelSwitch",
    ].map((event) => [
      event,
      [
        {
          matcher: "",
          hooks: [
            {
              type: "command",
              command,
              ...(options.platform === "win32" ? { shell: "powershell" } : {}),
              timeout: 120,
            },
          ],
        },
      ],
    ]),
  );
  const settings = path.join(directory, "settings.json");
  yield* fs.writeFileString(settings, json({ hooks }));
  return { settings };
});
