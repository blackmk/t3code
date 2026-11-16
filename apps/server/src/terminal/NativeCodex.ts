// @effect-diagnostics nodeBuiltinImport:off
import * as NodeWS from "@effect/platform-node/NodeWS";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Exit from "effect/Exit";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as Schema from "effect/Schema";
import * as CodexSchema from "effect-codex-app-server/schema";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as NodeCrypto from "node:crypto";
import {
  buildCodexTurnStartParams,
  makeCodexAppServerSpawnCommand,
} from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import type { McpProviderSessionConfig } from "@t3tools/provider-core/server/mcpSession";
import {
  buildCodexInitializeParams,
  codexAppServerEnvironment,
} from "../provider/CodexProvider.ts";
import { makeProviderTextDeltaCoalescer } from "../orchestration-v2/Adapters/ProviderTextDeltaCoalescer.ts";
import {
  ProviderAdapterProtocolError,
  type ProviderAdapterV2TurnInput,
} from "@t3tools/provider-core/server/ProviderAdapter";
import {
  ProviderDriverKind,
  type NativeTerminalError,
  type OrchestrationV2Subagent,
  type ThreadId,
} from "@t3tools/contracts";
import type { NativeRuntime } from "./NativeRuntime.ts";

export const record = (value: unknown): Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : {};
const text = (value: unknown) => (typeof value === "string" ? value : "");
const driver = ProviderDriverKind.make("codex");
// Mirrors the Codex adapter's mapping of collab agent states.
const CODEX_AGENT_STATUSES: Readonly<Record<string, OrchestrationV2Subagent["status"]>> = {
  pendingInit: "pending",
  running: "running",
  interrupted: "interrupted",
  completed: "completed",
  errored: "failed",
  shutdown: "cancelled",
  notFound: "failed",
};
const error = (cause: unknown) =>
  new ProviderAdapterProtocolError({ driver, detail: "Native Codex transport failed.", cause });
const json = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeFrame = Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown));
const parse = (data: string) => decodeFrame(data).pipe(Effect.map(record), Effect.mapError(error));
const decodeUserInput = Schema.decodeUnknownEffect(
  Schema.Array(CodexSchema.V2TurnStartParams__UserInput),
);

function nativeCodexPrompt(params: Record<string, unknown>): string {
  const input = Array.isArray(params.input) ? params.input : [];
  const parts = input
    .map((item) => {
      const value = record(item);
      return value.type === "text"
        ? text(value.text)
        : value.type === "localImage" || value.type === "image"
          ? "[Image attached in terminal]"
          : "";
    })
    .filter(Boolean);
  return parts.join("\n") || "Continue";
}

/** The CLI and T3 observe one app-server connection, with separate RPC id spaces. */
export const makeNativeCodex = Effect.fn("terminal.makeNativeCodex")(function* (options: {
  readonly command: string;
  readonly args: ReadonlyArray<string>;
  readonly env: NodeJS.ProcessEnv;
  readonly cwd: string;
  readonly resumeId?: string;
  readonly threadParams: Record<string, unknown>;
  readonly omitServiceTier?: boolean;
  readonly submit: (text: string) => Effect.Effect<ProviderAdapterV2TurnInput, NativeTerminalError>;
  readonly getRuntime: () => NativeRuntime | undefined;
  readonly readMcpSession: (
    threadId: ThreadId,
  ) => Effect.Effect<McpProviderSessionConfig | undefined>;
}) {
  const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
  const scope = yield* Effect.scope;
  const address = yield* Deferred.make<string, ProviderAdapterProtocolError>();
  const command = yield* makeCodexAppServerSpawnCommand({
    command: options.command,
    args: [...options.args, "app-server", "--listen", "ws://127.0.0.1:0"],
    cwd: options.cwd,
    env: codexAppServerEnvironment(options.env),
  });
  const handle = yield* spawner.spawn(command).pipe(Effect.mapError(error));
  yield* handle.stderr.pipe(
    Stream.decodeText(),
    Stream.splitLines,
    Stream.runForEach((line) => {
      const match = /ws:\/\/127\.0\.0\.1:\d+/.exec(line);
      return match ? Deferred.succeed(address, match[0]) : Effect.void;
    }),
    Effect.catch((cause) => Deferred.fail(address, error(cause))),
    Effect.forkScoped,
  );
  yield* handle.exitCode.pipe(
    Effect.flatMap((code) => Deferred.fail(address, error(`Codex app-server exited (${code}).`))),
    Effect.ignore,
    Effect.forkScoped,
  );
  yield* Stream.runDrain(handle.stdout).pipe(Effect.ignore, Effect.forkScoped);
  const url = yield* Deferred.await(address).pipe(
    Effect.timeout("20 seconds"),
    Effect.mapError(error),
  );
  const upstream = yield* Effect.acquireRelease(
    Effect.callback<NodeWS.WebSocket, ProviderAdapterProtocolError>((resume) => {
      const ws = new NodeWS.WebSocket(url);
      ws.once("open", () => resume(Effect.succeed(ws)));
      ws.once("error", (cause) => resume(Effect.fail(error(cause))));
      return Effect.sync(() => {
        if (ws.readyState !== NodeWS.WebSocket.OPEN) ws.terminate();
      });
    }),
    (ws) => Effect.sync(() => ws.terminate()),
  );
  const incoming = yield* Queue.unbounded<string>();
  const requests = new Map<
    string,
    Deferred.Deferred<Record<string, unknown>, ProviderAdapterProtocolError>
  >();
  let requestOrdinal = 0;
  let nativeThreadId = options.resumeId ?? "";
  let nativeTurnId = "";
  let cli: NodeWS.WebSocket | undefined;
  const cliIds = new Map<string, string | number>();
  const cliTurnIds = new Set<string>();
  const approvalIds = new Set<string>();
  const send = (value: unknown) =>
    Effect.try({ try: () => upstream.send(json(value)), catch: error });
  const request = Effect.fn("terminal.codex.request")(function* (method: string, params: unknown) {
    const id = `t3:${++requestOrdinal}`;
    const response = yield* Deferred.make<Record<string, unknown>, ProviderAdapterProtocolError>();
    requests.set(id, response);
    return yield* send({ id, method, params }).pipe(
      Effect.andThen(Deferred.await(response)),
      Effect.timeout("30 seconds"),
      Effect.mapError(error),
      Effect.ensuring(
        Effect.sync(() => {
          requests.delete(id);
        }),
      ),
    );
  });
  const deltas = yield* makeProviderTextDeltaCoalescer({
    flushIntervalMs: 40,
    emit: (update) =>
      options.getRuntime()?.assistant(update.itemId, update.text, update.completed) ?? Effect.void,
  });
  const notify = Effect.fn("terminal.codex.notify")(function* (
    method: string,
    params: Record<string, unknown>,
  ) {
    const runtime = options.getRuntime();
    if (!runtime) return;
    if (params.threadId !== nativeThreadId) {
      // A sub-agent thread starting another turn is running again.
      if (method === "turn/started" && typeof params.threadId === "string")
        yield* runtime.subagent(params.threadId, { status: "running", reopen: true });
      return;
    }
    if (method === "turn/started") {
      nativeTurnId = text(record(params.turn).id);
      yield* runtime.setNativeTurn(nativeTurnId);
    } else if (method === "turn/completed") {
      const turn = record(params.turn);
      yield* deltas.flushTurn(text(turn.id));
      yield* runtime.finish(
        turn.status === "interrupted"
          ? "interrupted"
          : turn.status === "failed"
            ? "failed"
            : "completed",
        text(record(turn.error).message) || undefined,
      );
    } else if (method === "item/agentMessage/delta") {
      yield* deltas.append({
        turnId: text(params.turnId),
        itemId: text(params.itemId),
        delta: text(params.delta),
      });
    } else if (method === "item/started" || method === "item/completed") {
      const item = record(params.item);
      const completed = method === "item/completed";
      if (item.type === "agentMessage") {
        if (completed)
          yield* deltas.complete({
            turnId: text(params.turnId),
            itemId: text(item.id),
            finalText: text(item.text),
          });
      } else if (item.type === "collabAgentToolCall") {
        // Sub-agents are keyed by their Codex thread id across spawn and state updates.
        if (item.tool === "spawnAgent" && Array.isArray(item.receiverThreadIds)) {
          for (const child of item.receiverThreadIds.map(text).filter(Boolean))
            yield* runtime.subagent(child, { status: "running", prompt: text(item.prompt) });
        }
        for (const [child, value] of Object.entries(record(item.agentsStates))) {
          const state = record(value);
          const status = CODEX_AGENT_STATUSES[text(state.status)];
          if (status)
            yield* runtime.subagent(child, {
              status,
              ...(typeof state.message === "string" ? { result: state.message } : {}),
            });
        }
      } else if (item.type === "subAgentActivity") {
        const child = text(item.agentThreadId);
        if (item.kind === "started")
          yield* runtime.subagent(child, {
            status: "running",
            title: text(item.agentPath) || null,
          });
        else if (item.kind === "completed" || item.kind === "interrupted")
          yield* runtime.subagent(child, { status: item.kind });
      } else if (item.type !== "userMessage") {
        yield* runtime.tool(
          text(item.id),
          text(item.tool) || text(item.type),
          completed ? item : (item.arguments ?? item),
          completed,
          item.status === "failed",
        );
      }
    }
  });
  upstream.on("message", (data) => {
    Queue.offerUnsafe(incoming, data.toString());
  });
  upstream.on("close", () => {
    for (const deferred of requests.values())
      Deferred.doneUnsafe(deferred, Exit.fail(error("Codex app-server disconnected.")));
    cli?.close();
    Queue.offerUnsafe(incoming, json({ method: "t3/disconnected" }));
  });
  upstream.on("error", () => {});
  yield* Stream.fromQueue(incoming).pipe(
    Stream.runForEach((raw) =>
      Effect.gen(function* () {
        const frame = yield* parse(raw);
        const id = String(frame.id);
        if (typeof frame.method === "string") {
          if (frame.method === "t3/disconnected") {
            yield* options.getRuntime()?.finish("interrupted") ?? Effect.void;
            return;
          }
          if (frame.id !== undefined) {
            approvalIds.add(id);
            yield* (
              options
                .getRuntime()
                ?.approval(
                  id,
                  text(record(frame.params).reason) || "Codex needs input in the terminal",
                  false,
                ) ?? Effect.void
            );
          } else {
            yield* notify(frame.method, record(frame.params));
          }
          cli?.send(raw);
        } else if (requests.has(id)) {
          const deferred = requests.get(id)!;
          yield* frame.error
            ? Deferred.fail(deferred, error(record(frame.error).message))
            : Deferred.succeed(deferred, record(frame.result));
        } else if (cliIds.has(id)) {
          if (cliTurnIds.delete(id) && frame.error)
            yield* (
              options.getRuntime()?.finish("failed", text(record(frame.error).message)) ??
                Effect.void
            );
          cli?.send(json({ ...frame, id: cliIds.get(id) }));
          cliIds.delete(id);
        }
      }).pipe(Effect.catch((cause) => Effect.logWarning("native-codex-observation-failed", cause))),
    ),
    Effect.forkScoped,
  );
  const initialization = yield* request("initialize", buildCodexInitializeParams());
  yield* send({ method: "initialized" });
  const thread = yield* request(options.resumeId ? "thread/resume" : "thread/start", {
    ...options.threadParams,
    ...(options.resumeId ? { threadId: options.resumeId } : {}),
    persistExtendedHistory: true,
  });
  nativeThreadId = text(record(thread.thread).id);
  if (!nativeThreadId) return yield* error("Codex did not return a conversation id.");
  const token = NodeCrypto.randomUUID();
  const server = yield* Effect.acquireRelease(
    Effect.callback<NodeWS.WebSocketServer, ProviderAdapterProtocolError>((resume) => {
      const ws = new NodeWS.WebSocketServer({
        host: "127.0.0.1",
        port: 0,
        maxPayload: 16 * 1024 * 1024,
        verifyClient: (info: { req: { headers: { authorization?: string | undefined } } }) =>
          info.req.headers.authorization === `Bearer ${token}`,
      });
      ws.once("listening", () => resume(Effect.succeed(ws)));
      ws.once("error", (cause) => resume(Effect.fail(error(cause))));
    }),
    (ws) =>
      Effect.sync(() => {
        for (const client of ws.clients) client.terminate();
        ws.close();
      }),
  );
  const context = yield* Effect.context<never>();
  let activated = false;
  server.on("connection", (client) => {
    if (cli?.readyState === NodeWS.WebSocket.OPEN) {
      client.close(1008, "This terminal already has a client.");
      return;
    }
    cli = client;
    client.on("error", () => {});
    client.on("message", (data) => {
      Effect.runForkWith(context)(
        Effect.gen(function* () {
          const frame = yield* parse(data.toString());
          const params = record(frame.params);
          const method = text(frame.method);
          const reject = (message: string) =>
            Effect.sync(() =>
              client.send(json({ id: frame.id, error: { code: -32000, message } })),
            );
          if (!method) {
            if (approvalIds.delete(String(frame.id)))
              yield* (
                options.getRuntime()?.approval(String(frame.id), "Codex terminal request", true) ??
                  Effect.void
              );
            yield* send(frame);
            return;
          }
          if (method === "initialize") {
            client.send(json({ id: frame.id, result: initialization }));
            return;
          }
          if (method === "initialized") return;
          if (method === "thread/start") {
            if (activated) {
              yield* reject("Return to Chat to create or switch conversations.");
              return;
            }
            activated = true;
            client.send(json({ id: frame.id, result: thread }));
            return;
          } else if (
            method === "thread/resume" &&
            params.threadId === nativeThreadId &&
            !activated
          ) {
            activated = true;
            client.send(json({ id: frame.id, result: thread }));
            return;
          } else if (
            /^thread\/(fork|rollback|revert|archive|unarchive)$/.test(method) ||
            (params.threadId !== undefined && params.threadId !== nativeThreadId)
          ) {
            yield* reject(
              "This terminal is bound to its T3 conversation. Return to Chat to switch or restore it.",
            );
            return;
          }
          if (method === "turn/start" || method === "thread/compact/start") {
            const prompt =
              method === "thread/compact/start" ? "/compact" : nativeCodexPrompt(params);
            const prepared = yield* options.submit(prompt).pipe(Effect.result);
            if (prepared._tag === "Failure") {
              yield* reject("T3 could not prepare this turn. Check the thread status and retry.");
              return;
            }
            if (method === "turn/start") {
              const input =
                prepared.success.message.text !== prompt
                  ? [
                      { type: "text", text: prepared.success.message.text, text_elements: [] },
                      ...(Array.isArray(params.input)
                        ? params.input.filter((item) => record(item).type !== "text")
                        : []),
                    ]
                  : params.input;
              const mcp = yield* options.readMcpSession(prepared.success.threadId);
              const runtimeParams = yield* Effect.gen(function* () {
                return yield* buildCodexTurnStartParams({
                  nativeThreadId,
                  codexInput: yield* decodeUserInput(input),
                  runtimePolicy: prepared.success.runtimePolicy,
                  modelSelection: prepared.success.modelSelection,
                  hasT3Mcp: mcp !== undefined,
                  browserToolsAvailable: mcp?.browserToolsAvailable ?? true,
                  deviceToolsAvailable: mcp?.capabilities?.has("device") ?? false,
                  ...(options.omitServiceTier ? { omitServiceTier: true } : {}),
                });
              }).pipe(Effect.result);
              if (runtimeParams._tag === "Failure") {
                yield* (
                  options.getRuntime()?.finish("failed", "Invalid native turn settings.") ??
                    Effect.void
                );
                yield* reject(
                  "T3 could not apply the turn settings. Return to Chat and check them.",
                );
                return;
              }
              frame.params = {
                ...params,
                ...runtimeParams.success,
              };
            }
          } else if (method === "turn/steer") {
            yield* reject("Wait for this turn to finish before submitting another message.");
            return;
          }
          if (frame.id !== undefined) {
            const mapped = `cli:${NodeCrypto.randomUUID()}`;
            cliIds.set(mapped, frame.id as string | number);
            if (method === "turn/start" || method === "thread/compact/start")
              cliTurnIds.add(mapped);
            frame.id = mapped;
          }
          yield* send(frame);
        }).pipe(
          Effect.catch((cause) => Effect.logWarning("native-codex-cli-request-failed", cause)),
          Effect.forkIn(scope),
          Effect.asVoid,
        ),
      );
    });
  });
  const bound = server.address();
  if (typeof bound === "string" || bound === null)
    return yield* error("No native Codex bridge address.");
  return {
    nativeThreadId,
    token,
    url: `ws://127.0.0.1:${bound.port}`,
    interrupt: Effect.suspend(() =>
      request("turn/interrupt", { threadId: nativeThreadId, turnId: nativeTurnId }),
    ).pipe(Effect.asVoid),
  };
});
