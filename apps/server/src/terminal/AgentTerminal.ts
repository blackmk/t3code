// @effect-diagnostics nodeBuiltinImport:off
import {
  AGENT_TERMINAL_ID,
  ClaudeSettings,
  CodexSettings,
  CommandId,
  MessageId,
  NativeTerminalError,
  ThreadId,
  type TerminalAttachInput,
  type TerminalAttachStreamEvent,
  type TerminalOpenInput,
  type TerminalCloseInput,
  type TerminalError,
  type TerminalSessionSnapshot,
} from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { resolveSpawnCommand, SpawnExecutableResolution } from "@t3tools/shared/shell";
import { tokenizeCliArgs } from "@t3tools/shared/cliArgs";
import { getModelSelectionStringOptionValue } from "@t3tools/shared/model";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";
import * as NodeCrypto from "node:crypto";
import * as ServerConfig from "../config.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as ProviderInstances from "../provider/ProviderInstanceRegistry.ts";
import { deriveProviderInstanceConfigMap } from "../provider/ProviderInstanceRegistryHydration.ts";
import { expandHomePath } from "../os-jank.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import { withAgentDeviceEnvironment } from "@t3tools/provider-core/server/mcpSession";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as Projections from "../orchestration-v2/ProjectionStore.ts";
import * as ProviderSessions from "../orchestration-v2/ProviderSessionManager.ts";
import * as ProviderEvents from "../orchestration-v2/ProviderEventIngestor.ts";
import * as RuntimePolicy from "../orchestration-v2/RuntimePolicy.ts";
import {
  ProviderAdapterOpenSessionError,
  ProviderAdapterProtocolError,
  ProviderAdapterTurnStartError,
  type ProviderAdapterV2TurnInput,
  type ProviderAdapterV2OpenSessionInput,
} from "@t3tools/provider-core/server/ProviderAdapter";
import * as Registry from "./NativeTerminalRegistry.ts";
import * as Manager from "./Manager.ts";
import { makeNativeRuntime, type NativeRuntime } from "./NativeRuntime.ts";
import { terminalOutputStream } from "./OutputProtocol.ts";
import { makeNativeCodex } from "./NativeCodex.ts";
import {
  codexThreadRuntimeParams,
  codexRuntimeModeTurnDefaults,
} from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import { makeNativeClaude } from "./NativeClaude.ts";
import { resolveCodexLaunchArgs } from "../provider/codexLaunchArgs.ts";
import {
  resolveCodexHomeLayout,
  materializeCodexShadowHome,
} from "../provider/Drivers/CodexHomeLayout.ts";

export class AgentTerminal extends Context.Service<
  AgentTerminal,
  {
    readonly open: (
      input: TerminalOpenInput,
    ) => Effect.Effect<TerminalSessionSnapshot, TerminalError>;
    readonly attachOutputStream: Manager.TerminalManager["Service"]["attachOutputStream"];
    readonly close: (input: TerminalCloseInput) => Effect.Effect<void, TerminalError>;
  }
>()("t3/terminal/AgentTerminal") {}

const isNativeTerminalError = Schema.is(NativeTerminalError);

const LIVE_RUN_STATUSES = new Set(["preparing", "starting", "running", "waiting", "queued"]);

/**
 * A native completion or interrupt reaches the projections asynchronously, so
 * a prompt typed right after it waits for that run to settle instead of being
 * rejected as concurrent work. Returns at once when the run already settled.
 */
export const awaitNativeRunSettled = Effect.fn("terminal.agent.awaitRunSettled")(function* (input: {
  readonly engine: Orchestrator.OrchestratorV2Shape;
  readonly threadId: ThreadId;
  readonly runId: ProviderAdapterV2TurnInput["runId"];
}) {
  const { engine, threadId, runId } = input;
  const sequence = yield* engine.getThreadEventSequence(threadId);
  const { runs } = yield* engine.getThreadRecords(threadId, ["runs"]);
  const run = runs.find((candidate) => candidate.id === runId);
  if (!run || !LIVE_RUN_STATUSES.has(run.status)) return;
  yield* engine.streamStoredEventsFrom({ threadId, afterSequence: sequence }).pipe(
    Stream.filter(
      ({ event }) =>
        event.type === "run.updated" &&
        event.payload.id === runId &&
        !LIVE_RUN_STATUSES.has(event.payload.status),
    ),
    Stream.take(1),
    Stream.runDrain,
    Effect.timeout("60 seconds"),
    Effect.ignore,
  );
});
const decodeCodexSettings = Schema.decodeUnknownEffect(CodexSettings);
const decodeClaudeSettings = Schema.decodeUnknownEffect(ClaudeSettings);
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

export const layer = Layer.effect(
  AgentTerminal,
  Effect.gen(function* () {
    const manager = yield* Manager.TerminalManager;
    const registry = yield* Registry.NativeTerminalRegistry;
    const engine = yield* Orchestrator.OrchestratorV2;
    const projections = yield* Projections.ProjectionStoreV2;
    const providerSessions = yield* ProviderSessions.ProviderSessionManagerV2;
    const providerEvents = yield* ProviderEvents.ProviderEventIngestorV2;
    const runtimePolicies = yield* RuntimePolicy.RuntimePolicyV2;
    const ids = yield* IdAllocator.IdAllocatorV2;
    const instances = yield* ProviderInstances.ProviderInstanceRegistry;
    const settingsService = yield* ServerSettings.ServerSettingsService;
    const config = yield* ServerConfig.ServerConfig;
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
    const hostEnvironment = yield* HostProcess.Environment;
    const platform = yield* HostProcess.Platform;
    const mcpSessions = yield* McpProviderSessions.McpProviderSessions;
    const resolveExecutable = yield* SpawnExecutableResolution;
    const scope = yield* Effect.scope;
    const lifecycle = yield* Semaphore.make(1);
    type Entry = {
      runtime: NativeRuntime;
      sessionId: import("@t3tools/contracts").ProviderSessionId;
      process: Manager.TerminalProcess;
      env: Record<string, string>;
      cwd: string;
      worktreePath: string | null;
    };
    const entries = new Map<ThreadId, Entry>();
    const fail = (threadId: string, detail: string, cause?: unknown) =>
      new NativeTerminalError({ threadId, detail, ...(cause === undefined ? {} : { cause }) });
    const mapError = (threadId: string, detail: string) =>
      Effect.mapError((cause: unknown) =>
        isNativeTerminalError(cause) ? cause : fail(threadId, detail, cause),
      );
    const resolveTerminalProcess = Effect.fn("terminal.agent.resolveProcess")(function* (
      command: string,
      args: ReadonlyArray<string>,
      env: NodeJS.ProcessEnv,
    ) {
      const launch = yield* resolveSpawnCommand(command, args, { env });
      if (!launch.shell) return { command: launch.command, args: launch.args };
      const executable = resolveExecutable(command, platform, env) ?? command;
      const quote = (value: string) => `'${value.replaceAll("'", "''")}'`;
      return {
        command: "powershell.exe",
        args: [
          "-NoLogo",
          "-NoProfile",
          "-NonInteractive",
          "-Command",
          `& ${[executable, ...args].map(quote).join(" ")}; exit $LASTEXITCODE`,
        ],
      };
    });

    const ensureIdle = Effect.fn("terminal.agent.ensureIdle")(function* (threadId: ThreadId) {
      const sequence = yield* engine.getThreadEventSequence(threadId);
      const { runs, checkpointScopes } = yield* projections.getCheckpointContext(threadId);
      const waiting = runs.find((run) => run.status === "waiting");
      const scope =
        waiting &&
        checkpointScopes.find((scope) => scope.runId === waiting.id && scope.kind === "root_run");
      const root =
        scope && waiting
          ? (yield* projections.getCheckpointCaptureContext(threadId, {
              runId: waiting.id,
              scopeId: scope.id,
            })).rootNode
          : undefined;
      const capturing = root?.status === "completed" ? waiting : undefined;
      if (
        runs.some(
          (run) =>
            run !== capturing &&
            ["preparing", "starting", "running", "waiting", "queued"].includes(run.status),
        )
      )
        return yield* fail(
          threadId,
          "Finish the current turn and queued messages before switching views.",
        );
      if (capturing) {
        yield* engine.streamStoredEventsFrom({ threadId, afterSequence: sequence }).pipe(
          Stream.filter(
            ({ event }) =>
              event.type === "run.updated" &&
              event.payload.id === capturing.id &&
              !["preparing", "starting", "running", "waiting", "queued"].includes(
                event.payload.status,
              ),
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.timeout("60 seconds"),
        );
      }
    });
    const awaitRunSettled = (threadId: ThreadId, runId: ProviderAdapterV2TurnInput["runId"]) =>
      awaitNativeRunSettled({ engine, threadId, runId });
    const release = Effect.fn("terminal.agent.release")(function* (threadId: ThreadId) {
      const entry = entries.get(threadId);
      if (!entry) return;
      const runId = entry.runtime.active?.input.runId;
      const sequence = runId ? yield* engine.getThreadEventSequence(threadId) : 0;
      yield* entry.runtime.finish("interrupted");
      // Background subagents of an earlier turn die with the native process.
      yield* entry.runtime.settleSubagents("interrupted");
      if (entry.runtime.providerThread.firstRunOrdinal === null) {
        const providerThread = entry.runtime.providerThread;
        yield* providerEvents.ingestNormalized({
          providerSessionId: entry.sessionId,
          providerInstanceId: providerThread.providerInstanceId!,
          threadId,
          event: {
            type: "provider_thread.updated",
            driver: providerThread.driver,
            providerThread: {
              ...providerThread,
              nativeThreadRef: null,
              nativeConversationHeadRef: null,
            },
          },
        });
      }
      if (runId)
        yield* engine.streamStoredEventsFrom({ threadId, afterSequence: sequence }).pipe(
          Stream.filter(
            ({ event }) =>
              event.type === "run.updated" &&
              event.payload.id === runId &&
              !["preparing", "starting", "running"].includes(event.payload.status),
          ),
          Stream.take(1),
          Stream.runDrain,
          Effect.timeout("10 seconds"),
          Effect.ignore,
        );
      yield* registry.unregister(threadId);
      entries.delete(threadId);
      yield* providerSessions.close(entry.sessionId);
    });
    yield* Effect.acquireRelease(
      manager.subscribe((event) =>
        event.terminalId === AGENT_TERMINAL_ID &&
        (event.type === "exited" || event.type === "closed")
          ? lifecycle
              .withPermits(1)(release(ThreadId.make(event.threadId)))
              .pipe(Effect.ignore, Effect.forkIn(scope), Effect.asVoid)
          : Effect.void,
      ),
      (unsubscribe) => Effect.sync(unsubscribe),
    );

    const open = Effect.fn("terminal.agent.open")(function* (input: TerminalOpenInput) {
      const threadId = ThreadId.make(input.threadId);
      const existing = entries.get(threadId);
      if (existing)
        return yield* manager.open({
          threadId,
          terminalId: AGENT_TERMINAL_ID,
          cwd: existing.cwd,
          worktreePath: existing.worktreePath,
          cols: input.cols,
          rows: input.rows,
          env: existing.env,
          process: existing.process,
        });
      const thread = yield* projections.getThread(threadId);
      if (thread.archivedAt !== null)
        return yield* fail(threadId, "Reopen this thread before starting its terminal.");
      yield* ensureIdle(threadId);
      const instance = yield* instances.getInstance(thread.modelSelection.instanceId);
      if (!instance || !["codex", "claudeAgent"].includes(instance.driverKind))
        return yield* fail(threadId, "Terminal view is available for Codex and Claude Code.");
      const driver = instance.driverKind;
      const originalCapabilities = yield* instance.orchestrationAdapter.getCapabilities();
      const capabilities = {
        ...originalCapabilities,
        sessions: {
          ...originalCapabilities.sessions,
          supportsMultipleProviderThreadsPerSession: false,
        },
        threads: {
          ...originalCapabilities.threads,
          canForkThread: false,
          canRollbackThread: false,
          canReadThreadSnapshot: false,
        },
        turns: { ...originalCapabilities.turns, supportsActiveSteering: false },
      };
      const policy = yield* runtimePolicies.resolve({
        thread,
        modelSelection: thread.modelSelection,
      });
      if (!policy.cwd) return yield* fail(threadId, "This thread has no workspace.");
      const settings = yield* settingsService.getSettings;
      const instanceConfig =
        deriveProviderInstanceConfigMap(settings)[thread.modelSelection.instanceId];
      const baseEnv = yield* Manager.resolveProviderInstanceTerminalEnvironment({
        deriveProviderInstanceConfigMap,
        serverSettings: settingsService,
        path,
        rawProviderInstanceId: thread.modelSelection.instanceId,
        env: undefined,
      });
      const { providerThreads } = yield* projections.getThreadRecords(threadId, [
        "providerThreads",
      ]);
      const previous =
        providerThreads.find(
          (candidate) =>
            candidate.id === thread.activeProviderThreadId &&
            candidate.providerInstanceId === thread.modelSelection.instanceId,
        ) ??
        providerThreads.findLast(
          (candidate) =>
            candidate.providerInstanceId === thread.modelSelection.instanceId &&
            candidate.ownerNodeId === null &&
            candidate.nativeThreadRef?.strength === "strong",
        );
      const sessionId = yield* ids.allocate.providerSession({
        providerInstanceId: thread.modelSelection.instanceId,
        threadId,
      });
      const promptLock = yield* Semaphore.make(1);
      const pending = new Map<MessageId, Deferred.Deferred<ProviderAdapterV2TurnInput>>();
      let nativeRuntime: NativeRuntime | undefined;
      let launchProcess: Manager.TerminalProcess | undefined;
      let launchEnv = baseEnv;
      const submit = (text: string) =>
        promptLock
          .withPermits(1)(
            Effect.gen(function* () {
              const previousRunId = nativeRuntime?.lastRunId;
              if (previousRunId) yield* awaitRunSettled(threadId, previousRunId);
              yield* ensureIdle(threadId);
              if (!registry.owns(threadId))
                return yield* fail(threadId, "This terminal has stopped. Restart it to continue.");
              const commandId = CommandId.make(NodeCrypto.randomUUID());
              const messageId = MessageId.make(NodeCrypto.randomUUID());
              const ready = yield* Deferred.make<ProviderAdapterV2TurnInput>();
              pending.set(messageId, ready);
              const result = yield* registry.withInputCommand(
                threadId,
                commandId,
                engine.dispatch({
                  type: "message.dispatch",
                  commandId,
                  threadId,
                  messageId,
                  text,
                  attachments: [],
                  createdBy: "user",
                  creationSource: "provider",
                  dispatchMode: { type: "start_immediately" },
                }),
              );
              const failed = engine
                .streamStoredEventsFrom({ threadId, afterSequence: result.sequence })
                .pipe(
                  Stream.filter(
                    ({ event }) =>
                      event.type === "run.updated" &&
                      event.payload.userMessageId === messageId &&
                      ["failed", "cancelled", "interrupted"].includes(event.payload.status),
                  ),
                  Stream.take(1),
                  Stream.runDrain,
                  Effect.andThen(Effect.fail(fail(threadId, "T3 could not prepare this turn."))),
                );
              return yield* Deferred.await(ready).pipe(
                Effect.raceFirst(failed),
                Effect.timeout("60 seconds"),
                Effect.ensuring(
                  Effect.sync(() => {
                    pending.delete(messageId);
                  }),
                ),
              );
            }),
          )
          .pipe(mapError(threadId, "Could not prepare the native turn."));
      const prepare = Effect.fn("terminal.agent.prepare")(function* (
        turnInput: ProviderAdapterV2TurnInput,
      ) {
        const ready = pending.get(turnInput.message.messageId);
        if (!ready)
          return yield* new ProviderAdapterTurnStartError({
            driver,
            threadId,
            providerThreadId: turnInput.providerThread.id,
            runId: turnInput.runId,
            cause: "The native prompt was cancelled before its workspace was ready.",
          });
        yield* Deferred.succeed(ready, turnInput);
      });
      const openNative: Parameters<typeof registry.register>[2] = Effect.fn(
        "terminal.agent.openSession",
      )(
        function* (sessionInput: ProviderAdapterV2OpenSessionInput) {
          // The CLI's hooks and bridge live in this session's scope, so a
          // session closed for any reason must not leave the CLI running.
          yield* Effect.addFinalizer(() =>
            manager.close({ threadId, terminalId: AGENT_TERMINAL_ID }).pipe(Effect.ignore),
          );
          const mcp = yield* mcpSessions.read(threadId);
          const env = withAgentDeviceEnvironment({ ...hostEnvironment, ...baseEnv }, mcp);
          if (driver === "codex") {
            const effective = instance.nativeCodexRuntime
              ? yield* instance.nativeCodexRuntime
              : undefined;
            const codexSettings =
              effective?.config ?? (yield* decodeCodexSettings(instanceConfig?.config ?? {}));
            const codexBinary = yield* expandHomePath(codexSettings.binaryPath || "codex").pipe(
              Effect.provideService(Path.Path, path),
            );
            const homeLayout = yield* resolveCodexHomeLayout(codexSettings).pipe(
              Effect.provideService(Path.Path, path),
            );
            yield* materializeCodexShadowHome(homeLayout).pipe(
              Effect.provideService(FileSystem.FileSystem, fs),
              Effect.provideService(Path.Path, path),
            );
            const codexEnv = {
              ...(effective?.environment ?? env),
              ...(homeLayout.effectiveHomePath ? { CODEX_HOME: homeLayout.effectiveHomePath } : {}),
            };
            const args = tokenizeCliArgs(
              resolveCodexLaunchArgs(codexSettings.launchArgs, effective?.environment ?? env),
            );
            const runtimeDefaults = codexRuntimeModeTurnDefaults(policy.runtimeMode);
            const sandbox =
              runtimeDefaults.sandboxPolicy.type === "dangerFullAccess"
                ? "danger-full-access"
                : runtimeDefaults.sandboxPolicy.type === "readOnly"
                  ? "read-only"
                  : "workspace-write";
            const effort = getModelSelectionStringOptionValue(
              thread.modelSelection,
              "reasoningEffort",
            );
            const resumeId =
              previous?.firstRunOrdinal != null ? previous.nativeThreadRef?.nativeId : undefined;
            const bridge = yield* makeNativeCodex({
              command: codexBinary,
              args,
              env: codexEnv,
              cwd: policy.cwd!,
              omitServiceTier: instance.nativeCodexRuntime !== undefined,
              ...(resumeId ? { resumeId } : {}),
              threadParams: {
                ...codexThreadRuntimeParams({
                  threadId,
                  mcpSession: mcp,
                  runtimePolicy: policy,
                  modelSelection: thread.modelSelection,
                }),
                approvalPolicy: policy.approvalPolicy ?? runtimeDefaults.approvalPolicy,
                approvalsReviewer: runtimeDefaults.approvalsReviewer,
                sandbox,
              },
              submit,
              getRuntime: () => nativeRuntime,
              readMcpSession: mcpSessions.read,
            }).pipe(Effect.provideService(ChildProcessSpawner.ChildProcessSpawner, spawner));
            nativeRuntime = yield* makeNativeRuntime({
              instanceId: thread.modelSelection.instanceId,
              driver,
              input: sessionInput,
              nativeThreadId: bridge.nativeThreadId,
              ...(previous ? { existingThread: previous } : {}),
              capabilities,
              ids,
              prepare,
              interrupt: bridge.interrupt,
            });
            launchEnv = {
              ...baseEnv,
              ...Object.fromEntries(
                Object.entries(codexEnv).filter(
                  (entry): entry is [string, string] => entry[1] !== undefined,
                ),
              ),
              T3_NATIVE_CODEX_TOKEN: bridge.token,
            };
            launchProcess = yield* resolveTerminalProcess(
              codexBinary,
              [
                ...args,
                // Remote resume loads permissions from the existing thread and rejects overrides.
                ...(resumeId
                  ? []
                  : [
                      "--model",
                      thread.modelSelection.model,
                      "--sandbox",
                      sandbox,
                      ...(typeof runtimeDefaults.approvalPolicy === "string"
                        ? ["--ask-for-approval", runtimeDefaults.approvalPolicy]
                        : []),
                      "-c",
                      `approvals_reviewer="${runtimeDefaults.approvalsReviewer}"`,
                      ...(effort ? ["-c", `model_reasoning_effort="${effort}"`] : []),
                    ]),
                "--no-alt-screen",
                "--remote",
                bridge.url,
                "--remote-auth-token-env",
                "T3_NATIVE_CODEX_TOKEN",
                ...(resumeId ? ["resume", bridge.nativeThreadId] : []),
              ],
              launchEnv,
            );
          } else {
            const claudeSettings = yield* decodeClaudeSettings(instanceConfig?.config ?? {});
            const nativeThreadId = previous?.nativeThreadRef?.nativeId ?? NodeCrypto.randomUUID();
            nativeRuntime = yield* makeNativeRuntime({
              instanceId: thread.modelSelection.instanceId,
              driver,
              input: sessionInput,
              nativeThreadId,
              ...(previous ? { existingThread: previous } : {}),
              capabilities,
              ids,
              prepare,
              interrupt: manager
                .write({ threadId, terminalId: AGENT_TERMINAL_ID, data: "\u0003" })
                .pipe(
                  Effect.mapError(
                    (cause) =>
                      new ProviderAdapterProtocolError({
                        driver,
                        detail: "Could not interrupt the Claude terminal.",
                        cause,
                      }),
                  ),
                ),
            });
            const hooks = yield* makeNativeClaude({
              stateDir: config.stateDir,
              nativeThreadId,
              platform,
              executable: process.execPath,
              submit,
              getRuntime: () => nativeRuntime,
            }).pipe(
              Effect.provideService(FileSystem.FileSystem, fs),
              Effect.provideService(Path.Path, path),
            );
            const permission =
              thread.interactionMode === "plan"
                ? "plan"
                : policy.runtimeMode === "full-access"
                  ? "bypassPermissions"
                  : policy.runtimeMode === "auto-accept-edits"
                    ? "acceptEdits"
                    : policy.runtimeMode === "auto"
                      ? "auto"
                      : "default";
            const args = [
              ...tokenizeCliArgs(claudeSettings.launchArgs),
              previous?.firstRunOrdinal != null ? "--resume" : "--session-id",
              nativeThreadId,
              "--model",
              thread.modelSelection.model,
              "--permission-mode",
              permission,
              "--settings",
              hooks.settings,
              ...(mcp
                ? [
                    "--mcp-config",
                    yield* encodeJson({
                      mcpServers: {
                        t3: {
                          type: "http",
                          url: mcp.endpoint,
                          headers: { Authorization: mcp.authorizationHeader },
                        },
                      },
                    }),
                  ]
                : []),
            ];
            launchEnv = Object.fromEntries(
              Object.entries(env).filter(
                (entry): entry is [string, string] => entry[1] !== undefined,
              ),
            );
            launchProcess = yield* resolveTerminalProcess(
              yield* expandHomePath(claudeSettings.binaryPath || "claude").pipe(
                Effect.provideService(Path.Path, path),
              ),
              args,
              launchEnv,
            );
          }
          return nativeRuntime.runtime;
        },
        Effect.mapError(
          (cause) =>
            new ProviderAdapterOpenSessionError({ driver, providerSessionId: sessionId, cause }),
        ),
      );
      return yield* Effect.gen(function* () {
        yield* registry.register(threadId, sessionId, openNative);
        if (previous?.providerSessionId)
          yield* providerSessions.detach({
            providerSessionId: previous.providerSessionId,
            threadId,
          });
        yield* providerSessions.open({
          threadId,
          providerSessionId: sessionId,
          modelSelection: thread.modelSelection,
          runtimePolicy: policy,
        });
        if (!nativeRuntime || !launchProcess)
          return yield* fail(threadId, "The native terminal did not start.");
        yield* providerEvents.ingestNormalized({
          providerSessionId: sessionId,
          providerInstanceId: thread.modelSelection.instanceId,
          threadId,
          event: {
            type: "provider_thread.updated",
            driver,
            providerThread: nativeRuntime.providerThread,
          },
        });
        entries.set(threadId, {
          runtime: nativeRuntime,
          sessionId,
          process: launchProcess,
          env: launchEnv,
          cwd: policy.cwd!,
          worktreePath: thread.worktreePath,
        });
        return yield* manager.open({
          threadId,
          terminalId: AGENT_TERMINAL_ID,
          cwd: policy.cwd!,
          worktreePath: thread.worktreePath,
          cols: input.cols,
          rows: input.rows,
          env: launchEnv,
          process: launchProcess,
        });
      }).pipe(
        // Interruption too: a dropped RPC must not leave the thread owned.
        Effect.onExit((exit) =>
          Exit.isSuccess(exit)
            ? Effect.void
            : Effect.gen(function* () {
                entries.delete(threadId);
                yield* registry.unregister(threadId);
                yield* providerSessions.close(sessionId).pipe(Effect.ignore);
              }),
        ),
      );
    });
    const close = Effect.fn("terminal.agent.close")(function* (input: TerminalCloseInput) {
      const threadId = ThreadId.make(input.threadId);
      const entry = entries.get(threadId);
      // Without a native session there is no turn to hand back to Chat.
      if (input.returnToChat && entry) {
        const lastRunId = entry.runtime.lastRunId;
        if (lastRunId && !entry.runtime.active) yield* awaitRunSettled(threadId, lastRunId);
        yield* ensureIdle(threadId);
      }
      const active = entry?.runtime.active;
      if (active && entry.runtime.runtime.driver === "codex")
        yield* entry.runtime.runtime.interruptTurn({
          providerThread: entry.runtime.providerThread,
          providerTurnId: active.turn.id,
        });
      yield* manager.close({ threadId, terminalId: AGENT_TERMINAL_ID });
      yield* release(threadId);
    });
    const attach = Effect.fn("AgentTerminal.attach")(function* (
      input: TerminalAttachInput,
      viewerId: string,
    ) {
      // Reconnecting after a close must not restart the native agent.
      if (input.resume && !entries.has(ThreadId.make(input.threadId))) {
        const resync: Parameters<
          typeof terminalOutputStream<TerminalAttachStreamEvent, TerminalError>
        >[0] = (listener, registerCleanup) =>
          manager.resyncStream(input, listener, registerCleanup);
        return terminalOutputStream(resync, resync);
      }
      yield* open({ ...input, cwd: input.cwd ?? "" });
      return manager.attachOutputStream(
        {
          threadId: input.threadId,
          terminalId: AGENT_TERMINAL_ID,
          ...(input.cols === undefined ? {} : { cols: input.cols }),
          ...(input.rows === undefined ? {} : { rows: input.rows }),
          ...(input.resume === undefined ? {} : { resume: input.resume }),
          ...(input.maxHistoryBytes === undefined
            ? {}
            : { maxHistoryBytes: input.maxHistoryBytes }),
        },
        viewerId,
      );
    });
    return AgentTerminal.of({
      open: Effect.fn("AgentTerminal.open")(function* (input) {
        return yield* lifecycle
          .withPermits(1)(open(input))
          .pipe(mapError(input.threadId, "Could not open the agent terminal."));
      }),
      attachOutputStream: (input, viewerId) =>
        Stream.unwrap(
          lifecycle
            .withPermits(1)(attach(input, viewerId))
            .pipe(mapError(input.threadId, "Could not attach to the agent terminal.")),
        ),
      close: Effect.fn("AgentTerminal.close")(function* (input) {
        yield* lifecycle
          .withPermits(1)(close(input))
          .pipe(mapError(input.threadId, "Could not close the agent terminal."));
      }),
    });
  }),
);
