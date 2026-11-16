import {
  PositiveInt,
  isOrchestrationV2WorkActive,
  type NodeId,
  type OrchestrationV2ProviderCapabilities,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2Subagent,
  type OrchestrationV2TurnItem,
  type ProviderDriverKind,
  type ProviderInstanceId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import type * as Cause from "effect/Cause";
import type { IdAllocatorV2 } from "@t3tools/provider-core/server/IdAllocator";
import {
  makeSubagentChildThread,
  makeSubagentConversationArtifacts,
  subagentThreadTitle,
} from "@t3tools/provider-core/server/subagentProjection";
import {
  ProviderAdapterProtocolError,
  ProviderAdapterTurnStartError,
  type ProviderAdapterV2Event,
  type ProviderAdapterV2OpenSessionInput,
  type ProviderAdapterV2SessionRuntime,
  type ProviderAdapterV2TurnInput,
} from "@t3tools/provider-core/server/ProviderAdapter";

/** Converts native observations into the events consumed by ordinary V2 runs. */
export const makeNativeRuntime = Effect.fn("terminal.makeNativeRuntime")(function* (options: {
  readonly instanceId: ProviderInstanceId;
  readonly driver: ProviderDriverKind;
  readonly input: ProviderAdapterV2OpenSessionInput;
  readonly nativeThreadId: string;
  readonly existingThread?: OrchestrationV2ProviderThread;
  readonly capabilities: OrchestrationV2ProviderCapabilities;
  readonly ids: IdAllocatorV2["Service"];
  readonly prepare: (
    input: ProviderAdapterV2TurnInput,
  ) => Effect.Effect<void, ProviderAdapterTurnStartError>;
  readonly interrupt: Effect.Effect<void, ProviderAdapterProtocolError>;
}) {
  const { driver, ids, input } = options;
  const queue = yield* Queue.unbounded<ProviderAdapterV2Event, Cause.Done>();
  const now = yield* DateTime.now;
  let providerThread: OrchestrationV2ProviderThread = {
    ...(options.existingThread ?? {
      id: ids.derive.providerThread({
        driver,
        nativeThreadId: options.nativeThreadId,
        providerInstanceId: options.instanceId,
      }),
      ownerNodeId: null,
      firstRunOrdinal: null,
      lastRunOrdinal: null,
      handoffIds: [],
      forkedFrom: null,
      createdAt: now,
    }),
    driver,
    providerInstanceId: options.instanceId,
    providerSessionId: input.providerSessionId,
    appThreadId: input.threadId,
    nativeThreadRef: { driver, nativeId: options.nativeThreadId, strength: "strong" },
    nativeConversationHeadRef: options.existingThread?.nativeConversationHeadRef ?? null,
    status: "idle",
    updatedAt: now,
  };
  let active:
    | {
        input: ProviderAdapterV2TurnInput;
        turn: OrchestrationV2ProviderTurn;
        done: Deferred.Deferred<void>;
      }
    | undefined;
  let lastRunId: ProviderAdapterV2TurnInput["runId"] | undefined;
  let ordinal = 0;
  const items = new Map<string, OrchestrationV2TurnItem>();
  const requests = new Map<string, OrchestrationV2RuntimeRequest>();
  // Keyed by the native task id; entries outlive the turn so a background
  // subagent can still report its result after the parent turn ended.
  const subagents = new Map<
    string,
    {
      task: OrchestrationV2Subagent;
      rootNodeId: NodeId;
      providerTurnId: OrchestrationV2ProviderTurn["id"];
      childRootNodeId: NodeId;
      turnItemId: OrchestrationV2TurnItem["id"];
      turnItemOrdinal: OrchestrationV2TurnItem["ordinal"];
      nativeItemId: string;
    }
  >();
  const emit = (event: ProviderAdapterV2Event) => Queue.offer(queue, event).pipe(Effect.asVoid);
  const unsupported = (detail: string) =>
    Effect.fail(new ProviderAdapterProtocolError({ driver, detail }));
  const nativeRef = (nativeId: string) => ({ driver, nativeId, strength: "strong" as const });

  const begin = Effect.fn("terminal.native.begin")(function* (
    turnInput: ProviderAdapterV2TurnInput,
  ) {
    if (active)
      return yield* new ProviderAdapterTurnStartError({
        driver,
        threadId: input.threadId,
        providerThreadId: providerThread.id,
        runId: turnInput.runId,
        cause: "A native turn is already running.",
      });
    providerThread = {
      ...turnInput.providerThread,
      nativeThreadRef: providerThread.nativeThreadRef,
      providerSessionId: input.providerSessionId,
      status: "active",
      updatedAt: yield* DateTime.now,
    };
    const turn: OrchestrationV2ProviderTurn = {
      id: ids.derive.providerTurn({ driver, nativeTurnId: String(turnInput.runId) }),
      providerThreadId: providerThread.id,
      nodeId: turnInput.rootNodeId,
      runAttemptId: turnInput.attemptId,
      nativeTurnRef: { driver, nativeId: String(turnInput.runId), strength: "weak" },
      ordinal: PositiveInt.make(turnInput.providerTurnOrdinal),
      status: "running",
      startedAt: yield* DateTime.now,
      completedAt: null,
    };
    active = { input: turnInput, turn, done: yield* Deferred.make<void>() };
    lastRunId = turnInput.runId;
    ordinal = 0;
    items.clear();
    requests.clear();
    yield* options.prepare(turnInput).pipe(
      Effect.catch((cause) => {
        active = undefined;
        return Effect.fail(cause);
      }),
    );
    yield* emit({
      type: "provider_turn.updated",
      driver,
      threadId: input.threadId,
      providerTurn: turn,
    });
  });

  const baseItem = (
    nativeId: string,
    title: string,
    status: OrchestrationV2TurnItem["status"],
    at: DateTime.Utc,
  ) => {
    const context = active!;
    const previous = items.get(nativeId);
    return {
      id: ids.derive.turnItemFromProviderItem({
        driver,
        nativeItemId: `${context.input.runId}:${nativeId}`,
      }),
      threadId: input.threadId,
      runId: context.input.runId,
      nodeId: ids.derive.nodeFromProviderItem({
        driver,
        nativeItemId: `${context.input.runId}:${nativeId}`,
      }),
      providerThreadId: providerThread.id,
      providerTurnId: context.turn.id,
      nativeItemRef: nativeRef(nativeId),
      parentItemId: null,
      ordinal: previous?.ordinal ?? PositiveInt.make(++ordinal),
      status,
      title,
      startedAt: previous?.startedAt ?? at,
      completedAt: status === "running" || status === "waiting" ? null : at,
      updatedAt: at,
    };
  };
  const publishItem = Effect.fn("terminal.native.item")(function* (item: OrchestrationV2TurnItem) {
    items.set(item.nativeItemRef?.nativeId ?? String(item.id), item);
    yield* emit({
      type: "node.updated",
      driver,
      node: {
        id: item.nodeId!,
        threadId: input.threadId,
        runId: active!.input.runId,
        parentNodeId: active!.input.rootNodeId,
        rootNodeId: active!.input.rootNodeId,
        kind:
          item.type === "assistant_message"
            ? "assistant_message"
            : item.type === "approval_request"
              ? "approval_request"
              : "tool_call",
        status: item.status === "pending" ? "waiting" : item.status,
        countsForRun: true,
        providerThreadId: providerThread.id,
        providerTurnId: active!.turn.id,
        nativeItemRef: item.nativeItemRef,
        runtimeRequestId: item.type === "approval_request" ? item.requestId : null,
        checkpointScopeId: null,
        startedAt: item.startedAt,
        completedAt: item.completedAt,
      },
    });
    yield* emit({ type: "turn_item.updated", driver, turnItem: item });
  });
  const tool = Effect.fn("terminal.native.tool")(function* (
    nativeId: string,
    name: string,
    value: unknown,
    completed: boolean,
    failed = false,
  ) {
    if (!active) return;
    const at = yield* DateTime.now;
    const previous = items.get(nativeId);
    yield* publishItem({
      ...baseItem(nativeId, name, completed ? (failed ? "failed" : "completed") : "running", at),
      type: "dynamic_tool",
      toolName: name || null,
      input: previous?.type === "dynamic_tool" ? previous.input : value,
      ...(completed ? { output: value } : {}),
    });
  });
  const publishAssistant = Effect.fn("terminal.native.assistantMessage")(function* (
    item: Extract<OrchestrationV2TurnItem, { type: "assistant_message" }>,
  ) {
    yield* emit({
      type: "message.updated",
      driver,
      message: {
        id: item.messageId,
        threadId: input.threadId,
        runId: item.runId,
        nodeId: item.nodeId,
        createdBy: "agent",
        creationSource: "provider",
        role: "assistant",
        text: item.text,
        attachments: [],
        streaming: item.streaming,
        createdAt: item.startedAt ?? item.updatedAt,
        updatedAt: item.updatedAt,
      },
    });
    yield* publishItem(item);
  });
  const assistant = Effect.fn("terminal.native.assistant")(function* (
    nativeId: string,
    text: string,
    completed: boolean,
  ) {
    if (!active || !text) return;
    const at = yield* DateTime.now;
    yield* publishAssistant({
      ...baseItem(nativeId, "Assistant", completed ? "completed" : "running", at),
      type: "assistant_message",
      messageId: ids.derive.messageFromProviderItem({
        driver,
        nativeItemId: `${active.input.runId}:${nativeId}`,
      }),
      text,
      streaming: !completed,
    });
  });
  const approval = Effect.fn("terminal.native.approval")(function* (
    nativeId: string,
    title: string,
    resolved: boolean,
  ) {
    if (!active) return;
    const prior = requests.get(nativeId);
    if (resolved && !prior) return;
    const at = yield* DateTime.now;
    const request = prior ?? {
      id: yield* ids.allocate.runtimeRequest({
        driver,
        providerTurnId: active.turn.id,
        nativeRequestId: nativeId,
      }),
      nodeId: baseItem(`approval:${nativeId}`, title, "waiting", at).nodeId,
      providerTurnId: active.turn.id,
      nativeRequestRef: nativeRef(nativeId),
      kind: "command" as const,
      status: "pending" as const,
      responseCapability: {
        type: "not_resumable" as const,
        reason: "Answer this request in the native terminal.",
      },
      createdAt: at,
      resolvedAt: null,
    };
    const updated = {
      ...request,
      status: resolved ? ("resolved" as const) : ("pending" as const),
      resolvedAt: resolved ? at : null,
    };
    requests.set(nativeId, updated);
    yield* emit({
      type: "runtime_request.updated",
      driver,
      threadId: input.threadId,
      runtimeRequest: updated,
    });
    yield* publishItem({
      ...baseItem(`approval:${nativeId}`, title, resolved ? "completed" : "waiting", at),
      type: "approval_request",
      requestId: request.id,
      requestKind: "command",
      prompt: title,
    });
  });
  /**
   * Mirrors a native subagent as a child thread under this conversation, the
   * same shape provider adapters emit in Chat. A task starts only inside a live
   * turn; later updates may arrive after that turn ended.
   */
  const subagent = Effect.fn("terminal.native.subagent")(function* (
    nativeId: string,
    update: {
      readonly status: OrchestrationV2Subagent["status"];
      readonly prompt?: string;
      readonly title?: string | null;
      readonly result?: string | null;
      /** The task began another run of its own, e.g. a reused Codex sub-agent. */
      readonly reopen?: boolean;
    },
  ) {
    const at = yield* DateTime.now;
    let entry = subagents.get(nativeId);
    const created = entry === undefined;
    if (!entry) {
      if (!active || !isOrchestrationV2WorkActive(update.status) || update.reopen) return;
      const context = active;
      const nativeItemId = `${context.input.runId}:agent:${nativeId}`;
      const nodeId = ids.derive.nodeFromProviderItem({ driver, nativeItemId });
      const childThreadId = ids.derive.threadFromProviderThread({
        driver,
        nativeThreadId: `${providerThread.id}:${nativeId}`,
      });
      const prompt = update.prompt ?? "";
      entry = {
        task: {
          id: nodeId,
          threadId: input.threadId,
          runId: context.input.runId,
          parentNodeId: context.input.rootNodeId,
          origin: "provider_native",
          createdBy: "agent",
          driver,
          providerInstanceId: options.instanceId,
          providerThreadId: null,
          childThreadId,
          nativeTaskRef: nativeRef(nativeId),
          prompt,
          title: update.title ?? null,
          model: null,
          status: update.status,
          result: null,
          startedAt: at,
          completedAt: null,
          updatedAt: at,
        },
        rootNodeId: context.input.rootNodeId,
        providerTurnId: context.turn.id,
        childRootNodeId: ids.derive.nodeFromProviderItem({
          driver,
          nativeItemId: `${nativeItemId}:thread-root`,
        }),
        turnItemId: ids.derive.turnItemFromProviderItem({
          driver,
          nativeItemId: `${nativeItemId}:subagent`,
        }),
        turnItemOrdinal: PositiveInt.make(++ordinal),
        nativeItemId,
      };
      subagents.set(nativeId, entry);
      yield* emit({
        type: "app_thread.created",
        driver,
        appThread: makeSubagentChildThread({
          parentThread: context.input.appThread,
          childThreadId,
          parentNodeId: nodeId,
          activeProviderThreadId: null,
          providerInstanceId: options.instanceId,
          modelSelection: context.input.modelSelection,
          title: subagentThreadTitle({
            parentTitle: context.input.appThread.title,
            prompt,
            title: update.title ?? null,
            ordinal: subagents.size,
          }),
          now: at,
          createdBy: "agent",
          creationSource: "provider",
        }),
      });
    } else if (isOrchestrationV2WorkActive(entry.task.status)) {
      if (update.reopen) return;
    } else if (isOrchestrationV2WorkActive(update.status)) {
      // Only an explicit new run reopens a settled task, never a late or
      // duplicate start. It moves to the live run, whose ingestion is open.
      if (!update.reopen || !active) return;
      entry.task = {
        ...entry.task,
        runId: active.input.runId,
        parentNodeId: active.input.rootNodeId,
        result: null,
        startedAt: at,
        completedAt: null,
      };
      entry.rootNodeId = active.input.rootNodeId;
      entry.providerTurnId = active.turn.id;
      entry.turnItemOrdinal = PositiveInt.make(++ordinal);
    }
    const current = entry;
    const previous = current.task;
    const settled = !isOrchestrationV2WorkActive(update.status);
    const task: OrchestrationV2Subagent = {
      ...previous,
      status: update.status,
      ...(update.result === undefined ? {} : { result: update.result }),
      completedAt: settled ? (previous.completedAt ?? at) : null,
      updatedAt: at,
    };
    current.task = task;
    const emitNodes = Effect.gen(function* () {
      yield* emit({
        type: "node.updated",
        driver,
        node: {
          id: task.id,
          threadId: task.threadId,
          runId: task.runId,
          parentNodeId: task.parentNodeId,
          rootNodeId: current.rootNodeId,
          kind: "subagent",
          status: task.status,
          countsForRun: false,
          providerThreadId: providerThread.id,
          providerTurnId: current.providerTurnId,
          nativeItemRef: task.nativeTaskRef,
          runtimeRequestId: null,
          checkpointScopeId: null,
          startedAt: task.startedAt,
          completedAt: task.completedAt,
        },
      });
      yield* emit({
        type: "node.updated",
        driver,
        node: {
          id: current.childRootNodeId,
          threadId: task.childThreadId!,
          runId: null,
          parentNodeId: null,
          rootNodeId: current.childRootNodeId,
          kind: "root_turn",
          status: task.status,
          countsForRun: false,
          providerThreadId: null,
          providerTurnId: null,
          nativeItemRef: task.nativeTaskRef,
          runtimeRequestId: null,
          checkpointScopeId: null,
          startedAt: task.startedAt,
          completedAt: task.completedAt,
        },
      });
    });
    const childMessage = (
      suffix: string,
      role: "user" | "assistant",
      text: string,
      order: number,
    ) =>
      makeSubagentConversationArtifacts({
        ...(role === "user" ? { senderThreadId: input.threadId } : {}),
        messageId: ids.derive.messageFromProviderItem({
          driver,
          nativeItemId: `${current.nativeItemId}:${suffix}`,
        }),
        turnItemId: ids.derive.turnItemFromProviderItem({
          driver,
          nativeItemId: `${current.nativeItemId}:${suffix}`,
        }),
        threadId: task.childThreadId!,
        rootNodeId: current.childRootNodeId,
        providerThreadId: null,
        providerTurnId: null,
        nativeItemRef: nativeRef(`${nativeId}:${suffix}`),
        role,
        text,
        ordinal: order,
        now: at,
      });
    const publishChildMessage = (artifacts: ReturnType<typeof childMessage>) =>
      emit({ type: "message.updated", driver, message: artifacts.message }).pipe(
        Effect.andThen(emit({ type: "turn_item.updated", driver, turnItem: artifacts.turnItem })),
      );
    if (created) {
      yield* emitNodes;
      if (task.prompt.trim().length > 0)
        yield* publishChildMessage(childMessage("prompt", "user", task.prompt, 100));
    }
    // The answer goes out before the settled lifecycle: a finished parent run
    // stops ingesting once its last subagent settles. A repeat update rewrites
    // the same message.
    if (settled && task.result !== null && task.result.trim().length > 0)
      yield* publishChildMessage(childMessage("result", "assistant", task.result, 101));
    if (!created && previous.status !== task.status) yield* emitNodes;
    yield* emit({ type: "subagent.updated", driver, subagent: task });
    yield* emit({
      type: "turn_item.updated",
      driver,
      turnItem: {
        id: current.turnItemId,
        threadId: task.threadId,
        runId: task.runId,
        nodeId: task.id,
        providerThreadId: providerThread.id,
        providerTurnId: current.providerTurnId,
        nativeItemRef: task.nativeTaskRef,
        parentItemId: null,
        ordinal: current.turnItemOrdinal,
        status: task.status,
        title: task.title,
        startedAt: task.startedAt,
        completedAt: task.completedAt,
        updatedAt: task.updatedAt,
        type: "subagent",
        subagentId: task.id,
        origin: task.origin,
        driver: task.driver,
        providerInstanceId: task.providerInstanceId,
        childThreadId: task.childThreadId,
        prompt: task.prompt,
        result: task.result,
      },
    });
  });
  /** Settles every subagent still running, e.g. when the native process ends. */
  const settleSubagents = Effect.fn("terminal.native.settleSubagents")(function* (
    status: "interrupted" | "cancelled" | "failed",
  ) {
    for (const [nativeId, entry] of subagents) {
      if (isOrchestrationV2WorkActive(entry.task.status)) yield* subagent(nativeId, { status });
    }
  });

  const finish = Effect.fn("terminal.native.finish")(function* (
    status: "completed" | "interrupted" | "cancelled" | "failed" = "completed",
    failureMessage = "The native provider could not complete this turn.",
  ) {
    if (!active) return;
    const context = active;
    const at = yield* DateTime.now;
    for (const [key, request] of requests) {
      if (request.status === "pending") yield* approval(key, "Native terminal request", true);
    }
    // Ingestion stops tracking background work once a turn ends any other way
    // than completed, so this run's subagents must settle with it.
    if (status !== "completed") {
      for (const [nativeId, entry] of subagents) {
        if (
          entry.task.runId === context.input.runId &&
          isOrchestrationV2WorkActive(entry.task.status)
        )
          yield* subagent(nativeId, { status: status === "failed" ? "failed" : "interrupted" });
      }
    }
    for (const item of items.values()) {
      if (item.status === "running" || item.status === "waiting") {
        const ended = {
          ...item,
          status: status === "completed" ? ("completed" as const) : ("interrupted" as const),
          completedAt: at,
          updatedAt: at,
        };
        yield* ended.type === "assistant_message"
          ? publishAssistant({ ...ended, streaming: false })
          : publishItem(ended);
      }
    }
    yield* emit({
      type: "provider_turn.updated",
      driver,
      threadId: input.threadId,
      providerTurn: { ...context.turn, status, completedAt: at },
    });
    providerThread = {
      ...providerThread,
      status: "idle",
      updatedAt: at,
      firstRunOrdinal: providerThread.firstRunOrdinal ?? PositiveInt.make(context.input.runOrdinal),
      lastRunOrdinal: PositiveInt.make(context.input.runOrdinal),
      nativeConversationHeadRef: driver === "claudeAgent" ? null : context.turn.nativeTurnRef,
    };
    yield* emit({ type: "provider_thread.updated", driver, providerThread });
    const terminal = {
      driver,
      providerThreadId: providerThread.id,
      providerTurnId: context.turn.id,
      runOrdinal: PositiveInt.make(context.input.runOrdinal),
      threadDisposition: "reusable" as const,
    };
    yield* emit(
      status === "failed"
        ? {
            ...terminal,
            type: "turn.terminal",
            status,
            failureItemOrdinal: PositiveInt.make(++ordinal),
            failure: {
              class: "provider_error",
              message: failureMessage.slice(0, 4096),
              code: null,
              retryable: null,
            },
          }
        : { ...terminal, type: "turn.terminal", status, failure: null },
    );
    active = undefined;
    yield* Deferred.succeed(context.done, undefined);
  });

  const runtime: ProviderAdapterV2SessionRuntime = {
    instanceId: options.instanceId,
    driver,
    providerSessionId: input.providerSessionId,
    providerSession: {
      id: input.providerSessionId,
      driver,
      providerInstanceId: options.instanceId,
      status: "ready",
      cwd: input.runtimePolicy.cwd ?? ".",
      model: input.modelSelection.model,
      capabilities: options.capabilities,
      createdAt: now,
      updatedAt: now,
      lastError: null,
    },
    events: Stream.fromQueue(queue),
    ensureThread: (request) =>
      Effect.sync(() => {
        if (request.existingProviderThread)
          providerThread = { ...providerThread, id: request.existingProviderThread.id };
        return providerThread;
      }),
    resumeThread: (request) =>
      request.providerThread.nativeThreadRef?.nativeId === options.nativeThreadId
        ? Effect.sync(() => {
            providerThread = {
              ...request.providerThread,
              providerSessionId: input.providerSessionId,
            };
            return providerThread;
          })
        : unsupported("Restart the terminal to load another native conversation."),
    startTurn: begin,
    compactThread: begin,
    steerTurn: () => unsupported("Submit follow-up messages in the native terminal."),
    interruptTurn: Effect.fn("terminal.native.interrupt")(function* () {
      const done = active?.done;
      yield* options.interrupt;
      if (done) yield* Deferred.await(done);
    }),
    respondToRuntimeRequest: () => unsupported("Answer approvals in the native terminal."),
    readThreadSnapshot: () => unsupported("Return to Chat before reading a provider snapshot."),
    rollbackThread: () => unsupported("Return to Chat before restoring a checkpoint."),
    forkThread: () => unsupported("Return to Chat before forking this conversation."),
  };
  return {
    runtime,
    tool,
    assistant,
    approval,
    subagent,
    settleSubagents,
    finish,
    get active() {
      return active;
    },
    /** The run of the most recent native turn, which may still be settling. */
    get lastRunId() {
      return lastRunId;
    },
    get providerThread() {
      return providerThread;
    },
    setNativeTurn: Effect.fn("terminal.native.setTurn")(function* (
      nativeId: string,
      strength: "strong" | "weak" = "strong",
    ) {
      if (!active) return;
      active.turn = { ...active.turn, nativeTurnRef: { driver, nativeId, strength } };
      yield* emit({
        type: "provider_turn.updated",
        driver,
        threadId: input.threadId,
        providerTurn: active.turn,
      });
    }),
  };
});

export type NativeRuntime = Effect.Success<ReturnType<typeof makeNativeRuntime>>;
