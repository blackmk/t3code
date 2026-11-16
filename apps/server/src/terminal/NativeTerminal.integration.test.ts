import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { checkpointRefForScopeOrdinal } from "../orchestration-v2/CheckpointService.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import { CodexProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import {
  ProviderAdapterV2Event,
  type ProviderAdapterV2,
  type ProviderAdapterV2TurnInput,
} from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderAdapterRegistry from "../orchestration-v2/ProviderAdapterRegistry.ts";
import { layerWithRegistry } from "../orchestration-v2/testkit/ProviderReplayHarness.ts";
import { checkpointWorkspace } from "@t3tools/provider-testing/replayWorkspace";
import * as Schema from "effect/Schema";
import { awaitNativeRunSettled } from "./AgentTerminal.ts";
import { makeNativeRuntime, type NativeRuntime } from "./NativeRuntime.ts";
import * as NativeTerminalRegistry from "./NativeTerminalRegistry.ts";

const driver = ProviderDriverKind.make("codex");
const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "gpt-5.4" };
const decodeProviderEvent = Schema.decodeUnknownEffect(ProviderAdapterV2Event);
const adapter: ProviderAdapterV2["Service"] = {
  instanceId,
  driver,
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
  openSession: () => Effect.die("Native ownership must override the normal adapter"),
};

it.effect(
  "native prompts capture history and checkpoints through V2, with guarded Chat sends and replayable receipts",
  () =>
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace("native-terminal", { "result.txt": "before\n" });
      const runtimeLayer = layerWithRegistry(
        { name: "native-terminal", runtimePolicyOverride: { cwd } },
        ProviderAdapterRegistry.layerFromAdapters([adapter]),
      ).pipe(Layer.provideMerge(NativeTerminalRegistry.layer));
      yield* Effect.gen(function* () {
        const engine = yield* Orchestrator.OrchestratorV2;
        const registry = yield* NativeTerminalRegistry.NativeTerminalRegistry;
        const ids = yield* IdAllocator.IdAllocatorV2;
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const threadId = ThreadId.make("native-terminal-thread");
        let ready = yield* Deferred.make<ProviderAdapterV2TurnInput>();
        let native: NativeRuntime | undefined;
        yield* engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make("native-create"),
          threadId,
          projectId: ProjectId.make("native-project"),
          title: "Native terminal",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: cwd,
          createdBy: "user",
          creationSource: "web",
        });
        yield* registry.register(threadId, ProviderSessionId.make("native-session"), (input) =>
          Effect.gen(function* () {
            native = yield* makeNativeRuntime({
              instanceId,
              driver,
              input,
              nativeThreadId: "native-conversation",
              ids,
              capabilities: CodexProviderCapabilitiesV2,
              interrupt: Effect.void,
              prepare: (turn) => Deferred.succeed(ready, turn).pipe(Effect.asVoid),
            });
            return native.runtime;
          }),
        );
        const command = {
          type: "message.dispatch" as const,
          commandId: CommandId.make("native-prompt"),
          threadId,
          messageId: MessageId.make("native-message"),
          text: "Change result.txt",
          attachments: [],
          createdBy: "user" as const,
          creationSource: "provider" as const,
          dispatchMode: { type: "start_immediately" as const },
        };
        const blocked = yield* engine.dispatch(command).pipe(Effect.result);
        assert.equal(blocked._tag, "Failure");
        if (blocked._tag === "Failure")
          assert.equal(blocked.failure._tag, "OrchestratorNativeTerminalOwnedError");
        // Chat's subagent Stop has no native path, so it is refused up front.
        const stopSubagents = yield* engine
          .dispatch({
            type: "subagent.stop",
            commandId: CommandId.make("native-subagent-stop"),
            threadId,
            runId: RunId.make("native-run"),
            subagentId: NodeId.make("native-subagent"),
          })
          .pipe(Effect.result);
        assert.equal(stopSubagents._tag, "Failure");
        if (stopSubagents._tag === "Failure")
          assert.equal(stopSubagents.failure._tag, "OrchestratorNativeTerminalOwnedError");
        const accepted = yield* registry.withInputCommand(
          threadId,
          command.commandId,
          engine.dispatch(command),
        );
        const turn = yield* Deferred.await(ready);
        assert.equal(turn.message.text, command.text);
        // A retry remains valid after the short-lived native admission has gone away.
        assert.equal((yield* engine.dispatch(command)).sequence, accepted.sequence);
        const baseline = yield* engine.getThreadProjection(threadId);
        const scope = baseline.checkpointScopes[0]!;
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const baselineRef = yield* checkpointRefForScopeOrdinal({
          scopeId: scope.id,
          ordinalWithinScope: 0,
        });
        assert.equal(
          Number(
            yield* spawner.exitCode(
              ChildProcess.make("git", ["cat-file", "-e", baselineRef], { cwd }),
            ),
          ),
          0,
        );
        assert.isDefined(native);
        const terminal = native!;
        yield* fs.writeFileString(path.join(cwd, "result.txt"), "after\n");
        yield* terminal.setNativeTurn("native-turn-1");
        yield* terminal.tool("same-tool", "edit", { path: "result.txt" }, false);
        yield* terminal.approval("same-approval", "Allow edit", false);
        yield* terminal.approval("same-approval", "Allow edit", true);
        yield* terminal.tool("same-tool", "edit", { path: "result.txt" }, true);
        yield* terminal.assistant("same-answer", "Changed result.txt", true);
        yield* terminal.finish();
        yield* engine.streamStoredEventsFrom({ threadId, afterSequence: accepted.sequence }).pipe(
          Stream.filter(
            ({ event }) =>
              event.type === "run.updated" &&
              event.payload.id === turn.runId &&
              event.payload.status === "completed",
          ),
          Stream.take(1),
          Stream.runDrain,
        );
        const projection = yield* engine.getThreadProjection(threadId);
        assert.equal(projection.runs.at(-1)?.status, "completed");
        assert.equal(projection.messages.filter((message) => message.role === "user").length, 1);
        assert.equal(
          projection.messages.find((message) => message.role === "assistant")?.text,
          "Changed result.txt",
        );
        assert.isTrue(projection.checkpoints.some((checkpoint) => checkpoint.appRunOrdinal === 1));
        assert.equal(projection.runtimeRequests.at(-1)?.status, "resolved");
        assert.equal(projection.providerTurns.at(-1)?.nativeTurnRef?.nativeId, "native-turn-1");
        ready = yield* Deferred.make<ProviderAdapterV2TurnInput>();
        const nextCommand = {
          ...command,
          commandId: CommandId.make("native-prompt-2"),
          messageId: MessageId.make("native-message-2"),
          text: "Continue",
        };
        const next = yield* registry.withInputCommand(
          threadId,
          nextCommand.commandId,
          engine.dispatch(nextCommand),
        );
        const nextTurn = yield* Deferred.await(ready);
        yield* terminal.approval("same-approval", "Allow next edit", false);
        yield* terminal.approval("same-approval", "Allow next edit", true);
        yield* terminal.assistant("same-answer", "Second answer", true);
        yield* terminal.finish();
        yield* engine.streamStoredEventsFrom({ threadId, afterSequence: next.sequence }).pipe(
          Stream.filter(
            ({ event }) =>
              event.type === "run.updated" &&
              event.payload.id === nextTurn.runId &&
              event.payload.status === "completed",
          ),
          Stream.take(1),
          Stream.runDrain,
        );
        const after = yield* engine.getThreadProjection(threadId);
        assert.equal(after.messages.filter((message) => message.role === "assistant").length, 2);
        const approvals = after.turnItems.filter((item) => item.type === "approval_request");
        assert.equal(approvals.length, 2);
        assert.notEqual(approvals[0]?.id, approvals[1]?.id);
        ready = yield* Deferred.make<ProviderAdapterV2TurnInput>();
        const interruptedCommand = {
          ...command,
          commandId: CommandId.make("native-prompt-interrupted"),
          messageId: MessageId.make("native-message-interrupted"),
          text: "Start a longer answer",
        };
        const interrupted = yield* registry.withInputCommand(
          threadId,
          interruptedCommand.commandId,
          engine.dispatch(interruptedCommand),
        );
        const interruptedTurn = yield* Deferred.await(ready);
        yield* terminal.assistant("partial-answer", "Partial answer", false);
        yield* terminal.finish("interrupted");
        yield* engine
          .streamStoredEventsFrom({ threadId, afterSequence: interrupted.sequence })
          .pipe(
            Stream.filter(
              ({ event }) =>
                event.type === "run.updated" &&
                event.payload.id === interruptedTurn.runId &&
                event.payload.status === "interrupted",
            ),
            Stream.take(1),
            Stream.runDrain,
          );
        const stopped = yield* engine.getThreadProjection(threadId);
        const partial = stopped.messages.find((message) => message.text === "Partial answer");
        assert.isDefined(partial);
        assert.isFalse(partial!.streaming);
        const partialItem = stopped.turnItems.find(
          (item) => item.type === "assistant_message" && item.messageId === partial!.id,
        );
        assert.equal(partialItem?.status, "interrupted");
        assert.isTrue(partialItem?.type === "assistant_message" && !partialItem.streaming);
        for (const event of [
          {
            type: "provider_thread.updated" as const,
            driver,
            providerThread: terminal.providerThread,
          },
        ]) {
          yield* decodeProviderEvent(event);
        }
      }).pipe(
        Effect.provide(
          Layer.mergeAll(runtimeLayer, IdAllocator.layer, NodeServices.layer).pipe(
            Layer.provideMerge(NodeCrypto.layer),
          ),
        ),
      );
    }),
);

it.effect(
  "native subagents become child threads, including background work that outlives its turn",
  () =>
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace("native-terminal-subagents", {
        "result.txt": "before\n",
      });
      const runtimeLayer = layerWithRegistry(
        { name: "native-terminal-subagents", runtimePolicyOverride: { cwd } },
        ProviderAdapterRegistry.layerFromAdapters([adapter]),
      ).pipe(Layer.provideMerge(NativeTerminalRegistry.layer));
      yield* Effect.gen(function* () {
        const engine = yield* Orchestrator.OrchestratorV2;
        const registry = yield* NativeTerminalRegistry.NativeTerminalRegistry;
        const ids = yield* IdAllocator.IdAllocatorV2;
        const threadId = ThreadId.make("native-subagent-thread");
        let ready = yield* Deferred.make<ProviderAdapterV2TurnInput>();
        let native: NativeRuntime | undefined;
        yield* engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make("native-subagent-create"),
          threadId,
          projectId: ProjectId.make("native-project"),
          title: "Native subagents",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: cwd,
          createdBy: "user",
          creationSource: "web",
        });
        yield* registry.register(
          threadId,
          ProviderSessionId.make("native-subagent-session"),
          (input) =>
            Effect.gen(function* () {
              native = yield* makeNativeRuntime({
                instanceId,
                driver,
                input,
                nativeThreadId: "native-subagent-conversation",
                ids,
                capabilities: CodexProviderCapabilitiesV2,
                interrupt: Effect.void,
                prepare: (turn) => Deferred.succeed(ready, turn).pipe(Effect.asVoid),
              });
              return native.runtime;
            }),
        );
        const command = {
          type: "message.dispatch" as const,
          commandId: CommandId.make("native-subagent-prompt"),
          threadId,
          messageId: MessageId.make("native-subagent-message"),
          text: "Review and audit",
          attachments: [],
          createdBy: "user" as const,
          creationSource: "provider" as const,
          dispatchMode: { type: "start_immediately" as const },
        };
        yield* registry.withInputCommand(threadId, command.commandId, engine.dispatch(command));
        const turn = yield* Deferred.await(ready);
        const terminal = native!;
        yield* terminal.subagent("foreground", {
          status: "running",
          prompt: "Review the diff",
          title: "Review",
        });
        yield* terminal.subagent("foreground", { status: "completed", result: "Looks good" });
        yield* terminal.subagent("background", { status: "running", prompt: "Audit the tests" });
        yield* terminal.assistant("answer", "Started the audit", true);
        yield* terminal.finish();

        // The CLI accepts its next prompt as soon as the turn ends; the run
        // settles in the projections later.
        yield* awaitNativeRunSettled({ engine, threadId, runId: turn.runId });
        const settled = yield* engine.getThreadProjection(threadId);
        assert.equal(settled.runs.find((run) => run.id === turn.runId)?.status, "completed");

        // A late duplicate start never reopens a finished task.
        yield* terminal.subagent("foreground", { status: "running" });
        const sequence = yield* engine.getThreadEventSequence(threadId);
        yield* terminal.subagent("background", { status: "completed", result: "Two flaky tests" });
        yield* engine.streamStoredEventsFrom({ threadId, afterSequence: sequence }).pipe(
          Stream.filter(
            ({ event }) =>
              event.type === "subagent.updated" &&
              event.payload.status === "completed" &&
              event.payload.result === "Two flaky tests",
          ),
          Stream.take(1),
          Stream.runDrain,
        );

        const projection = yield* engine.getThreadProjection(threadId);
        assert.deepEqual(
          projection.subagents
            .map((task) => [task.prompt, task.status, task.result])
            .toSorted(([left], [right]) => String(left).localeCompare(String(right))),
          [
            ["Audit the tests", "completed", "Two flaky tests"],
            ["Review the diff", "completed", "Looks good"],
          ],
        );
        const review = projection.subagents.find((task) => task.prompt === "Review the diff")!;
        const child = yield* engine.getThreadProjection(review.childThreadId!);
        assert.equal(child.thread.lineage.parentThreadId, threadId);
        assert.equal(child.thread.lineage.relationshipToParent, "subagent");
        assert.deepEqual(
          child.messages.map((message) => [message.role, message.text]),
          [
            ["user", "Review the diff"],
            ["assistant", "Looks good"],
          ],
        );
        // The answer of the last background subagent arrives after its parent
        // turn ended and must still reach its child thread.
        const audit = projection.subagents.find((task) => task.prompt === "Audit the tests")!;
        const auditChild = yield* engine.getThreadProjection(audit.childThreadId!);
        assert.deepEqual(
          auditChild.messages.map((message) => [message.role, message.text]),
          [
            ["user", "Audit the tests"],
            ["assistant", "Two flaky tests"],
          ],
        );

        // A sub-agent reused in a later prompt reopens under that live run.
        ready = yield* Deferred.make<ProviderAdapterV2TurnInput>();
        const followUp = {
          ...command,
          commandId: CommandId.make("native-subagent-prompt-2"),
          messageId: MessageId.make("native-subagent-message-2"),
          text: "Review again",
        };
        yield* registry.withInputCommand(threadId, followUp.commandId, engine.dispatch(followUp));
        const secondTurn = yield* Deferred.await(ready);
        yield* terminal.subagent("foreground", { status: "running", reopen: true });
        yield* terminal.subagent("foreground", { status: "completed", result: "Still good" });
        yield* terminal.finish();
        yield* awaitNativeRunSettled({ engine, threadId, runId: secondTurn.runId });
        const reopened = (yield* engine.getThreadProjection(threadId)).subagents.find(
          (task) => task.prompt === "Review the diff",
        );
        assert.equal(reopened?.runId, secondTurn.runId);
        assert.equal(reopened?.result, "Still good");
        const reviewChild = yield* engine.getThreadProjection(review.childThreadId!);
        assert.equal(reviewChild.messages.at(-1)?.text, "Still good");
      }).pipe(
        Effect.provide(
          Layer.mergeAll(runtimeLayer, IdAllocator.layer, NodeServices.layer).pipe(
            Layer.provideMerge(NodeCrypto.layer),
          ),
        ),
      );
    }),
);
