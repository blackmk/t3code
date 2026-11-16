import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  MessageId,
  NativeTerminalError,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  RunAttemptId,
  RunId,
  ThreadId,
  type OrchestrationV2AppThread,
} from "@t3tools/contracts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { ChildProcess, ChildProcessSpawner } from "effect/process";
import { ClaudeProviderCapabilitiesV2 } from "../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import {
  ProviderAdapterV2Event,
  type ProviderAdapterV2TurnInput,
} from "@t3tools/provider-core/server/ProviderAdapter";
import { makeNativeRuntime } from "./NativeRuntime.ts";
import { makeNativeClaude } from "./NativeClaude.ts";

const hookSettings = Schema.fromJsonString(
  Schema.Struct({
    hooks: Schema.Record(
      Schema.String,
      Schema.Array(
        Schema.Struct({
          hooks: Schema.Array(
            Schema.Struct({ command: Schema.String, shell: Schema.optional(Schema.String) }),
          ),
        }),
      ),
    ),
  }),
);
const decodeSettings = Schema.decodeEffect(hookSettings);
const encode = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decode = Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown));
const validateEvent = Schema.decodeUnknownEffect(ProviderAdapterV2Event);
const isNativeTerminalError = Schema.is(NativeTerminalError);

it.effect(
  "Claude command hooks admit prompts, capture tools and answers, preserve the resume cursor, and block failed preparation",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const platform = yield* HostProcess.Platform;
        const spawner = yield* ChildProcessSpawner.ChildProcessSpawner;
        const ids = yield* IdAllocator.IdAllocatorV2;
        const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-claude-hooks-" });
        const instanceId = ProviderInstanceId.make("claude");
        const driver = ProviderDriverKind.make("claudeAgent");
        const threadId = ThreadId.make("claude-native");
        const modelSelection = { instanceId, model: "claude-sonnet-4-6" };
        const runtimePolicy = {
          cwd,
          runtimeMode: "full-access" as const,
          interactionMode: "default" as const,
        };
        const now = yield* DateTime.now;
        const appThread: OrchestrationV2AppThread = {
          id: threadId,
          projectId: ProjectId.make("claude-project"),
          title: "Claude native",
          providerInstanceId: instanceId,
          modelSelection,
          ...runtimePolicy,
          branch: null,
          worktreePath: cwd,
          activeProviderThreadId: null,
          lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
          forkedFrom: null,
          createdBy: "user",
          creationSource: "web",
          createdAt: now,
          updatedAt: now,
          archivedAt: null,
          deletedAt: null,
          settledAt: null,
          settledOverride: null,
          lastVisitedAt: null,
        };
        const runtime = yield* makeNativeRuntime({
          driver,
          instanceId,
          nativeThreadId: "native-claude-session",
          ids,
          input: {
            threadId,
            providerSessionId: ProviderSessionId.make("claude-session"),
            modelSelection,
            runtimePolicy,
          },
          capabilities: ClaudeProviderCapabilitiesV2,
          prepare: () => Effect.void,
          interrupt: Effect.void,
        });
        let rejectPrompt = false;
        const submit = (text: string) =>
          Effect.gen(function* () {
            if (rejectPrompt)
              return yield* new NativeTerminalError({ threadId, detail: "Workspace unavailable" });
            const turn: ProviderAdapterV2TurnInput = {
              appThread,
              threadId,
              modelSelection,
              runtimePolicy,
              providerThread: runtime.providerThread,
              runId: RunId.make("claude-run"),
              runOrdinal: 1,
              providerTurnOrdinal: 1,
              attemptId: RunAttemptId.make("claude-attempt"),
              rootNodeId: NodeId.make("claude-root"),
              message: {
                messageId: MessageId.make("claude-prompt"),
                text,
                attachments: [],
                createdBy: "user",
                creationSource: "provider",
              },
            };
            yield* runtime.runtime.startTurn(turn);
            return turn;
          }).pipe(
            Effect.mapError((cause) =>
              isNativeTerminalError(cause)
                ? cause
                : new NativeTerminalError({ threadId, detail: "Preparation failed", cause }),
            ),
          );
        const bridge = yield* makeNativeClaude({
          stateDir: cwd,
          nativeThreadId: "native-claude-session",
          platform,
          executable: process.execPath,
          submit,
          getRuntime: () => runtime,
        });
        const settings = yield* decodeSettings(yield* fs.readFileString(bridge.settings));
        const hook = (event: string, payload: Record<string, unknown> = {}) =>
          Effect.scoped(
            Effect.gen(function* () {
              const command = settings.hooks[event]![0]!.hooks[0]!.command;
              const child = yield* spawner.spawn(
                ChildProcess.make(
                  platform === "win32" ? "powershell.exe" : "sh",
                  platform === "win32"
                    ? ["-NoProfile", "-NonInteractive", "-Command", command]
                    : ["-c", command],
                  {
                    stdin: Stream.make(
                      new TextEncoder().encode(
                        encode({
                          hook_event_name: event,
                          session_id: "native-claude-session",
                          ...payload,
                        }),
                      ),
                    ),
                  },
                ),
              );
              const output = yield* child.stdout.pipe(Stream.decodeText(), Stream.runCollect);
              const stderr = yield* child.stderr.pipe(Stream.decodeText(), Stream.runCollect);
              assert.equal(Number(yield* child.exitCode), 0, stderr.join(""));
              return yield* decode(output.join(""));
            }),
          );
        assert.deepEqual(yield* hook("UserPromptSubmit", { prompt: "Edit a file" }), {});
        // Claude Code delivers finished background work as a prompt mid-turn.
        assert.deepEqual(
          yield* hook("UserPromptSubmit", {
            prompt: "<task-notification>\n<status>completed</status>\n</task-notification>",
          }),
          {},
        );
        // So are messages its agents send, with or without a header line.
        for (const prompt of [
          '<agent-message from="agent-1">\nStaged.\n</agent-message>',
          'Another Claude session sent a message while you were working:\n<agent-message from="agent-1">\nStaged.\n</agent-message>',
        ]) {
          assert.deepEqual(yield* hook("UserPromptSubmit", { prompt }), {});
        }
        yield* hook("PreToolUse", {
          tool_use_id: "edit",
          tool_name: "Write",
          tool_input: { file_path: "example.txt" },
        });
        yield* hook("PostToolUse", {
          tool_use_id: "edit",
          tool_name: "Write",
          tool_response: "written",
        });
        yield* hook("PreToolUse", {
          tool_use_id: "agent-call",
          tool_name: "Agent",
          tool_input: { description: "Audit tests", prompt: "Find flaky tests" },
        });
        yield* hook("PreToolUse", {
          tool_use_id: "inside-agent",
          tool_name: "Read",
          tool_input: { file_path: "test.ts" },
          agent_id: "agent-1",
        });
        yield* hook("PostToolUse", {
          tool_use_id: "agent-call",
          tool_name: "Agent",
          tool_response: {
            status: "async_launched",
            agentId: "agent-1",
            prompt: "Find flaky tests",
          },
        });
        // A fast agent can stop before its own launch is acknowledged.
        yield* hook("PreToolUse", {
          tool_use_id: "fast-call",
          tool_name: "Agent",
          tool_input: { description: "Quick check", prompt: "Count files" },
        });
        yield* hook("SubagentStop", { agent_id: "fast-1", last_assistant_message: "12 files" });
        yield* hook("PostToolUse", {
          tool_use_id: "fast-call",
          tool_name: "Agent",
          tool_response: { status: "async_launched", agentId: "fast-1", prompt: "Count files" },
        });
        const transcript = `${cwd}/transcript.jsonl`;
        yield* fs.writeFileString(
          transcript,
          `${encode({ type: "assistant", uuid: "assistant-resume-cursor" })}\n`,
        );
        yield* hook("Stop", { transcript_path: transcript, last_assistant_message: "File edited" });
        const events = yield* runtime.runtime.events.pipe(
          Stream.takeUntil((event) => event.type === "turn.terminal"),
          Stream.runCollect,
        );
        for (const event of events) yield* validateEvent(event);
        // The notification did not interrupt the turn it arrived in.
        const terminal = events.at(-1);
        assert.equal(terminal?.type === "turn.terminal" ? terminal.status : null, "completed");
        assert.isTrue(
          events.some(
            (event) => event.type === "message.updated" && event.message.text === "File edited",
          ),
        );
        assert.isTrue(
          events.some(
            (event) =>
              event.type === "provider_turn.updated" &&
              event.providerTurn.nativeTurnRef?.nativeId === "assistant-resume-cursor",
          ),
        );
        assert.isUndefined(runtime.active);
        assert.isNull(runtime.providerThread.nativeConversationHeadRef);
        assert.isTrue(
          events.some(
            (event) =>
              event.type === "subagent.updated" &&
              event.subagent.prompt === "Count files" &&
              event.subagent.status === "completed" &&
              event.subagent.result === "12 files",
          ),
        );
        const launched = events.find((event) => event.type === "subagent.updated");
        assert.deepInclude(launched?.type === "subagent.updated" ? launched.subagent : {}, {
          prompt: "Find flaky tests",
          title: "Audit tests",
          status: "running",
        });
        // The Agent call and the subagent's own tools are not parent tool items.
        assert.isFalse(
          events.some(
            (event) =>
              event.type === "turn_item.updated" &&
              event.turnItem.type === "dynamic_tool" &&
              ["agent-call", "inside-agent"].includes(event.turnItem.nativeItemRef?.nativeId ?? ""),
          ),
        );
        // A hook for another session must never keep a subagent from stopping.
        assert.deepEqual(yield* hook("SubagentStop", { session_id: "other", agent_id: "x" }), {});
        // The background agent finishes after its parent turn ended.
        assert.deepEqual(
          yield* hook("SubagentStop", {
            agent_id: "agent-1",
            last_assistant_message: "Two flaky tests",
          }),
          {},
        );
        const finished = yield* runtime.runtime.events.pipe(
          Stream.filter((event) => event.type === "subagent.updated"),
          Stream.take(1),
          Stream.runCollect,
        );
        assert.deepInclude(finished[0]?.type === "subagent.updated" ? finished[0].subagent : {}, {
          status: "completed",
          result: "Two flaky tests",
        });
        // Esc fires no hook and only marks the transcript; a notification after
        // it starts its own turn instead of joining the cancelled one.
        assert.deepEqual(yield* hook("UserPromptSubmit", { prompt: "Long task" }), {});
        yield* fs.writeFileString(
          transcript,
          `${encode({ type: "user", message: { role: "user", content: [{ type: "text", text: "[Request interrupted by user]" }] } })}\n`,
        );
        assert.deepEqual(
          yield* hook("UserPromptSubmit", {
            prompt: "<task-notification>\n<status>completed</status>\n</task-notification>",
            transcript_path: transcript,
          }),
          {},
        );
        const cancelled = yield* runtime.runtime.events.pipe(
          Stream.filter((event) => event.type === "turn.terminal"),
          Stream.take(1),
          Stream.runCollect,
        );
        assert.equal(
          cancelled[0]?.type === "turn.terminal" ? cancelled[0].status : null,
          "interrupted",
        );
        assert.isDefined(runtime.active);
        rejectPrompt = true;
        const blocked = yield* hook("UserPromptSubmit", { prompt: "Must not run" });
        assert.equal((blocked as { decision: string }).decision, "block");
        assert.isUndefined(runtime.active);
      }),
    ).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, IdAllocator.layer))),
);
