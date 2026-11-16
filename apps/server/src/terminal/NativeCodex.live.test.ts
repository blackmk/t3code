import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeWS from "@effect/platform-node/NodeWS";
import { assert, it } from "@effect/vitest";
import { NativeTerminalError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import * as HostProcess from "@t3tools/shared/HostProcess";
import { makeNativeCodex, record } from "./NativeCodex.ts";

// Opt-in transport smoke test: no model requests or changes to the installed CLI's home.
const binary = process.env.T3_NATIVE_CODEX_TEST_BINARY;
const encode = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decode = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));

it.effect.skipIf(!binary)(
  "binds the installed Codex remote CLI protocol to one conversation",
  () =>
    Effect.scoped(
      Effect.gen(function* () {
        const fs = yield* FileSystem.FileSystem;
        const env = yield* HostProcess.Environment;
        const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-native-codex-live-" });
        const bridge = yield* makeNativeCodex({
          command: binary!,
          args: [],
          env: { ...env, CODEX_HOME: cwd },
          cwd,
          threadParams: {
            cwd,
            model: "gpt-5.4",
            approvalPolicy: "on-request",
            sandbox: "workspace-write",
          },
          submit: () =>
            Effect.fail(
              new NativeTerminalError({ threadId: "test", detail: "Deliberately rejected prompt" }),
            ),
          getRuntime: () => undefined,
          readMcpSession: () => Effect.undefined,
        });
        const client = yield* Effect.acquireRelease(
          Effect.callback<NodeWS.WebSocket, Error>((resume) => {
            const ws = new NodeWS.WebSocket(bridge.url, {
              headers: { Authorization: `Bearer ${bridge.token}` },
            });
            ws.once("open", () => resume(Effect.succeed(ws)));
            ws.once("error", (cause) => resume(Effect.fail(cause)));
          }),
          (ws) => Effect.sync(() => ws.terminate()),
        );
        let ordinal = 0;
        const request = (method: string, params: unknown) =>
          Effect.callback<Record<string, unknown>, Error>((resume) => {
            const id = ++ordinal;
            const listener = (data: NodeWS.RawData) => {
              const response = record(decode(data.toString()));
              if (response.id !== id) return;
              client.off("message", listener);
              resume(Effect.succeed(response));
            };
            client.on("message", listener);
            client.send(encode({ id, method, params }));
            return Effect.sync(() => client.off("message", listener));
          });
        assert.isUndefined(
          (yield* request("initialize", { clientInfo: { name: "terminal-test", version: "1" } }))
            .error,
        );
        const conversation = yield* request("thread/start", { cwd });
        assert.isUndefined(conversation.error, encode(conversation));
        assert.equal(record(record(conversation.result).thread).id, bridge.nativeThreadId);
        assert.isDefined(
          (yield* request("thread/fork", { threadId: bridge.nativeThreadId })).error,
        );
        assert.isDefined(
          (yield* request("turn/start", {
            threadId: bridge.nativeThreadId,
            input: [{ type: "text", text: "Do not execute" }],
          })).error,
        );
        assert.isDefined((yield* request("thread/start", { cwd })).error);
      }),
    ).pipe(Effect.provide(NodeServices.layer)),
  30_000,
);
