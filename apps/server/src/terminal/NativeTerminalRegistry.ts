import type { CommandId, ProviderSessionId, ThreadId } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type { ProviderAdapterV2 } from "@t3tools/provider-core/server/ProviderAdapter";

/** A terminal supplies a provider runtime; ordinary V2 runs still own its history. */
export class NativeTerminalRegistry extends Context.Service<
  NativeTerminalRegistry,
  {
    readonly register: (
      threadId: ThreadId,
      sessionId: ProviderSessionId,
      openSession: ProviderAdapterV2["Service"]["openSession"],
    ) => Effect.Effect<void>;
    readonly unregister: (threadId: ThreadId) => Effect.Effect<void>;
    readonly get: (threadId: ThreadId) => ProviderAdapterV2["Service"]["openSession"] | undefined;
    readonly owns: (threadId: ThreadId) => boolean;
    readonly ownsSession: (sessionId: ProviderSessionId) => boolean;
    readonly admits: (threadId: ThreadId, commandId: CommandId) => boolean;
    readonly withInputCommand: <A, E, R>(
      threadId: ThreadId,
      commandId: CommandId,
      dispatch: Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E, R>;
  }
>()("t3/terminal/NativeTerminalRegistry") {}

const make = Effect.sync(() => {
  const owners = new Map<
    ThreadId,
    {
      sessionId: ProviderSessionId;
      openSession: ProviderAdapterV2["Service"]["openSession"];
      commands: Set<CommandId>;
    }
  >();
  return NativeTerminalRegistry.of({
    register: Effect.fn("NativeTerminalRegistry.register")((threadId, sessionId, openSession) =>
      Effect.sync(() => {
        owners.set(threadId, { sessionId, openSession, commands: new Set() });
      }),
    ),
    unregister: Effect.fn("NativeTerminalRegistry.unregister")((threadId) =>
      Effect.sync(() => {
        owners.delete(threadId);
      }),
    ),
    get: (threadId) => owners.get(threadId)?.openSession,
    owns: (threadId) => owners.has(threadId),
    ownsSession: (sessionId) =>
      Array.from(owners.values()).some((owner) => owner.sessionId === sessionId),
    admits: (threadId, commandId) => owners.get(threadId)?.commands.has(commandId) === true,
    withInputCommand: Effect.fn("NativeTerminalRegistry.withInputCommand")(function* <A, E, R>(
      threadId: ThreadId,
      commandId: CommandId,
      dispatch: Effect.Effect<A, E, R>,
    ) {
      return yield* Effect.acquireUseRelease(
        Effect.sync(() => {
          owners.get(threadId)?.commands.add(commandId);
        }),
        () => dispatch,
        () =>
          Effect.sync(() => {
            owners.get(threadId)?.commands.delete(commandId);
          }),
      );
    }),
  });
});

export const layer = Layer.effect(NativeTerminalRegistry, make);
