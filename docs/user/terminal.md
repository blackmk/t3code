# Terminal history

## Native agent terminal

In the desktop app, choose **Terminal** in a Codex or Claude Code conversation to
use its native CLI. Chat and Terminal share the conversation; native prompts,
answers, tool activity, and workspace checkpoints appear in Chat. Subagents the
CLI starts appear under the conversation in the sidebar, as they do in Chat.
Answer native permission requests in Terminal. Claude answers appear in Chat
when the turn ends.

Use **Chat** to return after the current turn and checkpoint finish. The terminal
stays running when you switch threads or reconnect. **Restart** resumes the same
conversation after the CLI exits. **Stop** interrupts a turn and closes the CLI;
restart it or return to Chat afterward. Choose the model and initial permission
and plan modes in Chat before opening Terminal. The command palette and configurable
**Chat: Switch Chat and Terminal** shortcut provide the same switch; the default
is Ctrl+Alt+J on Windows/Linux and Cmd+Alt+J on macOS.

Terminal view requires an environment with native terminal support and a locally
installed Codex or Claude CLI. Other providers continue to use Chat. Create,
fork, or restore conversations from Chat before returning to Terminal.

## Scrollback

Each terminal keeps up to 5,000 lines and 8 MiB of scrollback on its environment
server. T3 Code removes the oldest output when either limit is reached. A long
line can be shortened at the start. New terminal output is not truncated.

These limits apply when you reconnect and when T3 Code restores saved terminal
history. A client can show less scrollback than the server keeps.

On web and desktop, use Shift+PageUp and Shift+PageDown to read scrollback
without leaving terminal input. Ctrl+Shift+Home and Ctrl+Shift+End jump to the
start and latest output; use Cmd instead of Ctrl on macOS. Full-screen terminal
programs keep these navigation keys.

To copy all retained output, choose Select all in the terminal's context menu,
then copy. Cmd+A on macOS and Ctrl+Shift+A elsewhere select the same output.
Ctrl+A still moves to the beginning of shell input on Windows and Linux.
Jump to latest in the context menu returns to the current output.

On iPhone and iPad, long-press the terminal to paste or to choose Select Text.
Select Text shows the visible output, where you can copy text or tap a link.

## fish

T3 Code starts fish with `no-query-term`, avoiding startup waits for terminal
replies. This disables optional fish features that depend on those replies.
T3 Code's exported `fish_features` also hides any universal flags you set with
`set -U fish_features`.

To preserve those flags or enable queries with `query-term`, include them in
`fish_features` in the environment that launches T3 Code. A shell export applies
when you launch T3 Code from that shell; it does not configure a desktop app
launched from the Dock or Finder. Enabling queries can restore the startup wait.
