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
