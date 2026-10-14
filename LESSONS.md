# LESSONS.md — Project Lessons

One entry per lesson, format:
`date | situation | what went wrong or worked | rule for next time`.
Keep each entry to 1–3 lines. Never delete entries; mark them `obsolete` with a
reason instead. A rule confirmed 3 times gets promoted into `AGENTS.md`.

## Active

- 2026-10-11 | Drove adb against the short package id while the app ran the
  branch-suffixed id | diagnosed a "frozen store" and dead processes that did
  not exist | always read `output-metadata.json` → `applicationId` for every
  adb command.
- 2026-10-11 | Trusted pid/pidof to confirm process death | PIDs are reused on
  the device and pidof came back empty while the app was alive | trust in-log
  lines (`ensureLoaded`/`restoreIfNeeded`) and `ps -A`, never pid/pidof alone.
- 2026-10-11 | Batch file-creation failures showed only a generic message | the
  real SAF `SecurityException` was swallowed into a `DocumentsContract` `Log.w`
  plus a null return | log failures with stack traces, and also read tag
  `DocumentsContract` before theorizing.
- 2026-10-11 | Advanced on the user's "done" replies | taps routinely happen
  after the reply arrives | verify each action's log signature before moving on
  to the next step.
- 2026-10-11 | WSL-side git operations recreated `values-iw` as an unreadable
  LX symlink | Windows Gradle/ktlint/AGP broke with `FileSystemException` |
  re-apply the plain-copy workaround + `skip-worktree`, verify from Windows.
- 2026-10-11 | Tested the same-title folder hypothesis with two
  different-titled playlists | the collision path could never trigger | design
  each test so it can actually trigger the hypothesis, or mark it unverified.
- 2026-10-11 | Kill-recovery left the item "Downloading" forever | reconcile treats
  a Giga-enqueued mission as DOWNLOADING but nothing resumes it; the restored
  service does not auto-start recovered missions | after re-attaching a mission,
  verify it actually runs (bytes moving), and resume it explicitly if idle.
- 2026-10-11 | Two NewPipe variants installed (short + branch-suffixed ids)
  logging the same tag | chased a "stale build" that was the other app's logs;
  pidof hid it, `ps -A` full names revealed it | map every logging pid to its
  package before attributing lines, and test with one app only.
- 2026-10-11 | Toolbar MenuProvider actions never appeared on device | the queue
  activity uses the app NoActionBar theme so menus have nowhere to render; the
  user tapped Delete twice instead of an invisible Clear finished | verify menu
  UI actually renders on device before relying on it in tests.
- 2026-10-11 | Fast "up-to-date" Gradle build after real source edits | nearly
  shipped a stale APK for verification (caught via APK timestamp + missing log
  lines on device) | after every build, confirm compile ran and the APK is
  fresh before installing.
- 2026-10-11 | Chased a log paradox for hours with pattern-filtered greps | the
  decisive lines (an early Clear-finished, full pid context) sat unread in the
  file while theories multiplied | read the full unfiltered window first, grep
  second; verify which app (pid to package) logged each line.
- 2026-10-11 | Assumed one app on device during a debugging session | a second
  variant logged the same tag with different code, poisoning every conclusion |
  map pid to package before attributing any log line, every round.
- 2026-10-11 | Fixed kill-recovery stall by splitting reconcile: idle-enqueued missions now call manager.resumeMission() in reconcileAfterRestart; 30s poll safety net as backstop | log proves immediate resume + bytes moving + batch DONE without Play press | always resume explicitly in reconcile, not just mark status; add a poll safety net for edge cases.
- 2026-10-11 | Corrupt store test: pushed garbage JSON, force-stop, opened queue | BatchStore.loadV2 logs Log.e with full stack trace, quarantines file as .corrupt-<ts>, loads empty queue, persists empty store; backup restore verified | corrupt-store handling is proven; the quarantine + empty-queue fallback works as designed.
- 2026-10-12 | Ran Gradle in a backgrounded `&` subshell from the bash tool | the log froze mid-configuration and the build died with the tool call; its stray daemon then blocked the retry on the config-cache lock | run long builds in the foreground with a long timeout, never `&`-background them.
- 2026-10-14 | `adb devices` right after daemon auto-start showed no devices; retry seconds later listed the phone | nearly reported the device missing | never declare the device absent on the first empty listing, retry once after daemon init.
- 2026-10-14 | Passed `$pkg` PowerShell variable inside a bash double-quoted `powershell.exe -Command` string | bash expanded `$pkg` to empty and `force-stop` failed with no package arg | inline literal package names, never `$variables`, in powershell commands from bash.
- 2026-10-14 | Grepped logcat for `checkNew|check-for` and concluded no check ran on the batch | the real lines say `check new:`, found only after grepping the exact string | confirm exact log keywords in code before grepping, or read the full window first.
- 2026-10-14 | 2 of 26 items failed late in a big batch, looked like app failures | device `/data` was 100% full (139M avail, 112 ENOSPC lines incl. persist writes) | check `df` free space when late-batch items fail; free space before big-batch tests.
- 2026-10-14 | ANR traces file visible in `/data/anr` but `cat` denied (system-only) | main-thread stack unrecoverable without root | attribute ANRs from logcat timing (named main-thread calls + Choreographer/Davey) plus code thread audit instead.
- 2026-10-14 | Every Gradle build died at settings-eval with kotlin-dsl `AbstractKotlinPluginWrapper` instrumentation failure; jars/distro/metadata all verified intact | a truncated 65MB instrumented-KGP jar in `caches/9.7.1/transforms` (no zip EOCD, written by the earlier build killed at wrapper timeout) was trusted as up-to-date | when the toolchain breaks with intact sources, sweep transform outputs for bad zips; never kill a build wrapper mid-run, it truncates cache writes.
- 2026-10-14 | "Add selected" merged 0 with "Nothing new": requeue→activate→merge let a synchronous bound pump complete the empty batch before the merge | merge into QUEUED first, activate only after, revert to DONE when nothing merged | order state changes so work exists before the pump can observe the state.

## Obsolete

(none yet)
