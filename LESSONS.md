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
- 2026-10-11 | Toolbar MenuProvider actions never appeared on device | the queue
  activity uses the app NoActionBar theme so menus have nowhere to render; the
  user tapped Delete twice instead of an invisible Clear finished | verify menu
  UI actually renders on device before relying on it in tests.

## Obsolete

(none yet)
