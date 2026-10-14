# AGENTS.md — Working Rules for This Repo

## 1. Project

NewPipe fork. The playlist batch download feature lives in
`app/src/main/java/org/schabi/newpipe/download/playlist/` (manager, store,
intake/selection UI, options, queue UI). **Read `PLAYLIST_BATCH_NOTES.md`
first** — it documents what the feature does, the architecture, storage,
verified-vs-unverified tests, limitations, and the release checklist.

## 2. Hard constraints

- Never `git commit` / `git push` unless explicitly asked (and then: exact
  commands only, never `git add .` — explicit paths every time).
- Never edit: `app/src/main/res/values-iw`, `settings.gradle.kts`, the
  extractor, `shared/`, `desktopApp/`, or the `newpipe.db` schema.
- Never commit `settings.gradle.kts` or `values-iw` content.
- No new dependencies, no FFmpeg (M4A/Opus only, by design).
- No force push, no push to `origin` (upstream, no write access).

## 3. Build / test

- Windows toolchain from WSL via `powershell.exe`:
  `.\gradlew.bat :app:assembleDebug --console=plain` with the JBR 21 JDK.
- `checkstyle` (Javadoc `@param` on new Java methods) and `ktlint` (ASCII import
  order) must pass before reporting done. Run builds in background, poll the log.
- Install with `adb install -r <apk>`; `adb install -r` does **not** reliably
  kill the running app — force-stop and confirm a fresh process before trusting
  a test.

## 4. Device testing rules (lessons learned)

- Derive the package id from
  `app/build/outputs/apk/debug/output-metadata.json` → `applicationId` for
  EVERY adb command. The debug suffix includes the branch name, so the id
  changes per branch; hardcoding it once caused cross-package confusion (two
  installs logging the same tag, each with its own data).
- Do **not** trust `pid` or `pidof` (PID reuse/wraparound, unreliable `pidof`);
  trust in-log lines (`ensureLoaded`, `restoreIfNeeded`) and `ps -A`.
- One user action per round; verify it in the log before the next step. Replies
  routinely precede taps — an action without its log signature did not happen.
- Dump logcat promptly after each action; the buffer rotates. Poll `logcat -d`
  yourself instead of asking the user to save logs.
- Prefer `run-as <package>` reads of the store file as ground truth alongside
  logs; validate JSON before theorizing.

## 5. Debugging rules

- Never swallow exceptions silently — log with tag `PlaylistBatch`, full stack
  traces, root/file/mime/URI context at every stage.
- Separate **"proven by log/code"** from **"hypothesis"** in every report.
- Read the real exception before proposing a fix — including other tags (e.g.
  `DocumentsContract` swallows provider errors into `Log.w` and returns null).
- Reproduce before fixing; re-verify after fixing (log evidence, not claims).
- When evidence conflicts, suspect measurement first (timestamps, pids, buffer
  rotation, which package actually runs) before inventing mechanisms.

## 6. Known environment pitfalls

- `app/src/main/res/values-iw` is an upstream-committed symlink to `values-he`.
  As a WSL symlink it is unreadable to Win32 JVMs and breaks Gradle/ktlint/AGP
  file walking (`FileSystemException ... values-iw`). Local workaround (never
  commit): replace the link with a plain copy of `values-he` and mark it with
  `git update-index --skip-worktree`.
- WSL-side git operations can recreate the symlink; verify readability from
  Windows (`Get-ChildItem`) after any checkout/stash/rebase strangeness.
- Git identity (`user.name`/`user.email`) must be set before any rebase —
  rebasing stamps the committer on rewritten commits.

## 7. Known open items

- Unverified tests listed in `PLAYLIST_BATCH_NOTES.md` section 4 (delete QUEUED
  batch, Clear finished with content, manual retry tap, same-title `"Name (2)"`,
  dubbed-playlist audio, low storage, notification Pause/Cancel, Doze).
- Release prep from section 6 (remove/lower debug logs) is not done.

## 8. Decision log

- MP3 declined: M4A/Opus only, no FFmpeg, no new dependencies.
- One ACTIVE batch at a time; FIFO queue, oldest first; failed items never block.
- One folder per `playlistKey` (`<serviceId>:<playlistId>`); title collisions
  get `"Name (2)"`, `"Name (3)"`; download roots captured per batch at creation.
- Every mission/item lookup carries `batchId` (missions keyed by batch + URI);
  ask-language events carry the asking batch and are ignored when stale.
- Downloaded files are never deleted by batch actions (missions are stopped or
  forgotten, never file-deleted).

## 9. Self-improvement protocol

- At the end of every task, before reporting done, append to `LESSONS.md`
  (repo root) one entry per new lesson, format:
  `date | situation | what went wrong or worked | rule for next time`.
- Record a lesson when: a hypothesis turned out wrong, an environment pitfall
  cost time, a measurement was misleading (like the wrong package id), a fix
  needed more than one round, or the user corrected you.
- Before starting any task: read `AGENTS.md`, `PLAYLIST_BATCH_NOTES.md` and
  `LESSONS.md`. In the first reply, list which lessons from `LESSONS.md` apply
  to this task.
- Before proposing a fix: state the evidence (log line or code quote). Label
  each claim "proven" or "hypothesis".
- When a rule in `LESSONS.md` has been confirmed 3 times, promote it into
  `AGENTS.md` sections 3–6 and report the promotion.
- Never delete lessons; mark them "obsolete" with a reason instead.
- Keep each lesson to 1–3 lines. Do not invent lessons.

## 10. Current state (handoff)

- Branch `feature/playlist-batch-download`, tracking
  `myfork/feature/playlist-batch-download`; all batch work committed, see log.
- Uncommitted (deliberately never commit): `settings.gradle.kts` (line-ending
 -only local diff).
- Built 2026-10-14: debug APK 07:19 (JDK 21 `jbr-21.0.11`, BUILD SUCCESSFUL in
  11m 31s, 81 tasks / 13 executed; runCheckstyle clean, runKtlint clean).
  Installed + force-stopped, fresh process confirmed via `ps -A`.
- Device store: 4 batches, all COMPLETED (8, 6, 4 items + Shakira Mundial, 26
  items after a check-new merge of 24).
- Done 2026-10-14: 2-item batch to DONE (2/2); kill-recovery x2 (reconcile
  resume, one errCode=1008 retry to clean finish); check-new both paths
  (nothing-new + merge-24); low-storage survival partial (ENOSPC, no crash).
- Open: 08:31:46 ANR opening Downloads (`DownloadActivity`, input-dispatch
  timeout) — 6.5 s main-thread stall in Giga mission-recovery provider
  queries, batch code idle; aggravated by 100%-full disk. Investigated
  read-only; no fix yet. Traces file unreadable (system-only).
- New 2026-10-14: selectable "Check for new videos" (fetch → checkbox dialog
  → `addNewVideos`, playlist-position numbering). Built 15:32 (JDK 21,
  BUILD SUCCESSFUL, runCheckstyle + runKtlint executed). Installed.
  Verified: dialog lists unselected, select-2 merge DONE->ACTIVE as files
  4+5. Open tests: Cancel no-op, Select-all. Real bug fixed on the way:
  requeue→activate→merge raced a synchronous pump (merged=0); merge into
  QUEUED first, activate after.
- Next: Cancel + Select-all device tests; free-space + Retry-failed on the 2
  ENOSPC-failed items; release checklist per PLAYLIST_BATCH_NOTES.md
  section 6.
