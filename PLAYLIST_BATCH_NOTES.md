# Playlist Batch Download — Team Notes

Feature branch: `feature/playlist-batch-download` (forked from upstream `dev`).
Scope: download a whole playlist as sequentially numbered files, with a persistent
FIFO queue of batches managed from an in-app Batch Manager screen.

Only what the code actually does is documented below. Anything not proven on a
device or in code is marked **unverified**.

## 1. What the feature does

- **Intake** (`PlaylistBatchActivity` / `PlaylistBatchFragment`): opened from a
  playlist screen or with a pasted/shared playlist URL. Loads the playlist items
  via the extractor, with checkbox selection, select-all/none, range select, and
  a URL bar fallback.
- **Per-item options** (`PlaylistItemOptionsSheet`): per-video override of target
  resolution, container, and audio track (track id + locale language).
- **Global options** (`PlaylistBatchOptionsSheet`): video/audio-only mode, target
  resolution, container (MP4/WebM), audio container (M4A/Opus), preferred audio
  language + missing-language action (Original / Skip / Ask at end), subtitles,
  playlist subfolder on/off, filename template, duplicate policy
  (Overwrite / Rename / Skip), parallel videos (1–3), Wi-Fi only, max retries.
  Last-used options persist in SharedPreferences (`PlaylistBatchPrefs`).
- **Sequential FIFO queue of batches** (`BatchDownloadManager` + `BatchStore`):
  starting a batch while another runs queues it; each batch keeps the options
  captured at creation time. When every item of the active batch is terminal
  (completed/failed/skipped/cancelled), it is marked DONE and the oldest QUEUED
  batch starts automatically. Failed items never block the next batch.
- **Batch Manager screen** (`PlaylistBatchQueueActivity`, also reachable from the
  main drawer and the Downloads toolbar): batch list (title, item count,
  completed/failed counts, Active/Queued/Done state, progress), tap for the item
  screen (reused per-item progress, speed, ETA, retry/cancel, filters, pending
  reorder), overflow menu per batch (Retry failed, Move up/down for queued,
  Delete), toolbar actions (Add batch → intake, Clear finished), and a
  "Retry all failed" button in the item screen.
- **Retry failed**: resets a batch's FAILED items to pending with cleared counters;
  a DONE batch goes back to QUEUED (or ACTIVE immediately if nothing runs).
- **Delete**: queued/done batches are dropped; deleting the ACTIVE batch stops its
  missions first (without deleting files), drops the record, and advances the
  queue. Downloaded files are always kept.
- **Duplicate handling**: starting a playlist that is ACTIVE or QUEUED does not
  create a batch; the UI offers Open / Add-missing-only (appends unknown urls,
  resets FAILED ones). Re-starting a DONE playlist creates a new batch that
  reuses its folder and defaults to SKIP so existing files are kept.
- **Restart recovery**: after an app kill, the persisted queue is reloaded; the
  ACTIVE batch is reconciled against live service missions (finished → COMPLETED,
  running → DOWNLOADING, orphaned → PENDING for retry); QUEUED batches wait; if
  no ACTIVE batch exists, the oldest QUEUED one starts. In-memory state is loaded
  lazily (`ensureLoaded`) by every mutating entry point, so a fresh process can
  never overwrite the store before reading it.

## 2. Architecture

File map of `app/src/main/java/org/schabi/newpipe/download/playlist/`:

- `BatchDownloadManager.kt` — singleton orchestrator: FIFO queue, resolve →
  launch → poll loop, retries, Wi-Fi gate, cooldown, folder events, persistence.
- `BatchStore.kt` — JSON persistence (queue file v2, legacy migration, folder map).
- `PlaylistBatchFragment.kt` — intake + selection + start/duplicate dialog.
- `PlaylistBatchViewModel.kt` — playlist loading, paging, selection, overrides cache.
- `PlaylistBatchAdapter.kt` — intake item rows.
- `PlaylistBatchOptionsSheet.kt` — global options bottom sheet.
- `PlaylistItemOptionsSheet.kt` — per-item override dialog.
- `PlaylistBatchPrefs.kt` — last-used options in SharedPreferences.
- `PlaylistBatchResolver.kt` — stream picking (video/audio/subtitle), filename
  builder with sanitization.
- `PlaylistBatchUrlHelper.kt` — URL → playlist normalization + playlistKey.
- `PlaylistDownloadOptions.kt` — serializable options incl. captured roots.
- `PlaylistBatchItem.kt` — intake item model.
- `PlaylistBatchQueueFragment.kt` — batch list + reused item screen.
- `PlaylistBatchListAdapter.kt` — batch rows + overflow menu.
- `PlaylistBatchQueueAdapter.kt` — item rows (progress/retry/cancel/filter/drag).
- `PlaylistBatchActivity.kt` — intake host (URL args + share-intent fallback).
- `PlaylistBatchQueueActivity.kt` — queue host.

Reuse of the existing pipeline (unchanged): missions are created through
`DownloadManagerService.startMission(...)` with `StoredFileHelper` targets;
`DownloadManager` runs/pauses/deletes them; `DownloadMission` recovery,
`Postprocessing` (mux/demux/subtitle conversion) and progress events
(RUNNING/FINISHED/ERROR/PAUSED) are consumed as-is. No new downloader, no
FFmpeg, no new dependencies, no extractor changes, no `newpipe.db` changes.

Small hooks in existing classes:

- `MainActivity.java` — "Batch downloads" drawer item (`ITEM_ID_BATCH_DOWNLOADS`)
  opening the queue activity.
- `MissionsFragment.java` + `res/menu/download_menu.xml` — batch-downloads toolbar
  item in the Downloads screen opening the queue activity.
- `NavigationHelper.java` — `openPlaylistBatchDownload(...)` (intake, fragment or
  activity) and `openPlaylistBatchQueue(context)` (queue activity, guarded).
- `PlaylistFragment.java` — playlist screen entry into the intake screen.
- `StoredDirectoryHelper.java` — `getSubfolder()` (grant-safe subfolder handle),
  `createFileUnderGrantedTree()`, `consumeCreateError()`, empty-dir cleanup.
- `StoredFileHelper.java` — unchanged public API surface used for mission targets.
- `AndroidManifest.xml` — the two batch activities (`exported=false`).

## 3. Storage

- `BatchStore` keeps `filesDir/playlist_batch/batch_queue.json` (schema v2:
  `{version, batches[], folderNames{}}`), written atomically via temp file +
  rename. A legacy v1 `current_batch.json` (single batch) is migrated into the
  list on first read (terminal → DONE, else ACTIVE; key derived from its URL).
- `playlistKey` (`"<serviceId>:<playlistId>"`, blank when unparseable) is stored
  per batch, never shown in the UI. `folderNames` maps key → subfolder name: the
  same playlist always reuses its folder; a different key whose sanitized title
  collides gets `"Name (2)"`, `"Name (3)"`, … Key-less batches get unique folders.
- **SAF handling**: the subfolder handle stays addressed *through* the
  user-granted root tree (`tree/<ROOT>/document/<SUB>`). A nested tree URI
  (`tree/<SUB>/…`) carries no grant of its own — the provider rejects writes to
  it with `SecurityException`, and `takePersistableUriPermission` on it throws —
  so no derived tree URI is ever built or persisted. File creation calls
  `DocumentsContract.createDocument()` directly with the grant-covered parent
  URI (leaf document handles cannot create children), recording the root tree as
  the mission parent, exactly like single-video downloads.
- **Captured roots**: the video/audio download-folder URIs are copied into each
  batch's options at creation; later preference changes never move queued
  batches (preferences remain only as a legacy fallback).

## 4. Verified on device vs NOT verified

Device: physical phone, debug builds, log tag `PlaylistBatch`.

Verified:

- SAF subfolder file creation (files land in `playlist/` subfolders).
- FIFO order (B starts only after A is DONE, C after B).
- Batch-scoped attribution, i.e. no cross-batch mixing (every launch/finish log
  line pairs batch id, url and file path; files land in their own batch folder).
- Duplicate playlist merge (add-missing-only) and re-download SKIP default.
- Kill-and-resume (force-stop mid-download → in-flight mission re-attached as
  DOWNLOADING, pending items resume, no duplicate files, 3/3 green).
- Kill-recovery resume: `reconcileAfterRestart` resumes idle enqueued missions
  (`manager.resumeMission()`), no Play press needed; 30 s poll safety net as
  backstop (not triggered in tests).
- Corrupt store quarantine: garbage JSON file → `Log.e` "loadQueue failed" with
  full stack trace, `.corrupt-<ts>` file created, empty queue loaded, backup
  restore verified.
- Progress persist throttled to 10 s (per-write byte-count logged, state changes
  persist immediately).
- Move up/down, delete ACTIVE batch (missions stop, files kept, queue advances).
- Retry-failed machinery (DONE → QUEUED → ACTIVE → run).
- Auto-recovery after network loss (airplane-mode failures retried to 3/3 green).
- Persist works (per-write path/byte-count/rename/mtime logging; 7-batch store).
- Audio-language correctness on a dubbed playlist (preferred=original and
  preferred=hi verified; per-video "audio pick:" log line).
- Delete QUEUED batch (row dropped, no DONE entry, active batch unaffected).
- Clear finished with content (DONE rows removed in one action).
- Queue screen toolbar (Add batch, Clear finished) renders (own Toolbar, since
  the app theme has no action bar).
- Check for new videos: 5 new videos merged, batch reactivated (DONE → QUEUED →
  ACTIVE); nothing-new path returns no merge.

NOT verified:

- Manual retry tap on a failed item (nothing reachable ever FAILED on playable
  content; only auto-recovery observed).
- Same-title folder `"Name (2)"` (code reading only; needs two distinct
  same-titled playlists).
- Low-storage handling (warning path never triggered).
- Battery optimization / Doze.
- Progress loss after kill is within ~10 s (not measured on device).

Not applicable: NewPipe download notifications have no Pause/Cancel actions
(content intent only).

## 5. Known limitations

- Audio output is M4A or Opus only; MP3/FFmpeg are not supported (no new
  dependencies by design).
- No extractor changes: if YouTube changes its clients, both single and batch
  downloads break the same way.
- One ACTIVE batch at a time (`parallelVideos` 1–3 applies within it).
- DONE batches accumulate in the JSON store until deleted/cleared (no auto-prune).
- A single untagged audio track carries no locale info (logged as `?(?)` with
  original-fallback).
- The queue row does not show the chosen language (log only).

## 6. Before release checklist for maintainers

- Re-run every test in section 4 marked NOT verified.
- Remove or lower debug logs (`Log.d` with tag `PlaylistBatch`, especially the
  per-persist and `batch=` launch/finish lines) — keep `Log.e` failure paths.
- Update the NewPipeExtractor version if YouTube changed (downloads break
  otherwise).
- Review `applicationId`/`applicationIdSuffix` in `app/build.gradle.kts`: the
  debug suffix currently includes the git branch name, so the package id changes
  per branch. Do not ship that.
- Do not commit `settings.gradle.kts` local changes or the
  `app/src/main/res/values-iw` workaround (upstream keeps `values-iw` as a
  symlink to `values-he`, which Win32 JVMs cannot traverse).
- Keep GPLv3 and the SPDX headers.

## 7. How to build and test

Windows toolchain from WSL:

```powershell
powershell.exe -NoProfile -Command "cd <repo-root>; $env:JAVA_HOME='<JDK-21-path>'; .\gradlew.bat :app:assembleDebug --console=plain"
```

Install and verify (use the **exact** id from the build output, it varies by branch):

```powershell
& '<sdk-path>\platform-tools\adb.exe' install -r '<repo-root>\app\build\outputs\apk\debug\app-debug.apk'
# package id: app/build/outputs/apk/debug/output-metadata.json -> applicationId
& '<sdk-path>\platform-tools\adb.exe' logcat -d -s PlaylistBatch:V AndroidRuntime:E *:S
```

### values-iw on Windows

Upstream keeps `app/src/main/res/values-iw` as a symlink to `values-he`. On
Windows that symlink can be unreadable to Win32 JVMs, which breaks Gradle,
ktlint and AGP file walking (`FileSystemException ... values-iw`). The local
workaround (never commit it): replace the link with a plain copy of `values-he`
and mark it so git ignores further changes:

```
git update-index --skip-worktree app/src/main/res/values-iw
```

Notes from device work: `adb install -r` does not reliably kill the running
process — force-stop and confirm a fresh pid (or fresh `ensureLoaded` line)
before trusting a test; `pidof` proved unreliable on the test device, `ps -A`
plus in-log restore lines are the ground truth. Derive the package id from
`output-metadata.json` for every adb command — hardcoding it caused a full
evening of cross-package confusion (debug vs branch-suffixed installs each keep
their own data and log the same tag).

## 8. Suggested split if upstreaming as PRs

1. Intake/selection UI (`PlaylistBatchFragment`, adapter, ViewModel, URL helper,
   playlist-screen hook) — no downloading.
2. Options (options sheet, per-item sheet, prefs, serializable models).
3. Executor + queue (`BatchDownloadManager`, `BatchStore`, SAF subfolder work,
   queue screens, persistence, recovery).
4. Manager UI (drawer/Downloads entries, batch list actions, retry/delete/clear,
   duplicate handling, identity/folder map).
