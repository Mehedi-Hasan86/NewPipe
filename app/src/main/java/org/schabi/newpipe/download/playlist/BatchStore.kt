/*
 * SPDX-FileCopyrightText: 2026 NewPipe e.V. <https://newpipe-ev.de>
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

package org.schabi.newpipe.download.playlist

import android.content.Context
import java.io.File
import kotlinx.serialization.Serializable
import kotlinx.serialization.json.Json

/**
 * Per-item state inside a batch. Persisted as JSON (separate from newpipe.db,
 * so no Room schema change or migration is needed).
 */
@Serializable
enum class BatchItemStatus {
    PENDING,
    RESOLVING,
    DOWNLOADING,
    MERGING,
    COMPLETED,
    FAILED,
    SKIPPED,
    CANCELLED
}

/** Batch-level execution state (only meaningful while the batch is ACTIVE). */
@Serializable
enum class BatchStatus {
    RUNNING,
    PAUSED,
    WAITING_WIFI,
    COOLDOWN,
    COMPLETED,
    CANCELLED
}

/** Queue position of a batch. At most one batch is ACTIVE at a time. */
@Serializable
enum class BatchQueueState {
    ACTIVE,
    QUEUED,
    DONE
}

/** One queued video. [storageUri] is the target file URI once created. */
@Serializable
data class BatchItemRecord(
    val url: String,
    val title: String,
    val position: Int,
    val serviceId: Int,
    val uploaderName: String? = null,
    val uploadDateText: String? = null,
    val status: BatchItemStatus = BatchItemStatus.PENDING,
    val retryCount: Int = 0,
    val failureReason: String? = null,
    val storageUri: String? = null,
    val storageTag: String? = null,
    val missionKind: String = "?",
    val override: PerItemOverride? = null,
    val lastProgress: Double = 0.0
)

/** One batch. Each batch keeps the options captured at creation time. */
@Serializable
data class BatchRecord(
    val id: String,
    val playlistName: String,
    val playlistUrl: String,
    val serviceId: Int,
    val createdAt: Long = 0L,
    val status: BatchStatus = BatchStatus.RUNNING,
    val queueState: BatchQueueState = BatchQueueState.ACTIVE,
    val options: PlaylistDownloadOptions = PlaylistDownloadOptions(),
    val items: List<BatchItemRecord> = emptyList(),
    /** Items skipped for missing language under ASK_AT_END, awaiting the end dialog. */
    val askLanguageUrls: List<String> = emptyList()
)

/** Versioned on-disk shape: a sequential FIFO queue of batches (schema version 2). */
@Serializable
data class BatchQueueFile(
    val version: Int = CURRENT_QUEUE_VERSION,
    val batches: List<BatchRecord> = emptyList()
) {
    companion object {
        const val CURRENT_QUEUE_VERSION = 2
    }
}

/**
 * Persists the sequential FIFO queue of batches as JSON in app-private storage
 * (`filesDir/playlist_batch/batch_queue.json`), independent of newpipe.db.
 */
object BatchStore {
    private const val DIR_NAME = "playlist_batch"
    private const val FILE_NAME = "batch_queue.json"
    private const val LEGACY_FILE_NAME = "current_batch.json"

    private val json = Json {
        prettyPrint = true
        ignoreUnknownKeys = true
    }

    private fun storeDir(context: Context): File {
        val dir = File(context.filesDir, DIR_NAME)
        if (!dir.exists()) {
            dir.mkdirs()
        }
        return dir
    }

    /**
     * Loads the persisted batch queue (schema version 2). A legacy version 1
     * `current_batch.json` (single batch) is migrated into the list as the first
     * batch on first read, keeping every item and its state: terminal batches
     * become DONE, anything else becomes ACTIVE.
     *
     * @param context the Android context
     * @return the persisted batches in queue order, or empty when none exist
     */
    fun loadQueue(context: Context): List<BatchRecord> {
        val dir = storeDir(context)
        val file = File(dir, FILE_NAME)
        if (file.exists()) {
            try {
                val stored = json.decodeFromString(
                    BatchQueueFile.serializer(),
                    file.readText()
                )
                return stored.batches
            } catch (e: Exception) {
                return emptyList()
            }
        }
        val legacy = File(dir, LEGACY_FILE_NAME)
        if (!legacy.exists()) {
            return emptyList()
        }
        return try {
            val single = json.decodeFromString(BatchRecord.serializer(), legacy.readText())
            val queueState = if (single.status == BatchStatus.COMPLETED ||
                single.status == BatchStatus.CANCELLED
            ) {
                BatchQueueState.DONE
            } else {
                BatchQueueState.ACTIVE
            }
            listOf(single.copy(queueState = queueState))
        } catch (e: Exception) {
            emptyList()
        }
    }

    /**
     * Atomically writes [batches] (temp file + rename). A leftover legacy version 1
     * file is deleted after a successful write so it is never migrated twice.
     *
     * @param context the Android context
     * @param batches the batches in queue order
     */
    fun saveQueue(context: Context, batches: List<BatchRecord>) {
        val dir = storeDir(context)
        val file = File(dir, FILE_NAME)
        val tmp = File(dir, "$FILE_NAME.tmp")
        tmp.writeText(
            json.encodeToString(BatchQueueFile.serializer(), BatchQueueFile(batches = batches))
        )
        if (!tmp.renameTo(file)) {
            file.delete()
            tmp.renameTo(file)
        }
        File(dir, LEGACY_FILE_NAME).delete()
    }

    /** Deletes both the version 2 queue file and any legacy version 1 file, if any. */
    fun clearQueue(context: Context) {
        val dir = storeDir(context)
        File(dir, FILE_NAME).delete()
        File(dir, LEGACY_FILE_NAME).delete()
    }
}
