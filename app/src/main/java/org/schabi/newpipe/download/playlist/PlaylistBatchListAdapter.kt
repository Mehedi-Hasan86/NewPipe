/*
 * SPDX-FileCopyrightText: 2026 NewPipe e.V. <https://newpipe-ev.de>
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

package org.schabi.newpipe.download.playlist

import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import androidx.appcompat.widget.PopupMenu
import androidx.recyclerview.widget.RecyclerView
import org.schabi.newpipe.R
import org.schabi.newpipe.databinding.ItemPlaylistBatchRowBinding

/**
 * Rows of the batch list: one row per batch (ACTIVE first, then QUEUED, then DONE).
 * Tapping a row opens its items; the overflow menu offers retry failed, move up/down
 * (queued batches) and delete.
 */
class PlaylistBatchListAdapter(
    private val onSelect: (batchId: String) -> Unit,
    private val onRetryFailed: (batchId: String) -> Unit,
    private val onCheckNew: (batchId: String) -> Unit,
    private val onMoveQueued: (batchId: String, delta: Int) -> Unit,
    private val onDeleteBatch: (batchId: String, isActive: Boolean) -> Unit
) : RecyclerView.Adapter<PlaylistBatchListAdapter.ViewHolder>() {

    private var batches: List<BatchSummary> = emptyList()

    fun setBatches(summaries: List<BatchSummary>) {
        batches = summaries
        notifyDataSetChanged()
    }

    override fun getItemCount(): Int = batches.size

    override fun onCreateViewHolder(parent: ViewGroup, viewType: Int): ViewHolder {
        val binding = ItemPlaylistBatchRowBinding.inflate(
            LayoutInflater.from(parent.context),
            parent,
            false
        )
        return ViewHolder(binding)
    }

    override fun onBindViewHolder(holder: ViewHolder, position: Int) {
        holder.bind(batches[position])
    }

    inner class ViewHolder(
        private val binding: ItemPlaylistBatchRowBinding
    ) : RecyclerView.ViewHolder(binding.root) {

        fun bind(summary: BatchSummary) {
            val context = binding.root.context
            binding.batchRowTitleView.text = summary.playlistName.ifBlank {
                context.getString(R.string.playlist_batch_state_no_batch)
            }
            val state = when (summary.queueState) {
                BatchQueueState.ACTIVE -> context.getString(R.string.playlist_batch_state_active)
                BatchQueueState.QUEUED -> context.getString(R.string.playlist_batch_state_queued)
                BatchQueueState.DONE -> context.getString(R.string.playlist_batch_state_done)
            }
            val counts = context.getString(
                R.string.playlist_batch_queue_counts,
                summary.completedCount,
                summary.totalCount,
                summary.failedCount
            )
            binding.batchRowStateView.text = "$state • $counts"
            binding.batchRowProgress.progress =
                (summary.overallFraction * 100).toInt().coerceIn(0, 100)
            binding.root.setOnClickListener { onSelect(summary.batchId) }
            binding.batchRowOverflowButton.setOnClickListener { anchor ->
                showMenu(anchor, summary)
            }
        }

        private fun showMenu(anchor: View, summary: BatchSummary) {
            val popup = PopupMenu(anchor.context, anchor)
            popup.menuInflater.inflate(R.menu.menu_playlist_batch_row, popup.menu)
            val queued = summary.queueState == BatchQueueState.QUEUED
            popup.menu.findItem(R.id.batch_row_retry_failed).isVisible = summary.failedCount > 0
            popup.menu.findItem(R.id.batch_row_move_up).isVisible = queued
            popup.menu.findItem(R.id.batch_row_move_down).isVisible = queued
            if (queued) {
                val queuedIds = batches
                    .filter { it.queueState == BatchQueueState.QUEUED }
                    .map { it.batchId }
                popup.menu.findItem(R.id.batch_row_move_up).isEnabled =
                    queuedIds.firstOrNull() != summary.batchId
                popup.menu.findItem(R.id.batch_row_move_down).isEnabled =
                    queuedIds.lastOrNull() != summary.batchId
            }
            popup.setOnMenuItemClickListener { item ->
                when (item.itemId) {
                    R.id.batch_row_retry_failed -> {
                        onRetryFailed(summary.batchId)
                        true
                    }

                    R.id.batch_row_check_new -> {
                        onCheckNew(summary.batchId)
                        true
                    }

                    R.id.batch_row_move_up -> {
                        onMoveQueued(summary.batchId, -1)
                        true
                    }

                    R.id.batch_row_move_down -> {
                        onMoveQueued(summary.batchId, 1)
                        true
                    }

                    R.id.batch_row_delete -> {
                        onDeleteBatch(
                            summary.batchId,
                            summary.queueState == BatchQueueState.ACTIVE
                        )
                        true
                    }

                    else -> false
                }
            }
            popup.show()
        }
    }
}
