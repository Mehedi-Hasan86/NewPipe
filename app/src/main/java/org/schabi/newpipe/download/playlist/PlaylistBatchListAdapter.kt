/*
 * SPDX-FileCopyrightText: 2026 NewPipe e.V. <https://newpipe-ev.de>
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

package org.schabi.newpipe.download.playlist

import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import androidx.recyclerview.widget.RecyclerView
import org.schabi.newpipe.R
import org.schabi.newpipe.databinding.ItemPlaylistBatchRowBinding

/**
 * Rows of the batch list: one row per batch (ACTIVE first, then QUEUED, then DONE).
 * Tapping a row opens its items; QUEUED rows offer cancel, move up and move down.
 */
class PlaylistBatchListAdapter(
    private val onSelect: (batchId: String) -> Unit,
    private val onCancelQueued: (batchId: String) -> Unit,
    private val onMoveQueued: (batchId: String, delta: Int) -> Unit
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

            val queued = summary.queueState == BatchQueueState.QUEUED
            binding.batchRowActions.visibility = if (queued) View.VISIBLE else View.GONE
            if (queued) {
                val queuedIds = batches
                    .filter { it.queueState == BatchQueueState.QUEUED }
                    .map { it.batchId }
                binding.batchRowUpButton.isEnabled =
                    queuedIds.firstOrNull() != summary.batchId
                binding.batchRowDownButton.isEnabled =
                    queuedIds.lastOrNull() != summary.batchId
                binding.batchRowCancelButton.setOnClickListener {
                    onCancelQueued(summary.batchId)
                }
                binding.batchRowUpButton.setOnClickListener {
                    onMoveQueued(summary.batchId, -1)
                }
                binding.batchRowDownButton.setOnClickListener {
                    onMoveQueued(summary.batchId, 1)
                }
            }
        }
    }
}
