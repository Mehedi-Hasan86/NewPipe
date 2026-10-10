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
import org.schabi.newpipe.databinding.ItemPlaylistBatchQueueBinding

/**
 * Rows of the batch queue screen (STEP 3). Progress, speed and ETA come from
 * the polled [BatchItemUiState]; the existing Downloads screen is untouched.
 */
class PlaylistBatchQueueAdapter(
    private val onRetry: (url: String) -> Unit,
    private val onCancel: (url: String) -> Unit
) : RecyclerView.Adapter<PlaylistBatchQueueAdapter.ViewHolder>() {

    enum class Filter {
        ALL,
        COMPLETED,
        FAILED
    }

    private var allItems: List<BatchItemUiState> = emptyList()
    private var visibleItems: List<BatchItemUiState> = emptyList()
    private var filter: Filter = Filter.ALL

    fun setItems(items: List<BatchItemUiState>) {
        allItems = items
        applyFilter()
    }

    fun setFilter(newFilter: Filter) {
        filter = newFilter
        applyFilter()
    }

    /** Adapter position -> item URL (for drag handling). */
    fun urlAt(position: Int): String? = visibleItems.getOrNull(position)?.url

    /** Index of [url] among PENDING items (for [BatchDownloadManager.movePending]). */
    fun pendingIndex(url: String): Int {
        return allItems.filter { it.status == BatchItemStatus.PENDING }
            .indexOfFirst { it.url == url }
    }

    fun isPendingAt(position: Int): Boolean {
        return visibleItems.getOrNull(position)?.status == BatchItemStatus.PENDING
    }

    private fun applyFilter() {
        visibleItems = when (filter) {
            Filter.ALL -> allItems

            Filter.COMPLETED -> allItems.filter { it.status == BatchItemStatus.COMPLETED }

            Filter.FAILED -> allItems.filter {
                it.status == BatchItemStatus.FAILED || it.status == BatchItemStatus.SKIPPED
            }
        }
        notifyDataSetChanged()
    }

    override fun getItemCount(): Int = visibleItems.size

    override fun onCreateViewHolder(parent: ViewGroup, viewType: Int): ViewHolder {
        val binding = ItemPlaylistBatchQueueBinding.inflate(
            LayoutInflater.from(parent.context),
            parent,
            false
        )
        return ViewHolder(binding)
    }

    override fun onBindViewHolder(holder: ViewHolder, position: Int) {
        holder.bind(visibleItems[position])
    }

    inner class ViewHolder(
        private val binding: ItemPlaylistBatchQueueBinding
    ) : RecyclerView.ViewHolder(binding.root) {

        fun bind(item: BatchItemUiState) {
            val context = binding.root.context
            binding.rowTitleView.text = context.getString(
                R.string.playlist_batch_queue_row_title,
                item.position,
                item.title
            )
            binding.rowStatusView.text = statusText(item)
            binding.rowProgressBar.isIndeterminate = item.status == BatchItemStatus.RESOLVING
            binding.rowProgressBar.progress = (item.fraction * 100).toInt().coerceIn(0, 100)

            if ((
                    item.status == BatchItemStatus.FAILED ||
                        item.status == BatchItemStatus.SKIPPED
                    ) &&
                !item.errorText.isNullOrBlank()
            ) {
                binding.rowErrorView.visibility = View.VISIBLE
                binding.rowErrorView.text = item.errorText
            } else {
                binding.rowErrorView.visibility = View.GONE
            }

            val retryable = item.status == BatchItemStatus.FAILED ||
                item.status == BatchItemStatus.SKIPPED
            binding.rowRetryButton.visibility = if (retryable) View.VISIBLE else View.GONE
            binding.rowRetryButton.setOnClickListener { onRetry(item.url) }

            val cancellable = item.status == BatchItemStatus.PENDING ||
                item.status == BatchItemStatus.RESOLVING ||
                item.status == BatchItemStatus.DOWNLOADING ||
                item.status == BatchItemStatus.MERGING
            binding.rowCancelButton.visibility = if (cancellable) View.VISIBLE else View.GONE
            binding.rowCancelButton.setOnClickListener { onCancel(item.url) }
        }

        private fun statusText(item: BatchItemUiState): String {
            val context = binding.root.context
            val base = when (item.status) {
                BatchItemStatus.PENDING -> context.getString(R.string.playlist_batch_state_pending)
                BatchItemStatus.RESOLVING -> context.getString(R.string.playlist_batch_state_resolving)
                BatchItemStatus.DOWNLOADING -> context.getString(R.string.playlist_batch_state_downloading)
                BatchItemStatus.MERGING -> context.getString(R.string.playlist_batch_state_merging)
                BatchItemStatus.COMPLETED -> context.getString(R.string.playlist_batch_state_completed)
                BatchItemStatus.FAILED -> context.getString(R.string.playlist_batch_state_failed)
                BatchItemStatus.SKIPPED -> context.getString(R.string.playlist_batch_state_skipped)
                BatchItemStatus.CANCELLED -> context.getString(R.string.playlist_batch_state_cancelled)
            }
            if (item.status != BatchItemStatus.DOWNLOADING &&
                item.status != BatchItemStatus.MERGING
            ) {
                return base
            }
            val parts = mutableListOf(base)
            if (item.speedBytesPerSec > 0) {
                parts.add(formatSpeed(context, item.speedBytesPerSec))
            }
            if (item.etaSeconds >= 0) {
                parts.add(context.getString(R.string.playlist_batch_eta, formatDuration(item.etaSeconds)))
            }
            return parts.joinToString(" • ")
        }

        private fun formatSpeed(context: android.content.Context, bytesPerSec: Long): String {
            return when {
                bytesPerSec >= 1024 * 1024 -> context.getString(R.string.playlist_batch_speed_mbps, bytesPerSec / (1024.0 * 1024.0))
                bytesPerSec >= 1024 -> context.getString(R.string.playlist_batch_speed_kbps, bytesPerSec / 1024.0)
                else -> context.getString(R.string.playlist_batch_speed_bps, bytesPerSec.toDouble())
            }
        }

        private fun formatDuration(totalSeconds: Long): String {
            val minutes = totalSeconds / 60
            val seconds = totalSeconds % 60
            return "%02d:%02d".format(minutes, seconds)
        }
    }
}
