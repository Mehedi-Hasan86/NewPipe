/*
 * SPDX-FileCopyrightText: 2026 NewPipe e.V. <https://newpipe-ev.de>
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

package org.schabi.newpipe.download.playlist

import android.view.LayoutInflater
import android.view.ViewGroup
import androidx.recyclerview.widget.RecyclerView
import org.schabi.newpipe.R
import org.schabi.newpipe.databinding.ItemPlaylistBatchBinding
import org.schabi.newpipe.util.Localization
import org.schabi.newpipe.util.image.CoilHelper

/**
 * Checkbox list of playlist items in loaded order, with per-item options
 * and an audio-language chip once the item's stream info was fetched.
 */
class PlaylistBatchAdapter(
    private val onToggle: (position: Int) -> Unit,
    private val onOptions: (PlaylistBatchItem) -> Unit
) : RecyclerView.Adapter<PlaylistBatchAdapter.ViewHolder>() {

    private var items: List<PlaylistBatchItem> = emptyList()
    private var audioCounts: Map<String, Int> = emptyMap()

    fun setItems(newItems: List<PlaylistBatchItem>) {
        items = newItems
        notifyDataSetChanged()
    }

    fun setAudioCounts(counts: Map<String, Int>) {
        audioCounts = counts
        notifyDataSetChanged()
    }

    override fun getItemCount(): Int = items.size

    override fun onCreateViewHolder(parent: ViewGroup, viewType: Int): ViewHolder {
        val binding = ItemPlaylistBatchBinding.inflate(
            LayoutInflater.from(parent.context),
            parent,
            false
        )
        return ViewHolder(binding)
    }

    override fun onBindViewHolder(holder: ViewHolder, position: Int) {
        holder.bind(items[position])
    }

    inner class ViewHolder(
        private val binding: ItemPlaylistBatchBinding
    ) : RecyclerView.ViewHolder(binding.root) {

        fun bind(item: PlaylistBatchItem) {
            binding.itemCheckBox.isChecked = item.selected
            binding.itemSerialView.text = item.position.toString()
            binding.itemTitleView.text = item.title

            val meta = buildString {
                if (item.durationSecs > 0) {
                    append(Localization.getDurationString(item.durationSecs))
                }
                if (!item.uploaderName.isNullOrBlank()) {
                    if (isNotEmpty()) {
                        append(" • ")
                    }
                    append(item.uploaderName)
                }
            }
            binding.itemUploaderView.text = meta
            binding.itemUploaderView.visibility =
                if (meta.isEmpty()) {
                    android.view.View.GONE
                } else {
                    android.view.View.VISIBLE
                }

            if (item.durationSecs > 0) {
                binding.itemDurationView.text =
                    Localization.getDurationString(item.durationSecs)
                binding.itemDurationView.visibility = android.view.View.VISIBLE
            } else {
                binding.itemDurationView.visibility = android.view.View.GONE
            }

            CoilHelper.loadThumbnail(binding.itemThumbnailView, item.thumbnails)

            val audioCount = audioCounts[item.url] ?: 0
            if (audioCount > 1) {
                binding.itemAudioChipView.visibility = android.view.View.VISIBLE
                binding.itemAudioChipView.text = binding.root.resources.getQuantityString(
                    R.plurals.playlist_batch_audio_tracks,
                    audioCount,
                    audioCount
                )
            } else {
                binding.itemAudioChipView.visibility = android.view.View.GONE
            }

            binding.root.setOnClickListener { onToggle(item.position) }
            binding.itemOptionsButton.setOnClickListener { onOptions(item) }
        }
    }
}
