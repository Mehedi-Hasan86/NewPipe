/*
 * SPDX-FileCopyrightText: 2026 NewPipe e.V. <https://newpipe-ev.de>
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

package org.schabi.newpipe.download.playlist

import android.view.LayoutInflater
import android.view.ViewGroup
import androidx.recyclerview.widget.RecyclerView
import org.schabi.newpipe.databinding.ItemNewVideoBinding

/**
 * Checkbox list of new videos in playlist order (position + title).
 * Everything starts unchecked; the whole row toggles.
 *
 * @param onSelectionChanged called with (selected, total) after every change
 */
class NewVideosAdapter(
    private val onSelectionChanged: (selected: Int, total: Int) -> Unit
) : RecyclerView.Adapter<NewVideosAdapter.ViewHolder>() {

    private var items: List<PlaylistBatchItem> = emptyList()
    private val checkedUrls = mutableSetOf<String>()

    init {
        setHasStableIds(true)
    }

    fun setItems(newItems: List<PlaylistBatchItem>) {
        items = newItems
        checkedUrls.clear()
        notifyDataSetChanged()
        onSelectionChanged(0, items.size)
    }

    fun selectAll() {
        checkedUrls.clear()
        checkedUrls.addAll(items.map { it.url })
        notifyItemRangeChanged(0, items.size)
        onSelectionChanged(checkedUrls.size, items.size)
    }

    fun selectNone() {
        checkedUrls.clear()
        notifyItemRangeChanged(0, items.size)
        onSelectionChanged(0, items.size)
    }

    fun selectedItems(): List<PlaylistBatchItem> {
        return items.filter { it.url in checkedUrls }
    }

    override fun getItemCount(): Int = items.size

    override fun getItemId(position: Int): Long = items[position].url.hashCode().toLong()

    override fun onCreateViewHolder(parent: ViewGroup, viewType: Int): ViewHolder {
        val binding = ItemNewVideoBinding.inflate(
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
        private val binding: ItemNewVideoBinding
    ) : RecyclerView.ViewHolder(binding.root) {

        init {
            binding.root.setOnClickListener {
                toggle(bindingAdapterPosition)
            }
        }

        private fun toggle(position: Int) {
            if (position == RecyclerView.NO_POSITION) {
                return
            }
            val url = items[position].url
            if (!checkedUrls.remove(url)) {
                checkedUrls.add(url)
            }
            notifyItemChanged(position)
            onSelectionChanged(checkedUrls.size, items.size)
        }

        fun bind(item: PlaylistBatchItem) {
            binding.newVideoCheckBox.isChecked = item.url in checkedUrls
            binding.newVideoSerialView.text = item.position.toString()
            binding.newVideoTitleView.text = item.title
        }
    }
}
