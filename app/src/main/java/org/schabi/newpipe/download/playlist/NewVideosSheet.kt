/*
 * SPDX-FileCopyrightText: 2026 NewPipe e.V. <https://newpipe-ev.de>
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

package org.schabi.newpipe.download.playlist

import android.os.Bundle
import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import androidx.fragment.app.DialogFragment
import androidx.recyclerview.widget.LinearLayoutManager
import org.schabi.newpipe.R
import org.schabi.newpipe.databinding.DialogNewVideosBinding

/**
 * Lets the user pick which new videos found by "Check for new videos" to add.
 * The list shows playlist positions + titles, starts fully unchecked, and uses
 * a RecyclerView so 100+ items stay smooth. Rotation is safe: the offer lives
 * in the manager, not in the fragment arguments.
 */
class NewVideosSheet : DialogFragment() {

    companion object {
        private const val ARG_BATCH_ID = "batch_id"
        private const val ARG_BATCH_NAME = "batch_name"

        fun newInstance(batchId: String, batchName: String): NewVideosSheet {
            val sheet = NewVideosSheet()
            sheet.arguments = Bundle().apply {
                putString(ARG_BATCH_ID, batchId)
                putString(ARG_BATCH_NAME, batchName)
            }
            return sheet
        }
    }

    private var _binding: DialogNewVideosBinding? = null
    private val binding get() = _binding!!

    private lateinit var manager: BatchDownloadManager
    private lateinit var adapter: NewVideosAdapter
    private var batchId: String = ""
    private var added = false

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        manager = BatchDownloadManager.getInstance(requireContext())
        batchId = requireArguments().getString(ARG_BATCH_ID).orEmpty()
    }

    override fun onCreateView(
        inflater: LayoutInflater,
        container: ViewGroup?,
        savedInstanceState: Bundle?
    ): View {
        _binding = DialogNewVideosBinding.inflate(inflater, container, false)
        return binding.root
    }

    override fun onViewCreated(view: View, savedInstanceState: Bundle?) {
        super.onViewCreated(view, savedInstanceState)
        val items = manager.pendingNewVideos(batchId)
        if (items == null) {
            dismiss()
            return
        }
        binding.newVideosTitleView.text = getString(
            R.string.playlist_batch_new_videos_title,
            requireArguments().getString(ARG_BATCH_NAME).orEmpty()
        )
        adapter = NewVideosAdapter { selected, total -> updateCount(selected, total) }
        binding.newVideosList.layoutManager = LinearLayoutManager(requireContext())
        binding.newVideosList.adapter = adapter
        adapter.setItems(items)
        binding.newVideosSelectAllButton.setOnClickListener { adapter.selectAll() }
        binding.newVideosSelectNoneButton.setOnClickListener { adapter.selectNone() }
        binding.newVideosAddButton.setOnClickListener {
            added = true
            manager.addNewVideos(batchId, adapter.selectedItems())
            dismiss()
        }
        binding.newVideosCancelButton.setOnClickListener { dismiss() }
    }

    override fun onStart() {
        super.onStart()
        dialog?.window?.setLayout(
            ViewGroup.LayoutParams.MATCH_PARENT,
            (resources.displayMetrics.heightPixels * 0.8).toInt()
        )
    }

    override fun onDestroyView() {
        if (!added && activity?.isChangingConfigurations != true) {
            manager.discardNewVideos(batchId)
        }
        _binding = null
        super.onDestroyView()
    }

    private fun updateCount(selected: Int, total: Int) {
        binding.newVideosCountView.text = getString(
            R.string.playlist_batch_new_videos_count,
            selected,
            total
        )
        binding.newVideosAddButton.isEnabled = selected > 0
    }
}
