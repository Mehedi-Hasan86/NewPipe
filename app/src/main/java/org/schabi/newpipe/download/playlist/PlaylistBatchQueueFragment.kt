/*
 * SPDX-FileCopyrightText: 2026 NewPipe e.V. <https://newpipe-ev.de>
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

package org.schabi.newpipe.download.playlist

import android.app.Activity
import android.os.Bundle
import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import android.widget.Toast
import androidx.activity.OnBackPressedCallback
import androidx.activity.result.contract.ActivityResultContracts
import androidx.appcompat.app.AlertDialog
import androidx.fragment.app.Fragment
import androidx.recyclerview.widget.ItemTouchHelper
import androidx.recyclerview.widget.LinearLayoutManager
import androidx.recyclerview.widget.RecyclerView
import org.schabi.newpipe.R
import org.schabi.newpipe.databinding.FragmentPlaylistBatchQueueBinding

/**
 * Batch queue screen: sequential FIFO list of batches (title, item count, state,
 * progress for the active one). Tapping a batch opens its items in the reused item
 * screen. Queued batches offer cancel, move up and move down. Pause/resume/cancel
 * all and the per-item retry/cancel buttons apply to the ACTIVE batch only; the
 * existing Downloads screen keeps working as before.
 */
class PlaylistBatchQueueFragment : Fragment() {

    companion object {
        @JvmStatic
        fun newInstance(): PlaylistBatchQueueFragment {
            return PlaylistBatchQueueFragment()
        }
    }

    private var _binding: FragmentPlaylistBatchQueueBinding? = null
    private val binding get() = _binding!!

    private lateinit var manager: BatchDownloadManager
    private lateinit var batchAdapter: PlaylistBatchListAdapter
    private lateinit var adapter: PlaylistBatchQueueAdapter
    private var pendingFolderKey: String? = null

    /** Id of the batch shown in the item view; null shows the batch list. */
    private var selectedBatchId: String? = null
    private var dragEnabled = true

    private val folderPickerLauncher = registerForActivityResult(
        ActivityResultContracts.StartActivityForResult()
    ) { result ->
        if (result.resultCode != Activity.RESULT_OK) {
            return@registerForActivityResult
        }
        val uri = result.data?.data ?: return@registerForActivityResult
        val key = pendingFolderKey ?: return@registerForActivityResult
        pendingFolderKey = null
        manager.onFolderPicked(key, uri)
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        manager = BatchDownloadManager.getInstance(requireContext())
    }

    override fun onCreateView(
        inflater: LayoutInflater,
        container: ViewGroup?,
        savedInstanceState: Bundle?
    ): View {
        _binding = FragmentPlaylistBatchQueueBinding.inflate(inflater, container, false)
        return binding.root
    }

    override fun onViewCreated(view: View, savedInstanceState: Bundle?) {
        super.onViewCreated(view, savedInstanceState)

        batchAdapter = PlaylistBatchListAdapter(
            onSelect = { batchId -> selectBatch(batchId) },
            onCancelQueued = { batchId -> confirmCancelQueued(batchId) },
            onMoveQueued = { batchId, delta -> manager.moveQueuedBatch(batchId, delta) }
        )
        binding.batchList.layoutManager = LinearLayoutManager(requireContext())
        binding.batchList.adapter = batchAdapter

        adapter = PlaylistBatchQueueAdapter(
            onRetry = { url -> manager.retryItem(url) },
            onCancel = { url -> manager.cancelItem(url) }
        )
        binding.queueList.layoutManager = LinearLayoutManager(requireContext())
        binding.queueList.adapter = adapter
        ItemTouchHelper(DragCallback()).attachToRecyclerView(binding.queueList)

        binding.pauseResumeButton.setOnClickListener { togglePause() }
        binding.cancelAllButton.setOnClickListener { confirmCancelAll() }
        binding.pickFolderButton.setOnClickListener { pickFolder() }
        binding.filterAllButton.setOnClickListener {
            adapter.setFilter(PlaylistBatchQueueAdapter.Filter.ALL)
        }
        binding.filterCompletedButton.setOnClickListener {
            adapter.setFilter(PlaylistBatchQueueAdapter.Filter.COMPLETED)
        }
        binding.filterFailedButton.setOnClickListener {
            adapter.setFilter(PlaylistBatchQueueAdapter.Filter.FAILED)
        }

        requireActivity().onBackPressedDispatcher.addCallback(
            viewLifecycleOwner,
            object : OnBackPressedCallback(true) {
                override fun handleOnBackPressed() {
                    if (selectedBatchId != null) {
                        selectedBatchId = null
                        showList()
                    } else {
                        isEnabled = false
                        requireActivity().onBackPressedDispatcher.onBackPressed()
                    }
                }
            }
        )

        // The queue reads persisted state from the manager (survives restarts).
        manager.batchList.observe(viewLifecycleOwner) { summaries ->
            renderBatchList(summaries)
        }
        manager.uiState.observe(viewLifecycleOwner) { state ->
            val selected = selectedBatchId
            if (selected != null && selected == state.batchId) {
                render(state)
            }
        }
        manager.messageEvent.observe(viewLifecycleOwner) { event ->
            event.consume()?.let { text ->
                Toast.makeText(requireContext(), text, Toast.LENGTH_LONG).show()
            }
        }
        manager.folderEvent.observe(viewLifecycleOwner) { event ->
            event.consume()?.let { prefKey ->
                pendingFolderKey = prefKey
                pickFolder()
            }
        }
        manager.askLanguageEvent.observe(viewLifecycleOwner) { event ->
            event.consume()?.let { urls -> showAskLanguageDialog(urls) }
        }
        manager.restoreIfNeeded()
        if (selectedBatchId != null) {
            selectBatch(selectedBatchId!!)
        } else {
            showList()
        }
    }

    override fun onDestroyView() {
        _binding = null
        super.onDestroyView()
    }

    // ------------------------------------------------------------------
    // Batch list
    // ------------------------------------------------------------------

    private fun renderBatchList(summaries: List<BatchSummary>) {
        if (_binding == null) {
            return
        }
        batchAdapter.setBatches(summaries)
        binding.batchList.visibility = if (summaries.isNotEmpty()) View.VISIBLE else View.GONE
        binding.batchesEmptyView.visibility =
            if (summaries.isNotEmpty()) View.GONE else View.VISIBLE
        val selected = selectedBatchId
        if (selected != null) {
            if (summaries.none { it.batchId == selected }) {
                selectedBatchId = null
                showList()
                return
            }
            val activeId = summaries
                .firstOrNull { it.queueState == BatchQueueState.ACTIVE }?.batchId
            if (selected != activeId) {
                manager.itemsSnapshot(selected)?.let { render(it) }
            }
        }
        refreshItemActions()
    }

    private fun selectBatch(batchId: String) {
        selectedBatchId = batchId
        showItems()
        val live = manager.uiState.value
        if (live != null && live.batchId == batchId) {
            render(live)
        } else {
            manager.itemsSnapshot(batchId)?.let { render(it) }
        }
    }

    private fun showList() {
        binding.batchListView.visibility = View.VISIBLE
        binding.batchItemView.visibility = View.GONE
    }

    private fun showItems() {
        binding.batchListView.visibility = View.GONE
        binding.batchItemView.visibility = View.VISIBLE
    }

    /**
     * Item actions (pause/resume/cancel all, per-item retry/cancel, drag reorder)
     * apply to the ACTIVE batch only; they are hidden while another batch is shown.
     */
    private fun refreshItemActions() {
        val summaries = manager.batchList.value.orEmpty()
        val activeId = summaries
            .firstOrNull { it.queueState == BatchQueueState.ACTIVE }?.batchId
        val viewingActive = selectedBatchId != null && selectedBatchId == activeId
        adapter.setActionsEnabled(viewingActive)
        dragEnabled = viewingActive
        val controls = if (viewingActive) View.VISIBLE else View.GONE
        binding.pauseResumeButton.visibility = controls
        binding.cancelAllButton.visibility = controls
        if (!viewingActive) {
            binding.pickFolderButton.visibility = View.GONE
        }
    }

    private fun confirmCancelQueued(batchId: String) {
        AlertDialog.Builder(requireContext())
            .setMessage(R.string.playlist_batch_confirm_cancel_batch)
            .setPositiveButton(R.string.playlist_batch_cancel_batch) { _, _ ->
                manager.cancelQueuedBatch(batchId)
            }
            .setNegativeButton(R.string.cancel, null)
            .show()
    }

    // ------------------------------------------------------------------
    // Item view (reused item screen)
    // ------------------------------------------------------------------

    private fun render(state: BatchUiState) {
        if (_binding == null) {
            return
        }
        binding.queuePlaylistNameView.text = state.playlistName
        binding.queueCountsView.text = getString(
            R.string.playlist_batch_queue_counts,
            state.completedCount,
            state.totalCount,
            state.failedCount
        )
        binding.queueOverallProgress.progress =
            (state.overallFraction * 100).toInt().coerceIn(0, 100)
        binding.queueStatusView.text = batchStatusText(state)
        binding.queueDetailStatusView.text = detailStatusText(state)
        binding.pauseResumeButton.text = getString(
            if (state.batchStatus == BatchStatus.PAUSED ||
                state.batchStatus == BatchStatus.WAITING_WIFI
            ) {
                R.string.playlist_batch_resume
            } else {
                R.string.playlist_batch_pause
            }
        )
        val terminal = state.batchStatus == null ||
            state.batchStatus == BatchStatus.COMPLETED ||
            state.batchStatus == BatchStatus.CANCELLED
        binding.pauseResumeButton.isEnabled = !terminal && state.totalCount > 0
        binding.cancelAllButton.isEnabled = !terminal && state.totalCount > 0
        binding.pickFolderButton.visibility =
            if (state.needsFolder) View.VISIBLE else View.GONE

        val hasItems = state.items.isNotEmpty()
        binding.queueList.visibility = if (hasItems) View.VISIBLE else View.GONE
        binding.queueEmptyView.visibility = if (hasItems) View.GONE else View.VISIBLE

        adapter.setItems(state.items)
        refreshItemActions()
    }

    private fun batchStatusText(state: BatchUiState): String {
        return when (state.batchStatus) {
            BatchStatus.RUNNING -> getString(R.string.playlist_batch_state_running)
            BatchStatus.PAUSED -> getString(R.string.playlist_batch_state_paused)
            BatchStatus.WAITING_WIFI -> getString(R.string.playlist_batch_waiting_wifi)
            BatchStatus.COOLDOWN -> getString(R.string.playlist_batch_cooldown)
            BatchStatus.COMPLETED -> getString(R.string.playlist_batch_state_batch_completed)
            BatchStatus.CANCELLED -> getString(R.string.playlist_batch_state_batch_cancelled)
            null -> getString(R.string.playlist_batch_state_no_batch)
        }
    }

    private fun detailStatusText(state: BatchUiState): String {
        val parts = mutableListOf<String>()
        val pending = state.items.count { it.status == BatchItemStatus.PENDING }
        val resolving = state.items.count { it.status == BatchItemStatus.RESOLVING }
        val downloading = state.items.count { it.status == BatchItemStatus.DOWNLOADING }
        val merging = state.items.count { it.status == BatchItemStatus.MERGING }
        val completed = state.completedCount
        val failed = state.failedCount

        if (resolving > 0) parts.add(getString(R.string.playlist_batch_detail_resolving, resolving))
        if (downloading > 0) parts.add(getString(R.string.playlist_batch_detail_downloading, downloading))
        if (merging > 0) parts.add(getString(R.string.playlist_batch_detail_merging, merging))
        if (pending > 0) parts.add(getString(R.string.playlist_batch_detail_pending, pending))

        if (state.waitingWifi) parts.add(getString(R.string.playlist_batch_waiting_wifi))
        if (state.needsFolder) parts.add(getString(R.string.playlist_batch_needs_folder))

        return parts.joinToString(" • ")
    }

    private fun togglePause() {
        val state = manager.uiState.value ?: return
        if (state.batchStatus == BatchStatus.PAUSED ||
            state.batchStatus == BatchStatus.WAITING_WIFI
        ) {
            manager.resumeBatch()
        } else {
            manager.pauseBatch()
        }
    }

    private fun confirmCancelAll() {
        AlertDialog.Builder(requireContext())
            .setMessage(R.string.playlist_batch_confirm_cancel_all)
            .setPositiveButton(R.string.playlist_batch_cancel_all) { _, _ ->
                manager.cancelBatch()
            }
            .setNegativeButton(R.string.cancel, null)
            .show()
    }

    private fun pickFolder() {
        try {
            folderPickerLauncher.launch(manager.folderPickerIntent())
        } catch (e: Exception) {
            Toast.makeText(
                requireContext(),
                R.string.playlist_batch_pick_folder_failed,
                Toast.LENGTH_LONG
            ).show()
        }
    }

    private fun showAskLanguageDialog(urls: List<String>) {
        AlertDialog.Builder(requireContext())
            .setTitle(R.string.playlist_batch_ask_language_title)
            .setMessage(
                resources.getQuantityString(
                    R.plurals.playlist_batch_ask_language_message,
                    urls.size,
                    urls.size
                )
            )
            .setPositiveButton(R.string.playlist_batch_ask_language_retry) { _, _ ->
                manager.retrySkippedWithOriginal(urls)
            }
            .setNegativeButton(R.string.cancel, null)
            .show()
    }

    private inner class DragCallback : ItemTouchHelper.SimpleCallback(
        ItemTouchHelper.UP or ItemTouchHelper.DOWN,
        0
    ) {
        override fun onMove(
            recyclerView: RecyclerView,
            viewHolder: RecyclerView.ViewHolder,
            target: RecyclerView.ViewHolder
        ): Boolean {
            if (!dragEnabled) {
                return false
            }
            val fromUrl = adapter.urlAt(viewHolder.bindingAdapterPosition) ?: return false
            val toUrl = adapter.urlAt(target.bindingAdapterPosition) ?: return false
            val from = adapter.pendingIndex(fromUrl)
            val to = adapter.pendingIndex(toUrl)
            if (from < 0 || to < 0) {
                return false
            }
            manager.movePending(from, to)
            return true
        }

        override fun getMovementFlags(
            recyclerView: RecyclerView,
            viewHolder: RecyclerView.ViewHolder
        ): Int {
            return if (dragEnabled && adapter.isPendingAt(viewHolder.bindingAdapterPosition)) {
                super.getMovementFlags(recyclerView, viewHolder)
            } else {
                0
            }
        }

        override fun onSwiped(viewHolder: RecyclerView.ViewHolder, direction: Int) {
            // no swipe actions
        }
    }
}
