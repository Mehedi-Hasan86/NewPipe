/*
 * SPDX-FileCopyrightText: 2026 NewPipe e.V. <https://newpipe-ev.de>
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

package org.schabi.newpipe.download.playlist

import android.os.Bundle
import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import android.widget.Toast
import androidx.fragment.app.Fragment
import androidx.lifecycle.ViewModelProvider
import androidx.recyclerview.widget.LinearLayoutManager
import androidx.recyclerview.widget.RecyclerView
import org.schabi.newpipe.R
import org.schabi.newpipe.databinding.FragmentPlaylistBatchBinding
import org.schabi.newpipe.extractor.ListExtractor
import org.schabi.newpipe.util.Localization
import org.schabi.newpipe.util.NO_SERVICE_ID

/**
 * STEP 1 only: playlist intake + item list + checkbox selection.
 * No downloading is started from here yet.
 */
class PlaylistBatchFragment : Fragment() {

    companion object {
        private const val ARG_SERVICE_ID = "playlist_batch_service_id"
        private const val ARG_URL = "playlist_batch_url"
        private const val ARG_NAME = "playlist_batch_name"

        @JvmStatic
        fun newInstance(serviceId: Int, url: String, name: String?): PlaylistBatchFragment {
            return PlaylistBatchFragment().apply {
                arguments = Bundle().apply {
                    putInt(ARG_SERVICE_ID, serviceId)
                    putString(ARG_URL, url)
                    putString(ARG_NAME, name)
                }
            }
        }
    }

    private var _binding: FragmentPlaylistBatchBinding? = null
    private val binding get() = _binding!!

    private lateinit var viewModel: PlaylistBatchViewModel
    private lateinit var adapter: PlaylistBatchAdapter

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        viewModel = ViewModelProvider(this)[PlaylistBatchViewModel::class.java]
    }

    override fun onCreateView(
        inflater: LayoutInflater,
        container: ViewGroup?,
        savedInstanceState: Bundle?
    ): View {
        _binding = FragmentPlaylistBatchBinding.inflate(inflater, container, false)
        return binding.root
    }

    override fun onViewCreated(view: View, savedInstanceState: Bundle?) {
        super.onViewCreated(view, savedInstanceState)

        adapter = PlaylistBatchAdapter(
            onToggle = { position -> viewModel.toggleSelection(position) },
            onOptions = { item -> openItemOptions(item) }
        )
        binding.itemsList.layoutManager = LinearLayoutManager(requireContext())
        binding.itemsList.adapter = adapter
        binding.itemsList.addOnScrollListener(object : RecyclerView.OnScrollListener() {
            override fun onScrolled(recyclerView: RecyclerView, dx: Int, dy: Int) {
                if (dy <= 0) {
                    return
                }
                val lm = recyclerView.layoutManager as? LinearLayoutManager ?: return
                val total = lm.itemCount
                val lastVisible = lm.findLastVisibleItemPosition()
                if (total > 0 && lastVisible >= total - 10 && viewModel.hasMorePages()) {
                    viewModel.loadMore()
                }
            }
        })

        binding.loadButton.setOnClickListener { loadFromInput() }
        binding.retryButton.setOnClickListener { viewModel.retry() }
        binding.selectAllButton.setOnClickListener { viewModel.selectAll() }
        binding.deselectAllButton.setOnClickListener { viewModel.deselectAll() }
        binding.rangeSelectButton.setOnClickListener { applyRangeSelection() }
        binding.downloadOptionsButton.setOnClickListener { openGlobalOptions() }

        viewModel.loadOptions()
        observeViewModel()
        loadInitial()
    }

    override fun onDestroyView() {
        _binding = null
        super.onDestroyView()
    }

    // ------------------------------------------------------------------

    private fun initialArgs(): Triple<Int, String, String?> {
        val args = arguments
        val serviceId = args?.getInt(ARG_SERVICE_ID, NO_SERVICE_ID)
            ?: NO_SERVICE_ID
        val url = args?.getString(ARG_URL).orEmpty()
        val name = args?.getString(ARG_NAME)
        return Triple(serviceId, url, name)
    }

    private fun loadInitial() {
        val (serviceId, url, name) = initialArgs()
        if (url.isNotBlank()) {
            if (!name.isNullOrBlank()) {
                binding.headerCard.visibility = View.VISIBLE
                binding.playlistNameView.text = name
            }
            binding.urlInput.setText(url)
            viewModel.load(serviceId, url)
        } else {
            // No URL yet: show the URL bar and an empty hint, wait for paste.
            binding.headerCard.visibility = View.GONE
        }
    }

    private fun loadFromInput() {
        val raw = binding.urlInput.text?.toString().orEmpty()
        if (raw.isBlank()) {
            Toast.makeText(requireContext(), R.string.playlist_batch_invalid_url, Toast.LENGTH_SHORT)
                .show()
            return
        }
        val resolved = PlaylistBatchUrlHelper.parsePastedText(raw)
        if (resolved == null) {
            val (serviceId, _) = initialArgs().let { it.first to it.second }
            // Fall back to raw input with current/default service; ViewModel maps errors.
            viewModel.load(serviceId, raw.trim(), true)
            return
        }
        binding.urlInput.setText(resolved.playlistUrl)
        viewModel.load(resolved.serviceId, resolved.playlistUrl, true)
    }

    private fun applyRangeSelection() {
        val from = binding.rangeFromInput.text?.toString()?.toIntOrNull()
        val to = binding.rangeToInput.text?.toString()?.toIntOrNull()
        if (from == null || to == null || from <= 0 || to <= 0) {
            Toast.makeText(
                requireContext(),
                R.string.playlist_batch_range_invalid,
                Toast.LENGTH_SHORT
            ).show()
            return
        }
        viewModel.selectRange(from, to)
    }

    private fun openGlobalOptions() {
        PlaylistBatchOptionsSheet.newInstance().show(childFragmentManager, "batch_options")
    }

    private fun openItemOptions(item: PlaylistBatchItem) {
        PlaylistItemOptionsSheet.newInstance(item.serviceId, item.url, item.title)
            .show(childFragmentManager, "batch_item_options")
    }

    // ------------------------------------------------------------------

    private fun observeViewModel() {
        viewModel.items.observe(viewLifecycleOwner) { items ->
            adapter.setItems(items)
            updateEmptyState()
        }

        viewModel.audioGroupCounts.observe(viewLifecycleOwner) { counts ->
            adapter.setAudioCounts(counts.orEmpty())
        }

        viewModel.header.observe(viewLifecycleOwner) { header ->
            if (header.name.isNotBlank()) {
                binding.headerCard.visibility = View.VISIBLE
                binding.playlistNameView.text = header.name
            } else if (binding.playlistNameView.text.isNullOrBlank()) {
                binding.headerCard.visibility =
                    if (viewModel.items.value.isNullOrEmpty()) View.GONE else View.VISIBLE
            }

            val countText = formatCount(header.totalCount, header.loadedCount, header.hasMore)
            val durationText = if (header.totalDurationSecs > 0) {
                Localization.getDurationString(
                    header.totalDurationSecs,
                    header.durationComplete,
                    true
                )
            } else {
                ""
            }
            binding.playlistMetaView.text = listOf(
                header.uploaderName.takeIf { it.isNotBlank() },
                countText.takeIf { it.isNotBlank() },
                durationText.takeIf { it.isNotBlank() }
            ).filterNotNull().joinToString(" • ")

            if (header.skippedCount > 0) {
                binding.skippedInfoView.visibility = View.VISIBLE
                binding.skippedInfoView.text = resources.getQuantityString(
                    R.plurals.playlist_batch_skipped,
                    header.skippedCount,
                    header.skippedCount
                )
            } else {
                binding.skippedInfoView.visibility = View.GONE
            }

            binding.loadingMoreProgress.visibility =
                if (viewModel.isLoadingMore.value == true) View.VISIBLE else View.GONE
            updateEmptyState()
        }

        viewModel.selectedCount.observe(viewLifecycleOwner) { count ->
            binding.selectedCountView.text = resources.getQuantityString(
                R.plurals.playlist_batch_selected,
                count,
                count
            )
        }

        viewModel.isLoading.observe(viewLifecycleOwner) { loading ->
            binding.loadingProgress.visibility = if (loading) View.VISIBLE else View.GONE
            if (loading) {
                binding.errorPanel.visibility = View.GONE
            }
            updateEmptyState()
        }

        viewModel.isLoadingMore.observe(viewLifecycleOwner) { loadingMore ->
            binding.loadingMoreProgress.visibility =
                if (loadingMore == true) View.VISIBLE else View.GONE
        }

        viewModel.errorKind.observe(viewLifecycleOwner) { kind ->
            val hasItems = !viewModel.items.value.isNullOrEmpty()
            if (kind != null && kind != PlaylistBatchViewModel.ErrorKind.NONE && !hasItems &&
                viewModel.isLoading.value != true
            ) {
                binding.errorPanel.visibility = View.VISIBLE
                binding.errorMessageView.text = errorText(kind, viewModel.errorMessage.value)
            } else {
                binding.errorPanel.visibility = View.GONE
            }
            updateEmptyState()
        }
    }

    private fun updateEmptyState() {
        val loading = viewModel.isLoading.value == true
        val hasItems = !viewModel.items.value.isNullOrEmpty()
        val hasError = viewModel.errorKind.value != null &&
            viewModel.errorKind.value != PlaylistBatchViewModel.ErrorKind.NONE &&
            !hasItems
        binding.emptyView.visibility =
            if (!loading && !hasItems && !hasError) View.VISIBLE else View.GONE
        binding.itemsList.visibility = if (hasItems) View.VISIBLE else View.GONE
    }

    private fun formatCount(total: Long, loaded: Int, hasMore: Boolean): String {
        return when (total) {
            ListExtractor.ITEM_COUNT_UNKNOWN -> {
                if (loaded > 0) {
                    if (hasMore) {
                        getString(R.string.playlist_batch_videos_loaded_more, loaded)
                    } else {
                        resources.getQuantityString(R.plurals.videos, loaded, loaded)
                    }
                } else {
                    ""
                }
            }

            ListExtractor.ITEM_COUNT_INFINITE ->
                getString(R.string.infinite_videos)

            ListExtractor.ITEM_COUNT_MORE_THAN_100 ->
                getString(R.string.more_than_100_videos)

            else -> Localization.localizeStreamCount(requireContext(), total)
        }
    }

    private fun errorText(
        kind: PlaylistBatchViewModel.ErrorKind,
        detail: String?
    ): String {
        val base = when (kind) {
            PlaylistBatchViewModel.ErrorKind.INVALID_URL ->
                getString(R.string.playlist_batch_invalid_url)

            PlaylistBatchViewModel.ErrorKind.NO_INTERNET ->
                getString(R.string.playlist_batch_no_internet)

            PlaylistBatchViewModel.ErrorKind.RATE_LIMITED ->
                getString(R.string.playlist_batch_rate_limited)

            PlaylistBatchViewModel.ErrorKind.NOT_AVAILABLE ->
                getString(R.string.playlist_batch_not_available)

            PlaylistBatchViewModel.ErrorKind.UNKNOWN ->
                getString(R.string.playlist_batch_error_unknown)

            PlaylistBatchViewModel.ErrorKind.NONE -> ""
        }
        return if (detail.isNullOrBlank()) {
            base
        } else {
            "$base\n$detail"
        }
    }
}
