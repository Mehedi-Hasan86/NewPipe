/*
 * SPDX-FileCopyrightText: 2026 NewPipe e.V. <https://newpipe-ev.de>
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

package org.schabi.newpipe.download.playlist

import android.os.Bundle
import android.view.LayoutInflater
import android.view.View
import android.view.ViewGroup
import android.widget.ArrayAdapter
import androidx.fragment.app.DialogFragment
import androidx.lifecycle.ViewModelProvider
import org.schabi.newpipe.R
import org.schabi.newpipe.databinding.SheetPlaylistItemOptionsBinding
import org.schabi.newpipe.extractor.stream.StreamInfo

/**
 * Per-item options for one playlist entry (STEP 2).
 * Fetches the item's [StreamInfo] lazily through the shared ViewModel
 * (cached, small limit) and stores a [PerItemOverride] for STEP 3.
 */
class PlaylistItemOptionsSheet : DialogFragment() {

    companion object {
        private const val ARG_SERVICE_ID = "item_options_service_id"
        private const val ARG_URL = "item_options_url"
        private const val ARG_TITLE = "item_options_title"

        fun newInstance(serviceId: Int, url: String, title: String): PlaylistItemOptionsSheet {
            return PlaylistItemOptionsSheet().apply {
                arguments = Bundle().apply {
                    putInt(ARG_SERVICE_ID, serviceId)
                    putString(ARG_URL, url)
                    putString(ARG_TITLE, title)
                }
            }
        }
    }

    private var _binding: SheetPlaylistItemOptionsBinding? = null
    private val binding get() = _binding!!

    private lateinit var viewModel: PlaylistBatchViewModel
    private var itemServiceId: Int = -1
    private var itemUrl: String = ""
    private var loadedInfo: StreamInfo? = null
    private var audioChoices: List<PlaylistBatchResolver.AudioTrackChoice> = emptyList()

    override fun onCreateView(
        inflater: LayoutInflater,
        container: ViewGroup?,
        savedInstanceState: Bundle?
    ): View {
        _binding = SheetPlaylistItemOptionsBinding.inflate(inflater, container, false)
        return binding.root
    }

    override fun onViewCreated(view: View, savedInstanceState: Bundle?) {
        super.onViewCreated(view, savedInstanceState)
        viewModel = ViewModelProvider(requireParentFragment())[PlaylistBatchViewModel::class.java]

        itemServiceId = arguments?.getInt(ARG_SERVICE_ID, -1) ?: -1
        itemUrl = arguments?.getString(ARG_URL).orEmpty()
        binding.itemTitleView.text = arguments?.getString(ARG_TITLE).orEmpty()

        binding.clearButton.setOnClickListener {
            viewModel.setOverride(itemUrl, null)
            dismiss()
        }
        binding.saveButton.setOnClickListener {
            saveOverride()
        }

        viewModel.streamStates.observe(viewLifecycleOwner) { states ->
            render(states[itemUrl])
        }
        viewModel.fetchStreamInfo(itemServiceId, itemUrl)
    }

    override fun onDestroyView() {
        _binding = null
        loadedInfo = null
        super.onDestroyView()
    }

    // ------------------------------------------------------------------

    private fun render(state: PlaylistBatchViewModel.StreamInfoState?) {
        if (_binding == null) {
            return
        }
        when (state) {
            null, is PlaylistBatchViewModel.StreamInfoState.Loading -> {
                binding.loadingProgress.visibility = View.VISIBLE
                binding.errorMessageView.visibility = View.GONE
                binding.optionsContainer.visibility = View.GONE
            }

            is PlaylistBatchViewModel.StreamInfoState.Error -> {
                binding.loadingProgress.visibility = View.GONE
                binding.optionsContainer.visibility = View.GONE
                binding.errorMessageView.visibility = View.VISIBLE
                binding.errorMessageView.text = getString(R.string.playlist_batch_item_load_error)
            }

            is PlaylistBatchViewModel.StreamInfoState.Loaded -> {
                loadedInfo = state.info
                binding.loadingProgress.visibility = View.GONE
                binding.errorMessageView.visibility = View.GONE
                binding.optionsContainer.visibility = View.VISIBLE
                fillOptions(state.info)
            }
        }
    }

    private fun fillOptions(info: StreamInfo) {
        val existing = viewModel.getOverride(itemUrl)

        val resolutions = info.videoStreams.orEmpty().mapNotNull { it.getResolution() }
            .plus(info.videoOnlyStreams.orEmpty().mapNotNull { it.getResolution() })
            .distinct()
        binding.qualitySpinner.adapter = spinnerAdapter(
            listOf(getString(R.string.playlist_batch_use_global)) + resolutions
        )
        val qualityIndex = resolutions.indexOf(existing?.targetResolution)
        binding.qualitySpinner.setSelection(if (qualityIndex >= 0) qualityIndex + 1 else 0, false)

        val formats = listOf(
            getString(R.string.playlist_batch_use_global),
            getString(R.string.playlist_batch_container_mp4),
            getString(R.string.playlist_batch_container_webm_video)
        )
        binding.videoFormatSpinner.adapter = spinnerAdapter(formats)
        binding.videoFormatSpinner.setSelection(
            when (existing?.videoContainer) {
                PlaylistVideoContainer.MP4 -> 1
                PlaylistVideoContainer.WEBM -> 2
                null -> 0
            },
            false
        )

        audioChoices = PlaylistBatchResolver.audioTrackChoices(requireContext(), info)
        if (audioChoices.size <= 1) {
            // Single default track: no language UI (spec).
            binding.audioLanguageLabel.visibility = View.GONE
            binding.audioLanguageSpinner.visibility = View.GONE
        } else {
            binding.audioLanguageLabel.visibility = View.VISIBLE
            binding.audioLanguageSpinner.visibility = View.VISIBLE
            val labels = listOf(getString(R.string.playlist_batch_use_global)) +
                audioChoices.map { it.displayName }
            binding.audioLanguageSpinner.adapter = spinnerAdapter(labels)
            val audioIndex = audioChoices.indexOfFirst { it.trackId == existing?.audioTrackId }
            binding.audioLanguageSpinner.setSelection(
                if (audioIndex >= 0) audioIndex + 1 else 0,
                false
            )
        }

        val subs = PlaylistBatchResolver.subtitleChoices(info)
        binding.subtitleInfoView.text = if (subs.isEmpty()) {
            getString(R.string.playlist_batch_no_subtitles)
        } else {
            getString(
                R.string.playlist_batch_available_subtitles,
                subs.joinToString(", ") { it.languageTag }
            )
        }
    }

    private fun saveOverride() {
        val info = loadedInfo
        if (info == null) {
            dismiss()
            return
        }
        val qualityPosition = binding.qualitySpinner.selectedItemPosition
        val resolutions = info.videoStreams.orEmpty().mapNotNull { it.getResolution() }
            .plus(info.videoOnlyStreams.orEmpty().mapNotNull { it.getResolution() })
            .distinct()
        val resolution = if (qualityPosition > 0) {
            resolutions.getOrNull(qualityPosition - 1)
        } else {
            null
        }
        val container = when (binding.videoFormatSpinner.selectedItemPosition) {
            1 -> PlaylistVideoContainer.MP4
            2 -> PlaylistVideoContainer.WEBM
            else -> null
        }
        var trackId: String? = null
        var trackLanguage: String? = null
        if (audioChoices.size > 1 && binding.audioLanguageSpinner.selectedItemPosition > 0) {
            val choice = audioChoices.getOrNull(
                binding.audioLanguageSpinner.selectedItemPosition - 1
            )
            trackId = choice?.trackId
            trackLanguage = choice?.languageCode
        }
        viewModel.setOverride(
            itemUrl,
            PerItemOverride(
                targetResolution = resolution,
                videoContainer = container,
                audioTrackId = trackId,
                audioTrackLanguage = trackLanguage
            )
        )
        dismiss()
    }

    private fun spinnerAdapter(labels: List<String>): ArrayAdapter<String> {
        return ArrayAdapter(
            requireContext(),
            android.R.layout.simple_spinner_item,
            labels
        ).apply {
            setDropDownViewResource(android.R.layout.simple_spinner_dropdown_item)
        }
    }
}
