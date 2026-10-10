/*
 * SPDX-FileCopyrightText: 2026 NewPipe e.V. <https://newpipe-ev.de>
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

package org.schabi.newpipe.download.playlist

import org.schabi.newpipe.extractor.Image
import org.schabi.newpipe.extractor.stream.StreamType

/**
 * STEP 1 only: intake + selection model for playlist batch download.
 * No downloading happens here; downloading will be added in a later step.
 *
 * @param position 1-based serial number shown in the UI, stable for the loaded order.
 */
data class PlaylistBatchItem(
    val serviceId: Int,
    val url: String,
    val title: String,
    val durationSecs: Long,
    val uploaderName: String?,
    val thumbnails: List<Image>,
    val streamType: StreamType?,
    val position: Int,
    var selected: Boolean = true,
    /** Textual upload date, used for the {date} filename token. Null when unknown. */
    val uploadDateText: String? = null
)
