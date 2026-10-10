/*
 * SPDX-FileCopyrightText: 2026 NewPipe e.V. <https://newpipe-ev.de>
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

package org.schabi.newpipe.download.playlist

import android.net.Uri
import org.schabi.newpipe.extractor.NewPipe
import org.schabi.newpipe.extractor.StreamingService
import org.schabi.newpipe.util.NO_SERVICE_ID

/**
 * Resolves user-supplied URLs to a playlist source.
 *
 * Covers:
 * - `playlist?list=` URLs (LinkType.PLAYLIST, used as-is)
 * - `watch?v=...&list=...` URLs (LinkType.STREAM on YouTube, but carrying a
 *   `list` query parameter -> rewritten to a canonical `playlist?list=` URL)
 * - pasted URLs / share-intent text (service detected via [NewPipe.getServiceByUrl])
 *
 * Existing RouterActivity/MainActivity intent handling is reused wherever possible;
 * this helper only normalizes the URL so STEP 1 can load it with PlaylistInfo.
 */
object PlaylistBatchUrlHelper {

    data class ResolvedPlaylist(
        val serviceId: Int,
        val playlistUrl: String,
        val linkType: StreamingService.LinkType
    )

    /**
     * Resolve [serviceId]/[url] into a playlist URL.
     * @throws Exception if no service knows the URL.
     */
    @Throws(Exception::class)
    fun resolve(serviceId: Int, url: String): ResolvedPlaylist {
        val trimmed = url.trim()
        require(trimmed.isNotEmpty()) { "Empty playlist URL" }

        val service = if (serviceId != NO_SERVICE_ID) {
            NewPipe.getService(serviceId)
        } else {
            NewPipe.getServiceByUrl(trimmed)
        }

        val linkType = try {
            service.getLinkTypeByUrl(trimmed)
        } catch (e: Exception) {
            StreamingService.LinkType.NONE
        }

        if (linkType == StreamingService.LinkType.PLAYLIST) {
            return ResolvedPlaylist(service.serviceId, trimmed, linkType)
        }

        // watch?v=...&list=... case: rewrite to a canonical playlist URL (YouTube + friends).
        val listId = extractListParam(trimmed)
        if (listId != null) {
            val base = extractOrigin(trimmed) ?: "https://www.youtube.com"
            return ResolvedPlaylist(
                service.serviceId,
                "$base/playlist?list=$listId",
                StreamingService.LinkType.PLAYLIST
            )
        }

        return ResolvedPlaylist(service.serviceId, trimmed, linkType)
    }

    /**
     * Try to detect service + playlist URL from arbitrary pasted/shared text.
     * Returns null when no URL in [text] is known to any service.
     */
    fun parsePastedText(text: String?): ResolvedPlaylist? {
        if (text.isNullOrBlank()) {
            return null
        }
        // Share intents often carry "<title> <url>"; pick out http(s) tokens first.
        val candidates = extractUrls(text)
        for (candidate in candidates) {
            try {
                val resolved = resolve(NO_SERVICE_ID, candidate)
                if (resolved.linkType == StreamingService.LinkType.PLAYLIST) {
                    return resolved
                }
            } catch (e: Exception) {
                // try next candidate
            }
        }
        // Fall back to the raw text itself (it may be a bare playlist URL).
        return try {
            val resolved = resolve(NO_SERVICE_ID, text.trim())
            if (resolved.linkType == StreamingService.LinkType.PLAYLIST) {
                resolved
            } else {
                null
            }
        } catch (e: Exception) {
            null
        }
    }

    private fun extractListParam(url: String): String? {
        return try {
            val list = Uri.parse(url).getQueryParameter("list")
            if (list.isNullOrBlank()) null else list
        } catch (e: Exception) {
            null
        }
    }

    /**
     * Stable identity of a playlist for batch deduplication and folder reuse:
     * `"<serviceId>:<playlistId>"`, or blank when no playlist id can be found.
     *
     * @param serviceId the streaming service id
     * @param url the playlist URL as stored on the batch
     * @return the playlist key, or blank
     */
    fun playlistKey(serviceId: Int, url: String): String {
        val listId = extractListParam(url) ?: return ""
        return "$serviceId:$listId"
    }

    private fun extractOrigin(url: String): String? {
        return try {
            val uri = Uri.parse(url)
            val scheme = uri.scheme ?: return null
            val host = uri.host ?: return null
            "$scheme://$host"
        } catch (e: Exception) {
            null
        }
    }

    private fun extractUrls(text: String): List<String> {
        val regex = Regex("https?://[^\\s\"'<>]+", RegexOption.IGNORE_CASE)
        return regex.findAll(text).map { it.value.trimEnd('.', ',', ';', ')', ']') }.toList()
    }
}
