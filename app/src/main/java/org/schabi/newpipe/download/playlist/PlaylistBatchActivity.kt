/*
 * SPDX-FileCopyrightText: 2026 NewPipe e.V. <https://newpipe-ev.de>
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

package org.schabi.newpipe.download.playlist

import android.content.Context
import android.content.Intent
import android.os.Bundle
import androidx.appcompat.app.AppCompatActivity
import org.schabi.newpipe.R
import org.schabi.newpipe.util.NO_SERVICE_ID
import org.schabi.newpipe.util.ThemeHelper

/**
 * STEP 1 host for [PlaylistBatchFragment].
 *
 * Reuses the existing share/view flow: plain launcher with extras, plus
 * ACTION_SEND fallback that extracts a playlist URL from shared text.
 * No new intent-filters: RouterActivity/MainActivity remain the entry point
 * for youtube.com playlist links; this activity is opened from the app
 * (playlist screen menu) or via [newIntent].
 */
class PlaylistBatchActivity : AppCompatActivity() {

    companion object {
        private const val EXTRA_SERVICE_ID = "playlist_batch_service_id"
        private const val EXTRA_URL = "playlist_batch_url"
        private const val EXTRA_NAME = "playlist_batch_name"

        @JvmStatic
        fun newIntent(context: Context, serviceId: Int, url: String, name: String?): Intent {
            return Intent(context, PlaylistBatchActivity::class.java).apply {
                putExtra(EXTRA_SERVICE_ID, serviceId)
                putExtra(EXTRA_URL, url)
                putExtra(EXTRA_NAME, name)
            }
        }
    }

    override fun onCreate(savedInstanceState: Bundle?) {
        ThemeHelper.setTheme(this)
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_playlist_batch)

        supportActionBar?.apply {
            setDisplayHomeAsUpEnabled(true)
            setTitle(R.string.playlist_batch_title)
        }

        if (savedInstanceState != null) {
            return
        }

        val (serviceId, url, name) = extractArgs(intent)
        supportFragmentManager.beginTransaction()
            .replace(
                R.id.playlistBatchContainer,
                PlaylistBatchFragment.newInstance(serviceId, url, name)
            )
            .commit()
    }

    override fun onSupportNavigateUp(): Boolean {
        onBackPressedDispatcher.onBackPressed()
        return true
    }

    private fun extractArgs(intent: Intent): Triple<Int, String, String?> {
        var serviceId = intent.getIntExtra(EXTRA_SERVICE_ID, NO_SERVICE_ID)
        var url = intent.getStringExtra(EXTRA_URL).orEmpty()
        var name: String? = intent.getStringExtra(EXTRA_NAME)

        if (url.isBlank()) {
            // Share-intent fallback: reuse whatever text was shared.
            val shared = intent.getStringExtra(Intent.EXTRA_TEXT)
                ?: intent.getCharSequenceExtra(Intent.EXTRA_TEXT)?.toString()
            val dataUrl = intent.data?.toString()
            val resolved = PlaylistBatchUrlHelper.parsePastedText(shared)
                ?: PlaylistBatchUrlHelper.parsePastedText(dataUrl)
            if (resolved != null) {
                serviceId = resolved.serviceId
                url = resolved.playlistUrl
            } else if (!shared.isNullOrBlank()) {
                url = shared
            } else if (!dataUrl.isNullOrBlank()) {
                url = dataUrl
            }
        }
        return Triple(serviceId, url, name)
    }
}
