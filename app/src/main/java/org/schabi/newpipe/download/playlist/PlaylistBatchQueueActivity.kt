/*
 * SPDX-FileCopyrightText: 2026 NewPipe e.V. <https://newpipe-ev.de>
 * SPDX-License-Identifier: GPL-3.0-or-later
 */

package org.schabi.newpipe.download.playlist

import android.os.Bundle
import androidx.appcompat.app.AppCompatActivity
import org.schabi.newpipe.R
import org.schabi.newpipe.util.ThemeHelper

/**
 * Host for [PlaylistBatchQueueFragment]. Opened via [NavigationHelper.openPlaylistBatchQueue].
 */
class PlaylistBatchQueueActivity : AppCompatActivity() {

    override fun onCreate(savedInstanceState: Bundle?) {
        ThemeHelper.setTheme(this)
        super.onCreate(savedInstanceState)
        setContentView(R.layout.activity_playlist_batch_queue)

        supportActionBar?.apply {
            setDisplayHomeAsUpEnabled(true)
            setTitle(R.string.playlist_batch_queue_title)
        }

        if (savedInstanceState == null) {
            supportFragmentManager.beginTransaction()
                .replace(R.id.playlistBatchQueueContainer, PlaylistBatchQueueFragment.newInstance())
                .commit()
        }
    }

    override fun onSupportNavigateUp(): Boolean {
        onBackPressed()
        return true
    }
}
