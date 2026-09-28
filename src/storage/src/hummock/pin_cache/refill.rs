// Copyright 2026 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::atomic::Ordering;
use std::sync::{Arc, LazyLock};

use risingwave_common::log::LogSuppressor;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_object_store::object::{
    MonitoredStreamingReader, ObjectError, ObjectResult, ObjectStoreRef,
};

use super::{
    PinCache, PinCacheEntry, PinCacheFile, PinCacheRefillOutcome, PinCacheRefillToken,
    PinCacheState,
};
use crate::monitor::GLOBAL_PIN_CACHE_METRICS;

pub(super) enum PinCacheDownloadStart {
    Download(PinCacheDownloadGuard),
    Complete(PinCacheRefillOutcome),
}

/// Owns an in-flight download until publication transfers its entry to the read index.
/// It must also handle cancellation before or during an upload, when no result is returned.
pub(super) struct PinCacheDownloadGuard {
    pin_cache: Arc<PinCache>,
    token: PinCacheRefillToken,
    // Publication moves the entry into the read index. Drop releases an unpublished attempt.
    pub(super) entry: Option<Arc<PinCacheEntry>>,
    // An unfinished uploader may leave backend-owned temporary files with unknown paths.
    upload_in_progress: bool,
}

impl PinCacheDownloadGuard {
    fn record_io_failure(&self, phase: &'static str) {
        GLOBAL_PIN_CACHE_METRICS
            .io_failures
            .with_label_values(&[phase])
            .inc();
    }

    /// Copies an existing SST stream into a unique local path using the object-store uploader.
    /// After finishing the upload, checks both the copied byte count and local file size before
    /// attempting publication. Publication still requires the current download token and membership.
    /// On error or cancellation, Drop releases this download and reclaims its file. An unfinished
    /// uploader keeps its capacity reservation until recovery can inventory temporary files.
    pub(super) async fn write(
        mut self,
        mut reader: MonitoredStreamingReader,
    ) -> ObjectResult<PinCacheRefillOutcome> {
        let entry = self
            .entry
            .as_ref()
            .expect("download owns an unpublished file");
        // Cancellation can occur while opening the uploader, before it returns a writer.
        self.upload_in_progress = true;
        let mut writer = self
            .pin_cache
            .store
            .streaming_upload(&entry.path)
            .await
            .inspect_err(|_| self.record_io_failure("local_upload_init"))?;
        let mut written = 0_u64;
        while let Some(chunk) = reader.read_bytes().await {
            let chunk = chunk.inspect_err(|_| self.record_io_failure("remote_read"))?;
            written = written.saturating_add(chunk.len() as u64);
            if written > entry.size {
                self.record_io_failure("size_validation");
                return Err(ObjectError::internal(
                    "pinned SST is larger than its version metadata",
                ));
            }
            writer
                .write_bytes(chunk)
                .await
                .inspect_err(|_| self.record_io_failure("local_upload_write"))?;
        }
        writer
            .finish()
            .await
            .inspect_err(|_| self.record_io_failure("local_upload_finish"))?;
        self.upload_in_progress = false;
        let local_size = self
            .pin_cache
            .store
            .metadata(&entry.path)
            .await
            .inspect_err(|_| self.record_io_failure("local_metadata"))?
            .total_size as u64;
        if written != entry.size || local_size != entry.size {
            self.record_io_failure("size_validation");
            return Err(ObjectError::internal(
                "pinned SST size does not match its version metadata",
            ));
        }
        Ok(self.publish())
    }

    /// Downloading -> Published only while this admission is current. Removing the object or
    /// revoking its generation rejects publication. Success transfers the file to the index;
    /// rejection leaves it with the guard, whose Drop runs after the shard lock is released.
    pub(super) fn publish(mut self) -> PinCacheRefillOutcome {
        let mut state = self.pin_cache.shard(self.token.object_id).write();
        let Some(object) = state.refill_object(self.token) else {
            return PinCacheRefillOutcome::Obsolete;
        };
        debug_assert!(matches!(object.file, PinCacheFile::Downloading { .. }));
        object.publish(
            self.entry
                .take()
                .expect("download owns an unpublished file"),
        );
        PinCacheRefillOutcome::Published
    }
}

impl Drop for PinCacheDownloadGuard {
    fn drop(&mut self) {
        let Some(entry) = self.entry.take() else {
            return; // Publication transferred ownership to the index.
        };
        if let Some(object) = self
            .pin_cache
            .shard(self.token.object_id)
            .write()
            .refill_object(self.token)
        {
            object.cancel_download();
        }

        if self.upload_in_progress {
            // Keep the full reservation until startup recovery inventories the actual files.
            // Do not let final-path deletion release capacity while hidden temporary bytes remain.
            self.pin_cache.gc.mark_uncertain(&entry);
            tracing::warn!(
                object_id = self.token.object_id.as_raw_id(),
                path = %entry.path,
                reserved_bytes = entry.size,
                "unfinished pin cache upload; retaining capacity until recovery"
            );
            return;
        }
        self.pin_cache.gc.reclaim([entry]);
    }
}

impl PinCache {
    /// Captures admission for a needed object before the caller queues a refill.
    /// Execution must use this token with the same cache; it must not recapture admission after
    /// waiting in a queue or when retrying the same work. Revocation invalidates all previously
    /// issued tokens for the object.
    pub(crate) fn prepare_refill(
        &self,
        object_id: HummockSstableObjectId,
    ) -> Option<PinCacheRefillToken> {
        let state = self.shard(object_id).read();
        let object = state.objects.get(&object_id)?;
        Some(PinCacheRefillToken {
            object_id,
            generation: object.generation,
        })
    }

    /// Revokes queued and active refills without withdrawing a published route.
    /// Running I/O is not aborted, but its guard can no longer publish. A subsequent
    /// `prepare_refill` issues fresh admission for the object if it is still needed.
    pub(crate) fn revoke_refill(&self, object_id: HummockSstableObjectId) {
        let mut state = self.shard(object_id).write();
        let PinCacheState {
            objects,
            next_generation,
            ..
        } = &mut *state;
        if let Some(object) = objects.get_mut(&object_id) {
            *next_generation += 1;
            object.generation = *next_generation;
            object.cancel_download();
        }
    }

    /// Executes a queued refill using admission previously captured by this cache.
    /// Rechecks admission before starting a download.
    /// A stale token skips the download. Failures or cancellation leave cleanup to the download guard.
    pub(crate) async fn refill(
        self: &Arc<Self>,
        token: PinCacheRefillToken,
        remote_store: ObjectStoreRef,
        remote_path: String,
    ) -> ObjectResult<PinCacheRefillOutcome> {
        let download = match self.start_download(token) {
            PinCacheDownloadStart::Download(download) => download,
            PinCacheDownloadStart::Complete(outcome) => return Ok(outcome),
        };
        let reader = remote_store
            .streaming_read(&remote_path, ..)
            .await
            .inspect_err(|_| download.record_io_failure("remote_read_init"))?;
        download.write(reader).await
    }

    pub(super) fn start_download(
        self: &Arc<Self>,
        token: PinCacheRefillToken,
    ) -> PinCacheDownloadStart {
        let object_id = token.object_id;
        let mut state = self.shard(object_id).write();
        let Some(object) = state.refill_object(token) else {
            return PinCacheDownloadStart::Complete(PinCacheRefillOutcome::Obsolete);
        };
        let size = match object.file {
            PinCacheFile::Missing { size } => size,
            PinCacheFile::Downloading { .. } => {
                return PinCacheDownloadStart::Complete(PinCacheRefillOutcome::InProgress);
            }
            PinCacheFile::Published(_) => {
                return PinCacheDownloadStart::Complete(PinCacheRefillOutcome::AlreadyPublished);
            }
        };
        let entry = PinCacheEntry {
            path: self.new_object_path(object_id),
            size,
        };
        if let Err(accounted_bytes) = self.gc.try_reserve(&entry) {
            drop(state);
            static LOG_SUPPRESSOR: LazyLock<LogSuppressor> =
                LazyLock::new(|| LogSuppressor::per_minute(1));
            if let Ok(suppressed_count) = LOG_SUPPRESSOR.check() {
                tracing::warn!(
                    suppressed_count,
                    object_id = object_id.as_raw_id(),
                    object_size = size,
                    accounted_bytes,
                    capacity = self.gc.capacity(),
                    "skipping pin cache refill because local capacity is exhausted"
                );
            }
            return PinCacheDownloadStart::Complete(PinCacheRefillOutcome::CapacityRejected);
        }
        let entry = Arc::new(entry);
        object.file = PinCacheFile::Downloading { size };
        PinCacheDownloadStart::Download(PinCacheDownloadGuard {
            pin_cache: Arc::clone(self),
            token,
            entry: Some(entry),
            upload_in_progress: false,
        })
    }

    fn new_object_path(&self, object_id: HummockSstableObjectId) -> String {
        let path_id = self.next_path_id.fetch_add(1, Ordering::Relaxed);
        format!("{}-{path_id}.sst", object_id.as_raw_id())
    }
}
