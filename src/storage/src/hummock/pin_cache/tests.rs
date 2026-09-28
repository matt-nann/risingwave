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

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use risingwave_common::config::{ObjectStoreConfig, RwConfig, extract_storage_memory_config};
use risingwave_common::system_param::system_params_for_test;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_object_store::object::{
    InMemObjectStore, MonitoredStreamingReader, ObjectError, ObjectResult, ObjectStore,
    ObjectStoreImpl, ObjectStoreRef, build_remote_object_store,
};

use super::refill::{PinCacheDownloadGuard, PinCacheDownloadStart};
use super::{PinCache, PinCacheRefillOutcome};
use crate::monitor::ObjectStoreMetrics;
use crate::opts::StorageOpts;

impl PinCache {
    pub(crate) async fn pin_sst(
        self: &Arc<Self>,
        remote_store: ObjectStoreRef,
        remote_path: String,
        object_id: HummockSstableObjectId,
    ) -> ObjectResult<PinCacheRefillOutcome> {
        let token = self
            .prepare_refill(object_id)
            .expect("test object must be needed");
        self.refill(token, remote_store, remote_path).await
    }
}

pub(super) fn in_memory_object_store() -> ObjectStoreRef {
    Arc::new(ObjectStoreImpl::InMem(
        InMemObjectStore::for_test().monitored(
            Arc::new(ObjectStoreMetrics::unused()),
            Arc::new(ObjectStoreConfig::default()),
        ),
    ))
}

pub(super) async fn local_object_store() -> (tempfile::TempDir, ObjectStoreRef) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = ObjectStoreConfig {
        upload_part_size: 1,
        ..Default::default()
    };
    config.set_atomic_write_dir();
    let store = Arc::new(
        build_remote_object_store(
            &format!("fs://{}", dir.path().display()),
            Arc::new(ObjectStoreMetrics::unused()),
            "test pin cache",
            Arc::new(config),
        )
        .await,
    );
    (dir, store)
}

pub(super) async fn wait_for_reclaim(pin_cache: &PinCache) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while pin_cache.gc.accounted_bytes() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

fn start_download(
    pin_cache: &Arc<PinCache>,
    object_id: HummockSstableObjectId,
) -> PinCacheDownloadGuard {
    let token = pin_cache.prepare_refill(object_id).unwrap();
    match pin_cache.start_download(token) {
        PinCacheDownloadStart::Download(download) => download,
        PinCacheDownloadStart::Complete(outcome) => {
            panic!("expected download, got {outcome:?}")
        }
    }
}

#[tokio::test]
#[should_panic(expected = "pin cache shard count must be greater than zero")]
async fn test_zero_shards_rejected() {
    PinCache::new(in_memory_object_store(), 16, 0, [])
        .await
        .unwrap();
}

#[test]
fn test_parse_finalized_object_path() {
    assert_eq!(
        PinCache::parse_object_id("1001-42.sst"),
        Some(HummockSstableObjectId::from(1001))
    );
    assert_eq!(PinCache::parse_object_id("1001-recovered.sst"), None);
    assert_eq!(PinCache::parse_object_id("1001-42.tmp"), None);
    assert_eq!(PinCache::parse_object_id("nested/1001-42.sst"), None);
}

#[tokio::test]
async fn test_pin_read_and_unpin_lifecycle() {
    let remote_store = in_memory_object_store();
    let (_dir, local_store) = local_object_store().await;
    let pin_cache = PinCache::new(local_store, u64::MAX, 1, []).await.unwrap();
    let object_id = HummockSstableObjectId::from(1001);
    let remote_path = "remote.sst";
    let original = Bytes::from_static(b"complete sst");
    remote_store
        .upload(remote_path, original.clone())
        .await
        .unwrap();

    pin_cache.update_policy(HashMap::from([(object_id, original.len() as u64)]));
    pin_cache
        .pin_sst(remote_store.clone(), remote_path.to_owned(), object_id)
        .await
        .unwrap();
    assert!(pin_cache.get(object_id).is_some());
    assert_eq!(
        pin_cache.get(object_id).unwrap().read(..).await.unwrap(),
        original
    );
    remote_store
        .upload(remote_path, Bytes::from_static(b"changed remote"))
        .await
        .unwrap();
    pin_cache
        .pin_sst(remote_store.clone(), remote_path.to_owned(), object_id)
        .await
        .unwrap();
    assert_eq!(
        pin_cache.get(object_id).unwrap().read(..).await.unwrap(),
        original
    );

    pin_cache.update_policy(HashMap::new());
    assert!(pin_cache.get(object_id).is_none());

    let changed = Bytes::from_static(b"changed remote");
    pin_cache.update_policy(HashMap::from([(object_id, changed.len() as u64)]));
    pin_cache
        .pin_sst(remote_store.clone(), remote_path.to_owned(), object_id)
        .await
        .unwrap();
    assert_eq!(
        pin_cache.get(object_id).unwrap().read(..).await.unwrap(),
        changed
    );

    pin_cache.update_policy(HashMap::new());
    assert!(pin_cache.prepare_refill(object_id).is_none());
    assert!(pin_cache.get(object_id).is_none());
}

#[tokio::test]
async fn test_revoked_token_cannot_begin_a_late_download() {
    let remote = in_memory_object_store();
    remote
        .upload("sst", Bytes::from_static(b"complete"))
        .await
        .unwrap();
    let object = HummockSstableObjectId::from(911);
    for revoke_by_unpin in [false, true] {
        let cache = PinCache::new(in_memory_object_store(), u64::MAX, 1, [])
            .await
            .unwrap();
        cache.update_policy([(object, 8)]);
        let token = cache.prepare_refill(object).unwrap();
        assert_eq!(token.object_id(), object);
        assert_eq!(cache.gc.accounted_bytes(), 0);
        if revoke_by_unpin {
            cache.update_policy([]);
            cache.update_policy([(object, 8)]);
        } else {
            cache.revoke_refill(object);
        }
        let replacement = cache.prepare_refill(object).unwrap();
        assert_eq!(
            cache
                .refill(token, remote.clone(), "sst".into())
                .await
                .unwrap(),
            PinCacheRefillOutcome::Obsolete
        );
        assert_eq!(
            cache
                .refill(replacement, remote.clone(), "sst".into())
                .await
                .unwrap(),
            PinCacheRefillOutcome::Published
        );
    }
}

#[tokio::test]
async fn test_revoke_retired_withdraws_previous_version_route() {
    let remote_store = in_memory_object_store();
    let cache = PinCache::new(in_memory_object_store(), u64::MAX, 1, [])
        .await
        .unwrap();
    let object = HummockSstableObjectId::from(911);
    remote_store
        .upload("sst", Bytes::from_static(b"complete"))
        .await
        .unwrap();
    cache.update_policy([(object, 8)]);
    cache
        .pin_sst(remote_store, "sst".into(), object)
        .await
        .unwrap();
    cache.update_version_delta(2.into(), [object], HashMap::new());
    assert!(cache.get(object).is_some());

    cache.revoke_retired();
    assert!(cache.get(object).is_none());
}

#[tokio::test]
async fn test_same_delta_replacement_keeps_route() {
    let remote_store = in_memory_object_store();
    let pin_cache = PinCache::new(in_memory_object_store(), u64::MAX, 1, [])
        .await
        .unwrap();
    let object_id = HummockSstableObjectId::from(1001);
    remote_store
        .upload("sst", Bytes::from_static(b"12345678"))
        .await
        .unwrap();
    pin_cache.update_policy([(object_id, 8)]);
    pin_cache
        .pin_sst(remote_store, "sst".into(), object_id)
        .await
        .unwrap();

    pin_cache.update_version_delta(2.into(), [object_id], HashMap::from([(object_id, 8)]));
    assert!(pin_cache.get(object_id).is_some());

    pin_cache.update_version_delta(3.into(), [object_id], HashMap::new());
    assert!(pin_cache.get(object_id).is_some());
    pin_cache.update_policy([]);
    assert!(pin_cache.get(object_id).is_none());
}

#[tokio::test]
async fn test_inflight_is_not_routable_and_cancellation_releases_token() {
    let pin_cache = PinCache::new(in_memory_object_store(), u64::MAX, 1, [])
        .await
        .unwrap();
    let object_id = HummockSstableObjectId::from(1001);
    pin_cache.update_policy(HashMap::from([(object_id, 8)]));

    let download = start_download(&pin_cache, object_id);
    assert!(pin_cache.get(object_id).is_none());
    assert_eq!(
        pin_cache
            .pin_sst(in_memory_object_store(), "unused".into(), object_id)
            .await
            .unwrap(),
        PinCacheRefillOutcome::InProgress
    );
    drop(download);

    let retry = start_download(&pin_cache, object_id);
    pin_cache
        .store
        .upload(
            &retry.entry.as_ref().unwrap().path,
            Bytes::from_static(b"complete"),
        )
        .await
        .unwrap();
    assert_eq!(retry.publish(), PinCacheRefillOutcome::Published);
    assert!(pin_cache.get(object_id).is_some());
    assert_eq!(
        pin_cache
            .pin_sst(in_memory_object_store(), "unused".into(), object_id)
            .await
            .unwrap(),
        PinCacheRefillOutcome::AlreadyPublished
    );
}

#[tokio::test]
async fn test_revoked_download_cannot_publish_or_remove_replacement() {
    for revoke_by_unpin in [false, true] {
        let pin_cache = PinCache::new(in_memory_object_store(), u64::MAX, 1, [])
            .await
            .unwrap();
        let object_id = HummockSstableObjectId::from(1001);
        let desired = [(object_id, 11)];
        pin_cache.update_policy(desired);
        let old = start_download(&pin_cache, object_id);

        if revoke_by_unpin {
            pin_cache.update_policy(HashMap::new());
        } else {
            pin_cache.update_policy(HashMap::from([(object_id, 12)]));
        }
        pin_cache.update_policy(desired);
        let replacement = start_download(&pin_cache, object_id);
        assert_ne!(
            old.entry.as_ref().unwrap().path,
            replacement.entry.as_ref().unwrap().path
        );

        assert_eq!(old.publish(), PinCacheRefillOutcome::Obsolete);
        assert!(pin_cache.get(object_id).is_none());
        assert_eq!(
            pin_cache
                .pin_sst(in_memory_object_store(), "unused".into(), object_id)
                .await
                .unwrap(),
            PinCacheRefillOutcome::InProgress,
        );
        pin_cache
            .store
            .upload(
                &replacement.entry.as_ref().unwrap().path,
                Bytes::from_static(b"replacement"),
            )
            .await
            .unwrap();
        assert_eq!(replacement.publish(), PinCacheRefillOutcome::Published);
        assert_eq!(
            pin_cache.get(object_id).unwrap().read(..).await.unwrap(),
            Bytes::from_static(b"replacement")
        );
    }
}

#[tokio::test]
async fn test_failed_download_can_be_retried() {
    let remote_store = in_memory_object_store();
    let pin_cache = PinCache::new(in_memory_object_store(), 8, 1, [])
        .await
        .unwrap();
    let object_id = HummockSstableObjectId::from(1001);
    pin_cache.update_policy(HashMap::from([(object_id, 8)]));
    let token = pin_cache.prepare_refill(object_id).unwrap();
    assert!(
        pin_cache
            .refill(token, remote_store.clone(), "sst".into())
            .await
            .is_err()
    );
    assert!(pin_cache.get(object_id).is_none());
    wait_for_reclaim(&pin_cache).await;

    remote_store
        .upload("sst", Bytes::from_static(b"complete"))
        .await
        .unwrap();
    assert_eq!(
        pin_cache
            .refill(token, remote_store, "sst".into())
            .await
            .unwrap(),
        PinCacheRefillOutcome::Published
    );
    assert!(pin_cache.get(object_id).is_some());
}

#[tokio::test]
async fn test_interrupted_fs_upload_keeps_capacity_until_recovery() {
    for cancel in [false, true] {
        let (_dir, local_store) = local_object_store().await;
        let pin_cache = PinCache::new(local_store.clone(), 8, 1, []).await.unwrap();
        let object_id = HummockSstableObjectId::from(1001);
        pin_cache.update_policy([(object_id, 8)]);

        let download = start_download(&pin_cache, object_id);
        let final_path = download.entry.as_ref().unwrap().path.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        let (fail_tx, fail_rx) = tokio::sync::oneshot::channel();
        let reader = MonitoredStreamingReader::new(
            "test",
            Box::pin(
                stream::iter([
                    Ok(Bytes::from_static(b"half")),
                    Ok(Bytes::from_static(b"x")),
                ])
                .chain(stream::once(async move {
                    // Two writes flush the FS position writer's one-chunk buffer.
                    started_tx.send(()).unwrap();
                    let _ = fail_rx.await;
                    Err(ObjectError::internal("injected remote read failure"))
                })),
            ),
            Arc::new(ObjectStoreMetrics::unused()),
            None,
        );
        let task = tokio::spawn(download.write(reader));
        tokio::time::timeout(Duration::from_secs(5), started_rx)
            .await
            .unwrap()
            .unwrap();
        // Wait for Tokio's buffered file write to reach the filesystem before cancellation.
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let files: Vec<_> = local_store
                    .list("", None, None)
                    .await
                    .unwrap()
                    .try_collect()
                    .await
                    .unwrap();
                if files.iter().any(|file| file.total_size == 4) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        if cancel {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            fail_tx.send(()).unwrap();
            assert!(task.await.unwrap().is_err());
        }

        assert!(pin_cache.get(object_id).is_none());
        assert!(
            local_store
                .metadata(&final_path)
                .await
                .unwrap_err()
                .is_object_not_found_error()
        );
        assert_eq!(pin_cache.gc.accounted_bytes(), 8);
        // The download slot is released, but unfinished temporary bytes still consume capacity.
        assert_eq!(
            pin_cache
                .pin_sst(in_memory_object_store(), "unused".into(), object_id)
                .await
                .unwrap(),
            PinCacheRefillOutcome::CapacityRejected
        );
        // Unpin must not release the reservation for the backend-owned temporary file either.
        pin_cache.update_policy([]);
        assert_eq!(pin_cache.gc.accounted_bytes(), 8);
        drop(pin_cache);

        let recovered = PinCache::new(local_store.clone(), 8, 1, [(object_id, 8)])
            .await
            .unwrap();

        wait_for_reclaim(&recovered).await;
        assert!(recovered.get(object_id).is_none());
        let files: Vec<_> = local_store
            .list("", None, None)
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        assert!(files.iter().all(|file| file.key.ends_with('/')));
        let remote_store = in_memory_object_store();
        remote_store
            .upload("sst", Bytes::from_static(b"complete"))
            .await
            .unwrap();
        recovered
            .pin_sst(remote_store, "sst".into(), object_id)
            .await
            .unwrap();
        assert!(recovered.get(object_id).is_some());
    }
}

#[tokio::test]
async fn test_completed_invalid_fs_upload_reclaims_capacity() {
    let (_dir, local_store) = local_object_store().await;
    let pin_cache = PinCache::new(local_store.clone(), 8, 1, []).await.unwrap();
    let remote_store = in_memory_object_store();
    remote_store
        .upload("sst", Bytes::from_static(b"half"))
        .await
        .unwrap();
    let object_id = HummockSstableObjectId::from(1001);
    pin_cache.update_policy([(object_id, 8)]);
    assert!(
        pin_cache
            .pin_sst(remote_store, "sst".into(), object_id)
            .await
            .is_err()
    );
    assert!(pin_cache.get(object_id).is_none());
    wait_for_reclaim(&pin_cache).await;
    let files: Vec<_> = local_store
        .list("", None, None)
        .await
        .unwrap()
        .try_collect()
        .await
        .unwrap();
    assert!(files.iter().all(|file| file.key.ends_with('/')));
}

#[tokio::test]
async fn test_recovery_rejects_incomplete_inventory() {
    for partial_inventory in [false, true] {
        let local_store = in_memory_object_store();
        let mut cache = PinCache::new(local_store.clone(), 16, 1, []).await.unwrap();
        local_store
            .upload("1001-42.sst", Bytes::from_static(b"complete"))
            .await
            .unwrap();
        let error = ObjectError::internal("injected inventory failure");
        let objects = if partial_inventory {
            let metadata = local_store.metadata("1001-42.sst").await.unwrap();
            Ok(stream::iter([Ok(metadata), Err(error)]).boxed())
        } else {
            Err(error)
        };
        // Exercise both list and mid-stream failures before sharing the cache.
        let result = Arc::get_mut(&mut cache)
            .unwrap()
            .recover_local_files(objects)
            .await;
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("injected inventory failure")
        );
        assert!(cache.get(1001.into()).is_none());
        assert!(local_store.metadata("1001-42.sst").await.is_ok());
    }
}

#[tokio::test]
async fn test_read_failure_only_invalidates_selected_publication() {
    let remote_store = in_memory_object_store();
    let pin_cache = PinCache::new(in_memory_object_store(), u64::MAX, 1, [])
        .await
        .unwrap();
    let object_id = HummockSstableObjectId::from(1001);
    pin_cache.update_policy(HashMap::from([(object_id, 8)]));
    remote_store
        .upload("sst", Bytes::from_static(b"complete"))
        .await
        .unwrap();
    pin_cache
        .pin_sst(remote_store.clone(), "sst".into(), object_id)
        .await
        .unwrap();
    let old = pin_cache.get(object_id).unwrap();
    pin_cache.store.delete(&old.entry.path).await.unwrap();
    assert!(old.read(..).await.is_err());
    assert!(pin_cache.get(object_id).is_none());

    pin_cache
        .pin_sst(remote_store, "sst".into(), object_id)
        .await
        .unwrap();
    // This handle stays on the old path and must not remove the new route.
    assert!(old.read(..).await.is_err());
    assert_eq!(
        pin_cache.get(object_id).unwrap().read(..).await.unwrap(),
        Bytes::from_static(b"complete")
    );
}

#[tokio::test]
async fn test_recovery_reclaims_files_outside_initial_membership() {
    let local_store = in_memory_object_store();
    for path in ["1001-42.sst", "unfinished.tmp"] {
        local_store
            .upload(path, Bytes::from_static(b"stale"))
            .await
            .unwrap();
    }
    let pin_cache = PinCache::new(local_store.clone(), 1024, 1, [])
        .await
        .unwrap();
    wait_for_reclaim(&pin_cache).await;
    for path in ["1001-42.sst", "unfinished.tmp"] {
        assert!(
            local_store
                .metadata(path)
                .await
                .unwrap_err()
                .is_object_not_found_error()
        );
    }
}

fn object_in_shard(shard: usize, shard_num: usize) -> HummockSstableObjectId {
    (1..)
        .map(HummockSstableObjectId::from)
        .find(|&id| PinCache::shard_index(id, shard_num) == shard)
        .unwrap()
}

#[tokio::test]
async fn test_other_shard_and_control_locks_do_not_block_lookup_or_publish() {
    let mut config = RwConfig::default();
    config.storage.cache.pin_cache_shard_num = 3;
    let system_params = system_params_for_test().into();
    let memory = extract_storage_memory_config(&config);
    let opts = StorageOpts::from((&config, &system_params, &memory));
    let cache = PinCache::new(in_memory_object_store(), 16, opts.pin_cache_shard_num, [])
        .await
        .unwrap();
    assert_eq!(cache.shards.len(), 3);

    let blocked = object_in_shard(0, 3);
    let available = object_in_shard(2, 3);
    cache.update_policy([(blocked, 8), (available, 8)]);
    let download = start_download(&cache, available);
    cache
        .store
        .upload(
            &download.entry.as_ref().unwrap().path,
            Bytes::from_static(b"complete"),
        )
        .await
        .unwrap();
    let runtime = tokio::runtime::Handle::current();

    // Keep the locks held until the other thread reports completion. A regression fails
    // with a bounded timeout, then releases the locks so the worker can still exit.
    let result = std::thread::scope(|scope| {
        let control = cache.membership_update.lock();
        let shard = cache.shard(blocked).write();
        let (tx, rx) = std::sync::mpsc::channel();
        let cache = &cache;
        scope.spawn(move || {
            let _runtime = runtime.enter();
            assert!(cache.is_desired(available));
            assert!(cache.get(available).is_none());
            assert_eq!(download.publish(), PinCacheRefillOutcome::Published);
            cache.get(available).unwrap().invalidate();
            assert!(cache.get(available).is_none());
            tx.send(()).unwrap();
        });
        let result = rx.recv_timeout(Duration::from_secs(5));
        drop(shard);
        drop(control);
        result
    });
    result.expect("an unrelated shard or control lock blocked the object lifecycle");
    wait_for_reclaim(&cache).await;
}

#[tokio::test]
async fn test_version_handoff_across_shards() {
    let cache = PinCache::new(in_memory_object_store(), 64, 3, [])
        .await
        .unwrap();

    let objects = [object_in_shard(0, 3), object_in_shard(2, 3)];
    cache.update_version_snapshot(1.into(), objects.into_iter().map(|id| (id, 8)).collect());
    for id in objects {
        let download = start_download(&cache, id);
        cache
            .store
            .upload(
                &download.entry.as_ref().unwrap().path,
                Bytes::from_static(b"complete"),
            )
            .await
            .unwrap();
        assert_eq!(download.publish(), PinCacheRefillOutcome::Published);
    }

    cache.update_version_snapshot(2.into(), HashMap::new());
    for id in objects {
        assert!(!cache.is_desired(id));
        assert!(cache.get(id).is_some());
        assert!(cache.prepare_refill(id).is_some());
    }
    cache.on_version_applied(1.into());
    assert!(objects.iter().all(|&id| cache.get(id).is_some()));
    // Reintroducing one object before applying version 2 must preserve its route.
    cache.update_version_delta(3.into(), [], HashMap::from([(objects[1], 8)]));
    cache.on_version_applied(2.into());
    assert!(cache.get(objects[0]).is_none());
    assert!(cache.is_desired(objects[1]));
    assert!(cache.get(objects[1]).is_some());

    // Retirement is cancelled on reintroduction. Its old deadline must neither accumulate
    // in the schedule nor remove the object after a later retirement.
    cache.update_version_delta(4.into(), [objects[1]], HashMap::new());
    cache.update_version_snapshot(5.into(), HashMap::from([(objects[1], 8)]));
    assert!(cache.shard(objects[1]).read().retirements.is_empty());
    cache.update_version_delta(6.into(), [objects[1]], HashMap::new());
    cache.on_version_applied(4.into());
    assert!(cache.get(objects[1]).is_some());
    cache.on_version_applied(6.into());
    assert!(cache.get(objects[1]).is_none());

    cache.update_policy([]);
    assert!(objects.iter().all(|&id| cache.get(id).is_none()));
    wait_for_reclaim(&cache).await;
}

#[tokio::test]
async fn test_recovery_returns_ready_routes_across_shards() {
    let (_dir, local) = local_object_store().await;
    let objects = [object_in_shard(0, 3), object_in_shard(2, 3)];
    for id in objects {
        for path_id in [1, 2] {
            local
                .upload(
                    &format!("{}-{path_id}.sst", id.as_raw_id()),
                    Bytes::from_static(b"complete"),
                )
                .await
                .unwrap();
        }
        local
            .upload(
                &format!("{}-3.sst", id.as_raw_id()),
                Bytes::from_static(b"short"),
            )
            .await
            .unwrap();
    }
    let cache = PinCache::new(local.clone(), 32, 3, objects.into_iter().map(|id| (id, 8)))
        .await
        .unwrap();

    for id in objects {
        assert!(cache.is_desired(id));
        assert_eq!(
            cache.get(id).unwrap().read(..).await.unwrap(),
            Bytes::from_static(b"complete")
        );
    }
    // Duplicate recovered paths are reclaimed, with exactly one publication per object.
    tokio::time::timeout(Duration::from_secs(5), async {
        while cache.gc.accounted_bytes() != 16 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    cache.update_policy([]);
    wait_for_reclaim(&cache).await;
}
