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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use risingwave_common::bitmap::Bitmap;
use risingwave_hummock_sdk::HummockSstableObjectId;
use risingwave_hummock_sdk::compaction_group::hummock_version_ext::SstDeltaInfo;
use risingwave_hummock_sdk::sstable_info::SstableInfo;
use risingwave_hummock_sdk::version::HummockVersion;
use risingwave_pb::id::TableId;

use crate::hummock::local_version::pinned_version::PinnedVersion;
use crate::hummock::pin_cache::{PinCacheReadHandle, PinCacheRefillOutcome, PinCacheRefillToken};
use crate::hummock::refill_locality::{block_vnode_range, vnode_range_overlaps_bitmap};
use crate::hummock::{HummockError, HummockResult, Sstable, SstableStoreRef};
use crate::monitor::StoreLocalStatistic;

mod executor;
use executor::{PinCacheRefillExecutor, Ticket};

/// Immutable worker-local admission. A matching block admits the complete physical object.
#[derive(Clone, Default)]
pub(crate) struct PinCacheRefillPlan {
    pub objects: HashMap<HummockSstableObjectId, Vec<SstableInfo>>,
    pub ownership: Arc<HashMap<TableId, Bitmap>>,
}

impl PinCacheRefillPlan {
    pub(crate) fn new(
        deltas: &[SstDeltaInfo],
        candidates: &HashSet<HummockSstableObjectId>,
        ownership: HashMap<TableId, Bitmap>,
    ) -> Self {
        let mut objects: HashMap<_, Vec<_>> = HashMap::new();
        for sst in deltas.iter().flat_map(|delta| &delta.insert_sst_infos) {
            if candidates.contains(&sst.object_id)
                && sst
                    .table_ids
                    .iter()
                    .any(|table| ownership.contains_key(table))
            {
                objects.entry(sst.object_id).or_default().push(sst.clone());
            }
        }
        Self {
            objects,
            ownership: Arc::new(ownership),
        }
    }

    pub(crate) fn retain_tables(&mut self, pinned_tables: &HashSet<TableId>) {
        Arc::make_mut(&mut self.ownership).retain(|table, _| pinned_tables.contains(table));
        self.objects.retain(|_, infos| {
            infos.iter().any(|info| {
                info.table_ids
                    .iter()
                    .any(|table| self.ownership.contains_key(table))
            })
        });
    }

    pub(crate) fn owns_object(
        sst: &Sstable,
        projections: &[SstableInfo],
        ownership: &HashMap<TableId, Bitmap>,
    ) -> bool {
        sst.meta
            .block_metas
            .iter()
            .enumerate()
            .any(|(index, block)| {
                let table = block.table_id();
                projections
                    .iter()
                    .any(|info| info.table_ids.contains(&table))
                    && ownership.get(&table).is_some_and(|bitmap| {
                        vnode_range_overlaps_bitmap(block_vnode_range(sst, index), bitmap)
                    })
            })
    }
}

/// Ownership rejection can retire only the publication that was checked. A stale backend
/// token says nothing about ownership of the currently published route.
enum PinCacheRefillAttemptOutcome {
    Completed(PinCacheRefillOutcome),
    NotOwned(Option<PinCacheReadHandle>),
}

async fn refill_pin_cache_object(
    store: &SstableStoreRef,
    projections: &[SstableInfo],
    ownership: &HashMap<TableId, Bitmap>,
    token: PinCacheRefillToken,
) -> Result<PinCacheRefillAttemptOutcome, PinCacheRefillError> {
    let route = store
        .pin_cache()
        .and_then(|cache| cache.get(token.object_id()));
    if !pin_cache_object_is_owned(store, projections, ownership)
        .await
        .map_err(|error| PinCacheRefillError {
            phase: "ownership_meta",
            error,
        })?
    {
        return Ok(PinCacheRefillAttemptOutcome::NotOwned(route));
    }
    store
        .pin_sst(token)
        .await
        .map(PinCacheRefillAttemptOutcome::Completed)
        .map_err(|error| PinCacheRefillError {
            phase: "object_copy",
            error,
        })
}

struct PinCacheRefillError {
    phase: &'static str,
    error: HummockError,
}

async fn pin_cache_object_is_owned(
    store: &SstableStoreRef,
    projections: &[SstableInfo],
    ownership: &HashMap<TableId, Bitmap>,
) -> HummockResult<bool> {
    let Some(info) = projections.first() else {
        return Ok(false);
    };
    let mut stats = StoreLocalStatistic::default();
    let sst = store.sstable(info, &mut stats).await;
    stats.discard();
    let sst = sst?;
    Ok(PinCacheRefillPlan::owns_object(
        &sst,
        projections,
        ownership,
    ))
}

#[derive(Clone, Copy)]
pub(crate) enum PinCacheMembershipUpdate {
    Delta,
    Rebuild,
}

/// Maintains logical pin-cache membership separately from Foyer block-refill policy.
pub(crate) struct PinCacheRefillController {
    sstable_store: SstableStoreRef,
    pinned_table_ids: Option<HashSet<TableId>>,
    object_ref_counts: HashMap<HummockSstableObjectId, u32>,
    version: PinnedVersion,
    ownership: Arc<HashMap<TableId, Bitmap>>,
    executor: PinCacheRefillExecutor,
}

impl PinCacheRefillController {
    pub(crate) fn new(
        sstable_store: SstableStoreRef,
        version: PinnedVersion,
        concurrency: Arc<tokio::sync::Semaphore>,
    ) -> Self {
        let executor = PinCacheRefillExecutor::new(sstable_store.clone(), concurrency);
        Self {
            sstable_store,
            pinned_table_ids: None,
            object_ref_counts: HashMap::new(),
            version,
            ownership: Arc::default(),
            executor,
        }
    }

    pub(crate) fn replace_pinned_tables(&mut self, pinned_table_ids: HashSet<TableId>) {
        if self.pinned_table_ids.as_ref() == Some(&pinned_table_ids) {
            return;
        }
        self.pinned_table_ids = Some(pinned_table_ids);
        self.rebuild_desired_objects(false);
    }

    /// Reprojects admitted work only. Ownership arrival or changes do not warm existing SSTs
    /// or validate recovered files through remote metadata reads. Recovery keeps local files
    /// selected by the pin policy and version; locality is checked when admitting a new refill.
    /// Published files are not rescanned on vnode changes and may stay until unpin/version removal.
    pub(crate) fn update_ownership(&mut self, ownership: HashMap<TableId, Bitmap>) {
        if self.ownership.as_ref() == &ownership {
            return;
        }
        self.ownership = Arc::new(ownership);
        if let Some(cache) = self.sstable_store.pin_cache() {
            cache.revoke_retired();
        }
        self.executor.reproject(self.ownership.clone());
    }

    pub(crate) fn submit(&self, mut plan: PinCacheRefillPlan) -> Ticket {
        // Retain policy admission while projecting unstarted work onto current ownership.
        plan.ownership = Arc::new(
            self.ownership
                .iter()
                .filter(|(table, _)| plan.ownership.contains_key(*table))
                .map(|(&table, bitmap)| (table, bitmap.clone()))
                .collect(),
        );
        plan.objects.retain(|_, infos| {
            infos.iter().any(|info| {
                info.table_ids
                    .iter()
                    .any(|table| plan.ownership.contains_key(table))
            })
        });
        self.executor.submit(plan)
    }

    pub(crate) fn on_version_applied(&self, version: risingwave_hummock_sdk::HummockVersionId) {
        if let Some(cache) = self.sstable_store.pin_cache() {
            cache.on_version_applied(version);
        }
        self.executor.reproject(self.ownership.clone());
    }

    pub(crate) fn apply_version_update(
        &mut self,
        deltas: &[SstDeltaInfo],
        new_version: PinnedVersion,
        membership_update: PinCacheMembershipUpdate,
    ) -> HashSet<HummockSstableObjectId> {
        self.version = new_version;
        let Some(pin_cache) = self.sstable_store.pin_cache().cloned() else {
            return HashSet::new();
        };
        match membership_update {
            PinCacheMembershipUpdate::Delta => {
                if self.apply_desired_object_delta(deltas).is_none() {
                    tracing::warn!(
                        "pin-cache object reference count is inconsistent; rebuilding membership"
                    );
                    self.rebuild_desired_objects(true);
                }
            }
            PinCacheMembershipUpdate::Rebuild => {
                self.rebuild_desired_objects(true);
            }
        };

        deltas
            .iter()
            .flat_map(|delta| &delta.insert_sst_infos)
            .filter(|sst| pin_cache.is_desired(sst.object_id))
            .map(|sst| sst.object_id)
            .collect()
    }

    /// Enumerates pinned logical SST references from a version without reading object storage.
    /// Startup recovery and subsequent policy rebuilds use the same membership selection.
    pub(crate) fn pinned_ssts<'a>(
        version: &'a HummockVersion,
        pinned_table_ids: &'a HashSet<TableId>,
    ) -> impl Iterator<Item = &'a SstableInfo> {
        let compaction_group_ids = pinned_table_ids
            .iter()
            .filter_map(|table_id| {
                version
                    .state_table_info
                    .info()
                    .get(table_id)
                    .map(|info| info.compaction_group_id)
            })
            .collect::<HashSet<_>>();
        compaction_group_ids
            .into_iter()
            .flat_map(move |compaction_group_id| {
                let levels = version.get_compaction_group_levels(compaction_group_id);
                levels.l0.sub_levels.iter().chain(levels.levels.iter())
            })
            .flat_map(|level| &level.table_infos)
            .filter(move |sst| Self::is_pinned(sst, pinned_table_ids))
    }

    fn rebuild_desired_objects(&mut self, preserve_versions: bool) {
        let Some(pin_cache) = self.sstable_store.pin_cache().cloned() else {
            self.object_ref_counts.clear();
            return;
        };
        let Some(pinned_table_ids) = &self.pinned_table_ids else {
            self.object_ref_counts.clear();
            return;
        };

        let mut objects = HashMap::new();
        let mut object_ref_counts = HashMap::new();
        for sst in Self::pinned_ssts(&self.version, pinned_table_ids) {
            objects
                .entry(sst.object_id)
                .and_modify(|size| {
                    assert_eq!(
                        *size, sst.file_size,
                        "one object must have one physical size"
                    )
                })
                .or_insert(sst.file_size);
            *object_ref_counts.entry(sst.object_id).or_insert(0) += 1;
        }
        if preserve_versions {
            pin_cache.update_version_snapshot(self.version.id(), objects);
        } else {
            pin_cache.update_policy(objects.iter().map(|(&id, &size)| (id, size)));
        }
        self.object_ref_counts = object_ref_counts;
    }

    fn apply_desired_object_delta(&mut self, deltas: &[SstDeltaInfo]) -> Option<()> {
        let Some(pin_cache) = self.sstable_store.pin_cache().cloned() else {
            return Some(());
        };
        let Some(pinned_table_ids) = &self.pinned_table_ids else {
            return Some(());
        };

        let mut initial_counts = HashMap::new();
        let mut inserted_sizes = HashMap::new();
        for delta in deltas {
            for sst in delta
                .delete_sst_infos
                .iter()
                .filter(|sst| Self::is_pinned(sst, pinned_table_ids))
            {
                let count = self.object_ref_counts.get_mut(&sst.object_id)?;
                initial_counts.entry(sst.object_id).or_insert(*count);
                *count = count.checked_sub(1)?;
                if *count == 0 {
                    self.object_ref_counts.remove(&sst.object_id);
                }
            }
            for sst in delta
                .insert_sst_infos
                .iter()
                .filter(|sst| Self::is_pinned(sst, pinned_table_ids))
            {
                let count = self.object_ref_counts.entry(sst.object_id).or_insert(0);
                initial_counts.entry(sst.object_id).or_insert(*count);
                *count = count.checked_add(1)?;
                inserted_sizes
                    .entry(sst.object_id)
                    .and_modify(|size| {
                        assert_eq!(
                            *size, sst.file_size,
                            "one object must have one physical size"
                        )
                    })
                    .or_insert(sst.file_size);
            }
        }

        let mut removed = Vec::new();
        let mut inserted = HashMap::new();
        for (object_id, initial_count) in initial_counts {
            let final_count = self.object_ref_counts.get(&object_id).copied().unwrap_or(0);
            if initial_count > 0 && final_count == 0 {
                removed.push(object_id);
            }
            if initial_count == 0 && final_count > 0 {
                inserted.insert(object_id, inserted_sizes[&object_id]);
            }
        }
        pin_cache.update_version_delta(self.version.id(), removed, inserted);
        Some(())
    }

    fn is_pinned(sst: &SstableInfo, pinned_table_ids: &HashSet<TableId>) -> bool {
        sst.table_ids
            .iter()
            .any(|table_id| pinned_table_ids.contains(table_id))
    }
}
