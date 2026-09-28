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

use risingwave_common::util::iter_util::ZipEqFast;
use risingwave_hummock_sdk::{HummockSstableObjectId, HummockVersionId};

use super::{PinCache, PinCacheState};

impl PinCacheState {
    fn apply_desired_object_delta(
        &mut self,
        version: HummockVersionId,
        removed: impl IntoIterator<Item = HummockSstableObjectId>,
        inserted: HashMap<HummockSstableObjectId, u64>,
    ) {
        for object_id in removed {
            // Remove+insert of the same immutable object keeps its admission and file.
            if !inserted.contains_key(&object_id) {
                self.retire_object(object_id, version);
            }
        }
        for (object_id, size) in inserted {
            self.insert_desired(object_id, size);
        }
    }

    fn replace_desired_objects(&mut self, desired: HashMap<HummockSstableObjectId, u64>) {
        // A policy replacement ends previous-version retention too.
        self.remove_objects_if(|id, object| desired.get(&id) != Some(&object.size()));
        for (id, size) in desired {
            self.insert_desired(id, size);
        }
    }
}

impl PinCache {
    /// Completes a version handoff after readers can use `applied`.
    /// Withdraws objects retained only for earlier versions, removing their local read routes.
    pub(crate) fn on_version_applied(&self, applied: HummockVersionId) {
        self.revoke_retired_through(applied);
    }

    /// Withdraws all objects retained only for previous versions, for example on ownership loss.
    /// Current-version membership and refills are unchanged.
    pub(crate) fn revoke_retired(&self) {
        self.revoke_retired_through(u64::MAX.into());
    }

    fn revoke_retired_through(&self, applied: HummockVersionId) {
        let _update = self.membership_update.lock();
        for shard in &self.shards {
            let mut state = shard.write();
            while let Some(&(version, id)) = state.retirements.first() {
                if version > applied {
                    break;
                }
                state.retirements.pop_first();
                let mut object = state
                    .objects
                    .remove(&id)
                    .expect("retirement belongs to an object");
                object.take_published();
            }
        }
    }

    fn partition_objects(
        &self,
        objects: impl IntoIterator<Item = (HummockSstableObjectId, u64)>,
    ) -> Vec<HashMap<HummockSstableObjectId, u64>> {
        let mut shards = vec![HashMap::new(); self.shards.len()];
        for (id, size) in objects {
            if let Some(previous) =
                shards[Self::shard_index(id, self.shards.len())].insert(id, size)
            {
                assert_eq!(previous, size, "one object must have one physical size");
            }
        }
        shards
    }

    /// Replaces the SST membership selected by the current pin policy.
    /// Removed objects lose their routes and refill admission immediately. This also ends all
    /// previous-version retention, even if the replacement version has not been applied yet.
    pub(crate) fn update_policy(
        &self,
        objects: impl IntoIterator<Item = (HummockSstableObjectId, u64)>,
    ) {
        let desired = self.partition_objects(objects);
        let _update = self.membership_update.lock();
        for (shard, desired) in self.shards.iter().zip_eq_fast(desired) {
            shard.write().replace_desired_objects(desired);
        }
    }

    /// Installs a version snapshot, retaining removed objects until `on_version_applied(version)`.
    pub(crate) fn update_version_snapshot(
        &self,
        version: HummockVersionId,
        objects: HashMap<HummockSstableObjectId, u64>,
    ) {
        let desired = self.partition_objects(objects);
        let _update = self.membership_update.lock();
        for (shard, desired) in self.shards.iter().zip_eq_fast(desired) {
            let mut state = shard.write();
            let PinCacheState {
                objects,
                retirements,
                ..
            } = &mut *state;
            for (&id, object) in objects {
                if object.retire_at.is_none() && !desired.contains_key(&id) {
                    object.retire_at = Some(version);
                    retirements.insert((version, id));
                }
            }
            for (id, size) in desired {
                state.insert_desired(id, size);
            }
        }
    }

    /// Updates membership from a version delta after the initial snapshot.
    /// Removed objects remain available until `on_version_applied(version)`; a policy or ownership
    /// change can revoke them sooner. Does not download the inserted objects.
    pub(crate) fn update_version_delta(
        &self,
        version: HummockVersionId,
        removed: impl IntoIterator<Item = HummockSstableObjectId>,
        inserted: HashMap<HummockSstableObjectId, u64>,
    ) {
        let mut removed_by_shard = vec![Vec::new(); self.shards.len()];
        for id in removed {
            removed_by_shard[Self::shard_index(id, self.shards.len())].push(id);
        }
        let inserted_by_shard = self.partition_objects(inserted);
        let _update = self.membership_update.lock();
        for ((shard, removed), inserted) in self
            .shards
            .iter()
            .zip_eq_fast(removed_by_shard)
            .zip_eq_fast(inserted_by_shard)
        {
            if !removed.is_empty() || !inserted.is_empty() {
                shard
                    .write()
                    .apply_desired_object_delta(version, removed, inserted);
            }
        }
    }

    /// Whether the current policy/version selects the object, excluding previous-version retention.
    /// A false result does not imply that `get` cannot still return a route during a version handoff.
    pub(crate) fn is_desired(&self, object_id: HummockSstableObjectId) -> bool {
        self.shard(object_id)
            .read()
            .objects
            .get(&object_id)
            .is_some_and(|object| object.retire_at.is_none())
    }
}
