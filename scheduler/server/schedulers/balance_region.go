// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
	"github.com/pingcap/tidb/util/math"
	"go.uber.org/zap"
	"sort"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

// Schedule implements the scheduling logic for BalanceRegionScheduler.
func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {

	suitableStores := filterStores(cluster)
	if len(suitableStores) <= 1 {
		return nil
	}
	sortStoresByRegionSizeDescending(suitableStores)

	for i, sourceStore := range suitableStores {
		candidateRegion := getCandidateRegions(cluster, sourceStore)
		if candidateRegion != nil {
			if len(candidateRegion.GetStoreIds()) < cluster.GetMaxReplicas() {
				continue // Skip if the region's store count is less than MaxReplicas
			}

			targetStore := findTargetStore(suitableStores, candidateRegion, i)
			if targetStore == nil {
				continue // No suitable target store found, try next region
			}

			// Check if the size difference is significant enough to proceed with the transfer
			if !isSizeDifferenceSignificant(sourceStore, targetStore, candidateRegion) {
				continue
			}

			// Create a new peer on the target store
			newPeer, err := cluster.AllocPeer(targetStore.GetID())
			if err != nil {
				log.Error("failed to allocate peer", zap.Error(err))
				continue
			}

			// Create and return the move peer operator
			op, err := operator.CreateMovePeerOperator("balance-region", cluster, candidateRegion, operator.OpBalance, sourceStore.GetID(), targetStore.GetID(), newPeer.GetId())
			if err != nil {
				log.Error("failed to create move peer operator", zap.Error(err))
				continue
			}
			return op
		}
	}
	return nil
}

// isSizeDifferenceSignificant checks if the size difference between the source and target store is significant.
func isSizeDifferenceSignificant(store *core.StoreInfo, store2 *core.StoreInfo, region *core.RegionInfo) bool {
	return math.Abs(store.GetRegionSize()-store2.GetRegionSize()) > region.GetApproximateSize()
}

// filterStores selects stores with DownTime less than MaxStoreDownTime.
func filterStores(cluster opt.Cluster) []*core.StoreInfo {
	var suitableStores []*core.StoreInfo
	stores := cluster.GetStores()
	for _, store := range stores {
		if store.IsUp() && store.DownTime() <= cluster.GetMaxStoreDownTime() {
			suitableStores = append(suitableStores, store)
		}
	}
	return suitableStores
}

// sortStoresByRegionSizeDescending sorts stores by their region size in descending order.
func sortStoresByRegionSizeDescending(stores []*core.StoreInfo) {
	sort.Slice(stores, func(i, j int) bool {
		return stores[i].GetRegionSize() > stores[j].GetRegionSize()
	})
}

// getCandidateRegions retrieves candidate regions for a given store.
func getCandidateRegions(cluster opt.Cluster, store *core.StoreInfo) *core.RegionInfo {
	var region *core.RegionInfo
	cluster.GetPendingRegionsWithLock(store.GetID(), func(rc core.RegionsContainer) {
		region = rc.RandomRegion(nil, nil)
	})
	if region != nil {
		return region
	}
	cluster.GetFollowersWithLock(store.GetID(), func(rc core.RegionsContainer) {
		region = rc.RandomRegion(nil, nil)
	})
	if region != nil {
		return region
	}
	cluster.GetLeadersWithLock(store.GetID(), func(rc core.RegionsContainer) {
		region = rc.RandomRegion(nil, nil)
	})
	return region
}

// findTargetStore finds a suitable target store for a region to be transferred to.
func findTargetStore(suitableStores []*core.StoreInfo, region *core.RegionInfo, sourceIdx int) *core.StoreInfo {
	for i := len(suitableStores) - 1; i > sourceIdx; i-- {
		store := suitableStores[i]
		if !regionContainsStore(region, store) {
			return store
		}
	}
	return nil
}

// regionContainsStore checks if a region contains a specific store.
func regionContainsStore(region *core.RegionInfo, store *core.StoreInfo) bool {
	for _, peer := range region.GetPeers() {
		if peer.GetStoreId() == store.GetID() {
			return true
		}
	}
	return false
}
