package allocation

import (
	"context"
	"sort"
	"strings"
	"time"

	"solo-0014-zone-rollout-control/internal/domain"
	"solo-0014-zone-rollout-control/internal/platform"
	"solo-0014-zone-rollout-control/internal/store"
)

type Service struct {
	store *store.Store
	clock platform.Clock
	ids   platform.IDGenerator
}

func New(stateStore *store.Store, clock platform.Clock, ids platform.IDGenerator) *Service {
	return &Service{store: stateStore, clock: clock, ids: ids}
}

func (s *Service) Allocate(
	ctx context.Context,
	planID string,
	input domain.LeaseInput,
) (domain.LeaseGroup, error) {
	if err := domain.ValidateLeaseInput(input); err != nil {
		return domain.LeaseGroup{}, err
	}
	input.Region = strings.TrimSpace(strings.ToLower(input.Region))
	requestIndex := planID + "\x00" + input.RequestKey
	fingerprint := domain.LeaseFingerprint(planID, input)
	now := s.clock.Now().UTC()
	var group domain.LeaseGroup

	err := s.store.Write(ctx, func(state *domain.State) error {
		plan, ok := state.Plans[planID]
		if !ok {
			return domain.NotFound("plan", planID)
		}
		if plan.State != domain.PlanActive {
			return domain.InvalidState("plan must be active for node leasing", map[string]any{
				"plan_id": planID,
				"state":   plan.State,
			})
		}
		if existingID, exists := state.LeaseRequestKeys[requestIndex]; exists {
			existing, found := state.LeaseGroups[existingID]
			if !found {
				return domain.NewError(domain.CodeStorageFailure, "lease request index is inconsistent", map[string]any{
					"lease_group_id": existingID,
				})
			}
			if existing.RequestFingerprint != fingerprint {
				return domain.NewError(domain.CodeIdempotencyConflict, "request key was reused with different lease parameters", map[string]any{
					"request_key": input.RequestKey,
				})
			}
			group = existing
			return nil
		}

		expireLeases(state, now)
		candidates := make([]domain.ResolverNode, 0)
		for _, node := range state.Nodes {
			if node.State != domain.NodeReady {
				continue
			}
			if input.Region != "" && node.Region != input.Region {
				continue
			}
			candidates = append(candidates, node)
		}
		if len(candidates) == 0 {
			return domain.NewError(domain.CodeNodeUnavailable, "no eligible node is ready", map[string]any{
				"plan_id": planID,
				"region":  input.Region,
			})
		}
		sort.Slice(candidates, func(i, j int) bool {
			if candidates[i].Region != candidates[j].Region {
				return candidates[i].Region < candidates[j].Region
			}
			return candidates[i].ID < candidates[j].ID
		})
		if len(candidates) > input.MaxNodes {
			candidates = candidates[:input.MaxNodes]
		}

		groupID := s.ids.New("grp")
		leaseIDs := make([]string, 0, len(candidates))
		expiresAt := now.Add(time.Duration(input.LeaseSeconds) * time.Second)
		for _, candidate := range candidates {
			leaseID := s.ids.New("lse")
			candidate.State = domain.NodeLeased
			candidate.Version++
			candidate.UpdatedAt = now
			state.Nodes[candidate.ID] = candidate
			state.Leases[leaseID] = domain.NodeLease{
				ID:           leaseID,
				PlanID:       planID,
				NodeID:       candidate.ID,
				AllocationID: groupID,
				State:        domain.LeaseClaimed,
				ClaimedAt:    now,
				ExpiresAt:    expiresAt,
			}
			leaseIDs = append(leaseIDs, leaseID)
		}
		group = domain.LeaseGroup{
			ID:                 groupID,
			PlanID:             planID,
			RequestKey:         input.RequestKey,
			RequestFingerprint: fingerprint,
			Region:             input.Region,
			LeaseIDs:           leaseIDs,
			CreatedAt:          now,
		}
		state.LeaseGroups[group.ID] = group
		state.LeaseRequestKeys[requestIndex] = group.ID
		store.AddAudit(state, s.ids, "lease_group", group.ID, "created", now, map[string]any{
			"plan_id":     planID,
			"lease_count": len(leaseIDs),
		})
		return nil
	})
	return group, err
}

func (s *Service) Release(
	ctx context.Context,
	leaseID string,
	input domain.ReleaseInput,
) (domain.NodeLease, error) {
	input = domain.NormalizeReleaseInput(input)
	if err := domain.ValidateReleaseInput(input); err != nil {
		return domain.NodeLease{}, err
	}
	fingerprint := domain.ReleaseFingerprint(leaseID, input)
	now := s.clock.Now().UTC()
	var result domain.NodeLease
	err := s.store.Write(ctx, func(state *domain.State) error {
		lease, ok := state.Leases[leaseID]
		if !ok {
			return domain.NotFound("lease", leaseID)
		}

		// A keyed request is an idempotent operation: the identical replay
		// returns the original terminal lease and the first release reason.
		if input.RequestKey != "" {
			requestIndex := leaseID + "\x00" + input.RequestKey
			if existingID, exists := state.ReleaseRequestKeys[requestIndex]; exists {
				existing, found := state.LeaseReleases[existingID]
				if !found {
					return domain.NewError(domain.CodeStorageFailure, "release request index is inconsistent", map[string]any{
						"release_id": existingID,
					})
				}
				if existing.PayloadFingerprint != fingerprint {
					return domain.NewError(domain.CodeIdempotencyConflict, "request key was reused with different release content", map[string]any{
						"lease_id":    leaseID,
						"request_key": input.RequestKey,
					})
				}
				current, found := state.Leases[existing.LeaseID]
				if !found {
					return domain.NewError(domain.CodeStorageFailure, "release references a missing lease", map[string]any{
						"lease_id": existing.LeaseID,
					})
				}
				result = current
				return nil
			}
		}

		if lease.State == domain.LeaseReleased {
			if input.RequestKey != "" {
				return domain.NewError(domain.CodeReleaseConflict, "lease was already released by another request", map[string]any{
					"lease_id": leaseID,
					"state":    lease.State,
				})
			}
			result = lease
			return nil
		}
		if lease.State == domain.LeaseExpired {
			if input.RequestKey != "" {
				return domain.NewError(domain.CodeLeaseExpired, "lease has expired and cannot be released", map[string]any{
					"lease_id": leaseID,
					"state":    lease.State,
				})
			}
			result = lease
			return nil
		}
		if lease.State != domain.LeaseClaimed {
			return domain.InvalidState("only a claimed lease can be released", map[string]any{
				"lease_id": leaseID,
				"state":    lease.State,
			})
		}
		node, ok := state.Nodes[lease.NodeID]
		if !ok {
			return domain.NewError(domain.CodeStorageFailure, "lease references a missing node", map[string]any{
				"node_id": lease.NodeID,
			})
		}

		releaseID := s.ids.New("rel")
		reason := domain.ReleaseReason(input.ReasonCategory)
		release := domain.LeaseRelease{
			ID:                 releaseID,
			LeaseID:            leaseID,
			PlanID:             lease.PlanID,
			NodeID:             lease.NodeID,
			RequestKey:         input.RequestKey,
			ReasonCategory:     reason,
			PayloadFingerprint: fingerprint,
			ReleasedAt:         now,
		}
		if input.ReasonNote != "" {
			release.ReasonNote = input.ReasonNote
		}
		state.LeaseReleases[releaseID] = release
		if input.RequestKey != "" {
			state.ReleaseRequestKeys[leaseID+"\x00"+input.RequestKey] = releaseID
		}

		finishedAt := now
		lease.State = domain.LeaseReleased
		lease.FinishedAt = &finishedAt
		lease.ReleaseID = releaseID
		lease.ReleaseReason = reason
		lease.ReleaseNote = release.ReasonNote
		state.Leases[leaseID] = lease
		if node.State == domain.NodeLeased {
			node.State = domain.NodeReady
			node.Version++
			node.UpdatedAt = now
			state.Nodes[node.ID] = node
		}

		attributes := map[string]any{
			"plan_id":    lease.PlanID,
			"node_id":    lease.NodeID,
			"release_id": releaseID,
		}
		if input.RequestKey != "" {
			attributes["request_key"] = input.RequestKey
		}
		if input.ReasonCategory != "" {
			attributes["reason_category"] = input.ReasonCategory
		}
		if input.ReasonNote != "" {
			attributes["reason_note"] = input.ReasonNote
		}
		store.AddAudit(state, s.ids, "lease", leaseID, "released", now, attributes)
		result = lease
		return nil
	})
	return result, err
}

func (s *Service) ExpireDue(ctx context.Context) (int, error) {
	now := s.clock.Now().UTC()
	expired := 0
	err := s.store.Write(ctx, func(state *domain.State) error {
		expired = expireLeases(state, now)
		return nil
	})
	return expired, err
}

func expireLeases(state *domain.State, now time.Time) int {
	count := 0
	for leaseID, lease := range state.Leases {
		if lease.State != domain.LeaseClaimed || lease.ExpiresAt.After(now) {
			continue
		}
		finishedAt := now
		lease.State = domain.LeaseExpired
		lease.FinishedAt = &finishedAt
		state.Leases[leaseID] = lease
		if node, ok := state.Nodes[lease.NodeID]; ok && node.State == domain.NodeLeased {
			node.State = domain.NodeReady
			node.Version++
			node.UpdatedAt = now
			state.Nodes[node.ID] = node
		}
		count++
	}
	return count
}
