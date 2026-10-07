package workflowprobe

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"

	"solo-0014-zone-rollout-control/internal/domain"
)

func runNodeLeaseAllocation() error {
	harness, err := newHarness()
	if err != nil {
		return err
	}
	defer harness.close()

	if _, err := enrollNode(harness, "resolver-east-01", "east"); err != nil {
		return err
	}
	if _, err := enrollNode(harness, "resolver-east-02", "east"); err != nil {
		return err
	}
	plan, _, err := createActivePlan(harness, "lease.example", 1001, 2, 0.5, 100)
	if err != nil {
		return err
	}
	first, err := allocate(harness, plan.ID, "lease-request-0001", "east", 2)
	if err != nil {
		return err
	}
	if len(first.LeaseIDs) != 2 {
		return ensure(false, "first lease group must contain two leases")
	}
	replay, err := allocate(harness, plan.ID, "lease-request-0001", "east", 2)
	if err != nil {
		return err
	}
	if replay.ID != first.ID {
		return ensure(false, "lease request replay returned another group")
	}
	_, err = allocate(harness, plan.ID, "lease-request-0002", "east", 2)
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeNodeUnavailable)); err != nil {
		return err
	}

	// Release with an optional reason category and handover note.
	leaseOne := first.LeaseIDs[0]
	released, err := releaseLease(harness, leaseOne, domain.ReleaseInput{
		RequestKey:     "release-request-0001",
		ReasonCategory: "plan_abandoned",
		ReasonNote:     "现场交接：窗口取消，提前归还节点",
	})
	if err != nil {
		return err
	}
	if released.State != domain.LeaseReleased {
		return ensure(false, "lease release did not update state")
	}
	if released.ReleaseReason != domain.ReleaseReasonPlanAbandoned || released.ReleaseID == "" {
		return ensure(false, "release did not persist the reason category")
	}
	if released.ReleaseNote != "现场交接：窗口取消，提前归还节点" {
		return ensure(false, "release did not persist the handover note")
	}

	// Repeating the same release returns the original lease and reuses
	// the first reason without rewriting the terminal state.
	repeated, err := releaseLease(harness, leaseOne, domain.ReleaseInput{
		RequestKey:     "release-request-0001",
		ReasonCategory: "plan_abandoned",
		ReasonNote:     "现场交接：窗口取消，提前归还节点",
	})
	if err != nil {
		return err
	}
	if repeated.ID != released.ID || repeated.ReleaseID != released.ReleaseID {
		return ensure(false, "release replay returned another lease or release record")
	}
	if repeated.ReleaseReason != domain.ReleaseReasonPlanAbandoned {
		return ensure(false, "release replay changed the first reason")
	}
	if repeated.FinishedAt == nil || released.FinishedAt == nil ||
		!repeated.FinishedAt.Equal(*released.FinishedAt) {
		return ensure(false, "release replay rewrote the terminal finished_at")
	}

	// Same request key with different content is an idempotency conflict.
	_, err = releaseLease(harness, leaseOne, domain.ReleaseInput{
		RequestKey:     "release-request-0001",
		ReasonCategory: "other",
	})
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeIdempotencyConflict)); err != nil {
		return err
	}

	// A different operation request on the already released lease is a
	// stable release conflict and must not overwrite the terminal reason.
	_, err = releaseLease(harness, leaseOne, domain.ReleaseInput{
		RequestKey:     "release-request-0002",
		ReasonCategory: "zone_rollback",
	})
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeReleaseConflict)); err != nil {
		return err
	}

	// Unknown reason categories are rejected before any state change.
	leaseTwo := first.LeaseIDs[1]
	_, err = releaseLease(harness, leaseTwo, domain.ReleaseInput{
		RequestKey:     "release-request-0003",
		ReasonCategory: "mystery",
	})
	if err := expectFailure(err, http.StatusBadRequest, string(domain.CodeValidationFailed)); err != nil {
		return err
	}

	// Concurrent releases racing for one claimed lease: exactly one wins,
	// the other receives a stable conflict.
	raceErrors := make(chan error, 2)
	raceResults := make(chan domain.NodeLease, 2)
	var wg sync.WaitGroup
	for index := 0; index < 2; index++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			lease, callErr := releaseLease(harness, leaseTwo, domain.ReleaseInput{
				RequestKey:     fmt.Sprintf("release-race-%04d", index),
				ReasonCategory: "operator_choice",
			})
			if callErr != nil {
				raceErrors <- callErr
				return
			}
			raceResults <- lease
		}(index)
	}
	wg.Wait()
	close(raceErrors)
	close(raceResults)
	if len(raceErrors) != 1 {
		return ensure(false, "concurrent release must have exactly one loser")
	}
	if err := expectFailure(<-raceErrors, http.StatusConflict, string(domain.CodeReleaseConflict)); err != nil {
		return err
	}
	winner := <-raceResults
	if winner.State != domain.LeaseReleased || winner.ReleaseReason != domain.ReleaseReasonOperatorChoice {
		return ensure(false, "concurrent release winner did not complete")
	}

	// The original body-less release request still works against an
	// already released lease and keeps returning the terminal lease.
	legacy, err := releaseLease(harness, leaseOne, nil)
	if err != nil {
		return err
	}
	if legacy.State != domain.LeaseReleased || legacy.ReleaseReason != domain.ReleaseReasonPlanAbandoned {
		return ensure(false, "legacy body-less release changed behavior")
	}

	// Audit must link the lease, plan, node, operation request and reason.
	snapshot, err := readSnapshot(harness)
	if err != nil {
		return err
	}
	releasedAudit := 0
	for _, event := range snapshot.AuditEvents {
		if event.EntityType != "lease" || event.EntityID != leaseOne || event.Action != "released" {
			continue
		}
		releasedAudit++
		if auditString(event, "plan_id") != plan.ID {
			return ensure(false, "release audit is missing the plan reference")
		}
		if auditString(event, "node_id") != released.NodeID {
			return ensure(false, "release audit is missing the node reference")
		}
		if auditString(event, "request_key") != "release-request-0001" {
			return ensure(false, "release audit is missing the operation request key")
		}
		if auditString(event, "reason_category") != "plan_abandoned" {
			return ensure(false, "release audit is missing the reason category")
		}
		if auditString(event, "reason_note") != "现场交接：窗口取消，提前归还节点" {
			return ensure(false, "release audit is missing the handover note")
		}
	}
	if releasedAudit != 1 {
		return ensure(false, "release replay must not append another audit event")
	}
	return nil
}

func auditString(event domain.AuditEvent, key string) string {
	value, _ := event.Attributes[key].(string)
	return value
}

func readSnapshot(harness *harness) (domain.State, error) {
	payload, err := os.ReadFile(harness.statePath())
	if err != nil {
		return domain.State{}, fmt.Errorf("read state snapshot: %w", err)
	}
	var snapshot domain.State
	if err := json.Unmarshal(payload, &snapshot); err != nil {
		return domain.State{}, fmt.Errorf("decode state snapshot: %w", err)
	}
	return snapshot, nil
}
