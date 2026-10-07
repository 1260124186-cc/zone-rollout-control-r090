package workflowprobe

import (
	"net/http"
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

	// A bodyless release keeps working exactly as before.
	var released domain.NodeLease
	if err := harness.call(http.MethodPatch, "/v1/leases/"+first.LeaseIDs[0]+"/release", nil, &released); err != nil {
		return err
	}
	if released.State != domain.LeaseReleased {
		return ensure(false, "lease release did not update state")
	}
	if released.ReleaseReason != "" || released.ReleaseNote != "" || released.ReleaseRequestKey != "" {
		return ensure(false, "bodyless release must not record a reason or request key")
	}

	// Releasing the finished lease again with no body returns the original
	// lease without rewriting the terminal state.
	var bodylessReplay domain.NodeLease
	if err := harness.call(http.MethodPatch, "/v1/leases/"+first.LeaseIDs[0]+"/release", nil, &bodylessReplay); err != nil {
		return err
	}
	if bodylessReplay.ID != released.ID || bodylessReplay.State != domain.LeaseReleased {
		return ensure(false, "bodyless replay must return the original released lease")
	}
	if bodylessReplay.FinishedAt == nil || !bodylessReplay.FinishedAt.Equal(*released.FinishedAt) {
		return ensure(false, "bodyless replay must preserve the original finish time")
	}

	// Lease the first node again to exercise the reason-bearing flow.
	reasonGroup, err := allocate(harness, plan.ID, "lease-request-0003", "east", 1)
	if err != nil {
		return err
	}
	reasonLeaseID := reasonGroup.LeaseIDs[0]
	reasoned, err := releaseLease(harness, reasonLeaseID, domain.ReleaseInput{
		RequestKey: "release-request-0001",
		Reason:     domain.ReleaseReasonHandoverComplete,
		Note:       "现场交接完成，提前归还节点",
	})
	if err != nil {
		return err
	}
	if reasoned.State != domain.LeaseReleased {
		return ensure(false, "reasoned release did not update state")
	}
	if reasoned.ReleaseReason != domain.ReleaseReasonHandoverComplete ||
		reasoned.ReleaseNote != "现场交接完成，提前归还节点" ||
		reasoned.ReleaseRequestKey != "release-request-0001" {
		return ensure(false, "reasoned release did not persist the reason and request key")
	}

	// Identical replay returns the original lease and reuses the first reason.
	reasonedReplay, err := releaseLease(harness, reasonLeaseID, domain.ReleaseInput{
		RequestKey: "release-request-0001",
		Reason:     domain.ReleaseReasonHandoverComplete,
		Note:       "现场交接完成，提前归还节点",
	})
	if err != nil {
		return err
	}
	if !sameTerminalLease(reasoned, reasonedReplay) {
		return ensure(false, "identical release replay must return the unchanged terminal lease")
	}

	// Same request key with different content is a stable idempotency conflict.
	_, err = releaseLease(harness, reasonLeaseID, domain.ReleaseInput{
		RequestKey: "release-request-0001",
		Reason:     domain.ReleaseReasonHandoverComplete,
		Note:       "改写终态说明",
	})
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeIdempotencyConflict)); err != nil {
		return err
	}

	// A different release operation against the finished lease is rejected and
	// must not overwrite the first reason.
	_, err = releaseLease(harness, reasonLeaseID, domain.ReleaseInput{
		RequestKey: "release-request-0002",
		Reason:     domain.ReleaseReasonRollback,
	})
	if err := expectFailure(err, http.StatusConflict, string(domain.CodeLeaseReleaseConflict)); err != nil {
		return err
	}

	// The conflicting attempts must leave the original terminal state intact.
	unchanged, err := releaseLease(harness, reasonLeaseID, domain.ReleaseInput{
		RequestKey: "release-request-0001",
		Reason:     domain.ReleaseReasonHandoverComplete,
		Note:       "现场交接完成，提前归还节点",
	})
	if err != nil {
		return err
	}
	if !sameTerminalLease(reasoned, unchanged) {
		return ensure(false, "failed release attempts must not rewrite the terminal lease")
	}

	// Unknown reason categories and reason-less notes fail before the write.
	secondLeaseID := first.LeaseIDs[1]
	_, err = releaseLease(harness, secondLeaseID, domain.ReleaseInput{
		RequestKey: "release-request-0003",
		Reason:     domain.ReleaseReason("mystery_category"),
	})
	if err := expectFailure(err, http.StatusBadRequest, string(domain.CodeUnknownReleaseReason)); err != nil {
		return err
	}
	_, err = releaseLease(harness, secondLeaseID, domain.ReleaseInput{
		Note: "只有说明没有类别",
	})
	if err := expectFailure(err, http.StatusBadRequest, string(domain.CodeValidationFailed)); err != nil {
		return err
	}

	// Concurrent distinct releases race once and then expose a stable
	// conflict; the winning reason must survive and replay idempotently.
	results := make(chan releaseOutcome, 2)
	var wg sync.WaitGroup
	for index, requestKey := range []string{"release-concurrent-a", "release-concurrent-b"} {
		wg.Add(1)
		go func(key string, reason domain.ReleaseReason) {
			defer wg.Done()
			lease, callErr := releaseLease(harness, secondLeaseID, domain.ReleaseInput{
				RequestKey: key,
				Reason:     reason,
			})
			results <- releaseOutcome{lease: lease, err: callErr}
		}(requestKey, []domain.ReleaseReason{
			domain.ReleaseReasonOperatorRequest,
			domain.ReleaseReasonRollback,
		}[index])
	}
	wg.Wait()
	close(results)

	successes := 0
	conflicts := 0
	var winner domain.NodeLease
	for outcome := range results {
		switch {
		case outcome.err == nil:
			successes++
			winner = outcome.lease
		default:
			if err := expectFailure(outcome.err, http.StatusConflict, string(domain.CodeLeaseReleaseConflict)); err != nil {
				return err
			}
			conflicts++
		}
	}
	if successes != 1 || conflicts != 1 {
		return ensure(false, "concurrent releases must resolve to one success and one conflict")
	}
	if winner.State != domain.LeaseReleased {
		return ensure(false, "winning concurrent release must finish the lease")
	}
	winnerReplay, err := releaseLease(harness, secondLeaseID, domain.ReleaseInput{
		RequestKey: winner.ReleaseRequestKey,
		Reason:     winner.ReleaseReason,
		Note:       winner.ReleaseNote,
	})
	if err != nil {
		return err
	}
	if !sameTerminalLease(winner, winnerReplay) {
		return ensure(false, "winning release must replay to the same terminal lease")
	}
	return nil
}

type releaseOutcome struct {
	lease domain.NodeLease
	err   error
}

func sameTerminalLease(left domain.NodeLease, right domain.NodeLease) bool {
	if left.ID != right.ID ||
		left.State != right.State ||
		left.ReleaseRequestKey != right.ReleaseRequestKey ||
		left.ReleaseReason != right.ReleaseReason ||
		left.ReleaseNote != right.ReleaseNote {
		return false
	}
	if left.FinishedAt == nil || right.FinishedAt == nil {
		return left.FinishedAt == nil && right.FinishedAt == nil
	}
	return left.FinishedAt.Equal(*right.FinishedAt)
}
