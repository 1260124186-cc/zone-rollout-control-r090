package httpapi

import (
	"net/http"

	"solo-0014-zone-rollout-control/internal/domain"
)

func (s *Server) allocateLeases(w http.ResponseWriter, r *http.Request) {
	var input domain.LeaseInput
	if err := decode(w, r, &input); err != nil {
		sendError(w, r, err)
		return
	}
	group, err := s.allocations.Allocate(r.Context(), r.PathValue("id"), input)
	if err != nil {
		sendError(w, r, err)
		return
	}
	sendData(w, r, http.StatusCreated, group)
}

func (s *Server) releaseLease(w http.ResponseWriter, r *http.Request) {
	// The original body-less release request remains valid; the reason
	// category, note, and idempotency request key are all optional.
	var input domain.ReleaseInput
	if r.ContentLength != 0 {
		if err := decode(w, r, &input); err != nil {
			sendError(w, r, err)
			return
		}
	}
	lease, err := s.allocations.Release(r.Context(), r.PathValue("id"), input)
	if err != nil {
		sendError(w, r, err)
		return
	}
	sendData(w, r, http.StatusOK, lease)
}
