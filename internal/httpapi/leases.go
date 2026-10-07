package httpapi

import (
	"encoding/json"
	"errors"
	"io"
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
	// Release remains callable with no body; an empty body means no
	// idempotency key and no release reason.
	input := domain.ReleaseInput{}
	if r.ContentLength != 0 {
		r.Body = http.MaxBytesReader(w, r.Body, requestLimit)
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			if errors.Is(err, io.EOF) {
				input = domain.ReleaseInput{}
			} else {
				sendError(w, r, domain.Validation("request body must contain valid JSON", map[string]any{
					"reason": err.Error(),
				}))
				return
			}
		} else {
			var extra any
			if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
				sendError(w, r, domain.Validation("request body must contain one JSON value", nil))
				return
			}
		}
	}
	lease, err := s.allocations.Release(r.Context(), r.PathValue("id"), input)
	if err != nil {
		sendError(w, r, err)
		return
	}
	sendData(w, r, http.StatusOK, lease)
}
