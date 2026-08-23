package v1alpha1

import "github.com/satwiksps/streamweld/internal/conformance"

// FindCachedBackendProbe returns the newest independent probe metadata whose
// exact (imageDigest, spec.model, tokenizerHash) cache key belongs to the
// current route generation. Backend identity, address, and serving state are
// deliberately excluded from the protocol cache key and cleared in the
// result; callers must bind those fields to the current candidate.
func (route *InferenceRoute) FindCachedBackendProbe(
	model string,
	imageDigest string,
	tokenizerHash string,
) (*BackendStatus, bool) {
	if route == nil || model == "" || imageDigest == "" || tokenizerHash == "" ||
		model != route.Spec.Model || route.Status.ObservedGeneration != route.Generation {
		return nil, false
	}
	var newest *BackendStatus
	for index := range route.Status.Backends {
		backend := &route.Status.Backends[index]
		if backend.ImageDigest != imageDigest || backend.TokenizerHash != tokenizerHash ||
			backend.LastProbedAt == nil || backend.LastProbedAt.IsZero() ||
			backend.TemplateVerdict == conformance.VerdictUnknown || !backend.TemplateVerdict.Valid() {
			continue
		}
		if newest == nil || backend.LastProbedAt.After(newest.LastProbedAt.Time) ||
			(backend.LastProbedAt.Equal(newest.LastProbedAt) && backend.ID < newest.ID) {
			newest = backend
		}
	}
	if newest == nil {
		return nil, false
	}
	cached := newest.DeepCopy()
	cached.ID = ""
	cached.Address = ""
	cached.Ready = false
	cached.Draining = false
	return cached, true
}
