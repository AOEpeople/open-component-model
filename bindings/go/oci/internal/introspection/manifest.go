package introspection

import (
	ociImageSpecV1 "github.com/opencontainers/image-spec/specs-go/v1"
)

// IsOCICompliantManifest checks if a descriptor describes a manifest that is recognizable by OCI.
func IsOCICompliantManifest(desc ociImageSpecV1.Descriptor) bool {
	return IsOCICompliantMediaType(desc.MediaType)
}

// IsOCICompliantMediaType checks if a media type is recognised by OCI or Docker registry
// clients as a manifest (as opposed to a raw blob). Manifests must be pushed to and resolved
// from the manifests endpoint (/v2/{name}/manifests/{reference}); blobs use the blobs endpoint.
// Getting this wrong causes two cascading failures:
//   - resolution uses the blobs endpoint → 404 (Docker manifests live in the manifest store)
//   - classification sends the descriptor to Layers instead of AdditionalDescriptorManifests
//     → Harbor rejects the component version index PUT with 400 "blob unknown"
func IsOCICompliantMediaType(mediaType string) bool {
	switch mediaType {
	case ociImageSpecV1.MediaTypeImageManifest,
		ociImageSpecV1.MediaTypeImageIndex,
		// Docker schema v2 single-platform manifest
		"application/vnd.docker.distribution.manifest.v2+json",
		// Docker schema v2 multi-platform manifest list (the common case for multi-arch images)
		"application/vnd.docker.distribution.manifest.list.v2+json":
		return true
	default:
		return false
	}
}
