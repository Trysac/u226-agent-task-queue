// Package agenttaskqueue is a small, dependency-free Go library: In-memory priority task queue for agent workers in Go: leases with tokens, retries with capped exponential backoff, a bounded dead list and an injected clock
//
// The public API is every exported identifier of this root package. Functions are pure: they do
// no I/O, keep no global mutable state and report invalid input through returned errors.
package agenttaskqueue
