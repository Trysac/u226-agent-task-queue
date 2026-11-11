// Package agenttaskqueue provides an in-memory priority task queue for agent workers.
//
// It supports leased tasks with token-checked acknowledgements, retries with
// capped exponential backoff, priority ordering with FIFO tie-breaking, and a
// bounded dead list. The queue is safe for concurrent use and can be driven by
// an injected clock and token source for deterministic testing.
package agenttaskqueue
