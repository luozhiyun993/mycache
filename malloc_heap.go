// +build appengine windows

package mycache

func getChunk() []byte {
	return make([]byte, chunkSize)
}

func putChunk(chunk []byte) {
	// No-op.
}
