package btree

/*
Node Structure in Memory:

A node is stored as a byte array with the following layout:
1. Header (4 bytes):
   - Node type (2 bytes): BNODE_NODE or BNODE_LEAF
   - Number of keys (2 bytes)
2. Pointers (8 bytes * number of keys):
   - Used to store references to child nodes
3. Offsets (2 bytes * number of keys):
   - Used to store positions of key-value pairs
4. Key-Value Pairs:
   - Each pair has: key length (2 bytes) + value length (2 bytes) + key bytes + value bytes
*/

import (
	"bytes"
	"encoding/binary"
)

const (
	// Node types
	BNODE_NODE = 1 // Internal nodes that only contain keys and pointers
	BNODE_LEAF = 2 // Leaf nodes that contain keys and values
)

// B+ tree configuration constants
const (
	BTREE_PAGE_SIZE    = 4096 // Size of each node page in bytes
	BTREE_MAX_KEY_SIZE = 1000 // Maximum allowed key size in bytes
	BTREE_MAX_VAL_SIZE = 3000 // Maximum allowed value size in bytes
)

// BNode represents a B+ tree node as a byte slice
type BNode []byte

// Header Operations

// btype returns the type of the node (BNODE_NODE or BNODE_LEAF)
func (node BNode) btype() uint16 {
	return binary.LittleEndian.Uint16(node[0:2])
}

// nkeys returns the number of keys stored in the node
func (node BNode) nkeys() uint16 {
	return binary.LittleEndian.Uint16(node[2:4])
}

// setHeader writes the node type and number of keys to the node header
func (node BNode) setHeader(btype uint16, nkeys uint16) {
	binary.LittleEndian.PutUint16(node[0:2], btype)
	binary.LittleEndian.PutUint16(node[2:4], nkeys)
}

// Pointer Operations

// getPtr returns the child pointer at the given index
func (node BNode) getPtr(idx uint16) uint64 {
	assert(idx < node.nkeys())
	pos := 4 + 8*idx // Skip header (4) + pointer size (8) * index
	return binary.LittleEndian.Uint64(node[pos:])
}

// setPtr sets the child pointer at the given index
func (node BNode) setPtr(idx uint16, val uint64) {
	assert(idx < node.nkeys())
	pos := 4 + 8*idx
	binary.LittleEndian.PutUint64(node[pos:], val)
}

// Offset Operations

// offsetPos calculates the position of the offset for the given index
func offsetPos(node BNode, idx uint16) uint16 {
	assert(1 <= idx && idx <= node.nkeys())
	return 4 + 8*node.nkeys() + 2*(idx-1) // Skip header + pointers + previous offsets
}

// getOffset returns the offset value at the given index
// Index 0 returns 0 as it represents the start of the key-value area
func (node BNode) getOffset(idx uint16) uint16 {
	assert(idx <= node.nkeys())
	if idx == 0 {
		return 0
	}

	pos := 4 + 8*node.nkeys() + 2*(idx-1)
	return binary.LittleEndian.Uint16(node[pos:])
}

// setOffset sets the offset value at the given index
func (node BNode) setOffset(idx uint16, offset uint16) {
	assert(1 <= idx && idx <= node.nkeys())
	binary.LittleEndian.PutUint16(node[offsetPos(node, idx):], offset)
}

// Key-Value Operations

// kvPos calculates the position where the key-value pair starts
func (node BNode) kvPos(idx uint16) uint16 {
	assert(idx <= node.nkeys())
	return 4 + 8*node.nkeys() + 2*node.nkeys() + node.getOffset(idx)
}

// getKey returns the key at the given index
func (node BNode) getKey(idx uint16) []byte {
	assert(idx < node.nkeys())
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos:])
	return node[pos+4:][:klen]
}

// getVal returns the value at the given index
func (node BNode) getVal(idx uint16) []byte {
	assert(idx < node.nkeys())
	pos := node.kvPos(idx)
	klen := binary.LittleEndian.Uint16(node[pos+0:])
	vlen := binary.LittleEndian.Uint16(node[pos+2:])
	return node[pos+4+klen:][:vlen]
}

// nbytes returns the total number of bytes used in the node
func (node BNode) nbytes() uint16 {
	return node.kvPos(node.nkeys())
}

// Search Operations

// nodeLookupLE finds the last position where the key is less than or equal to the target
// Returns the index of the found position, or MAX_UINT16 if no such position exists
func nodeLookupLE(node BNode, key []byte) uint16 {
	nkeys := node.nkeys()

	// Linear search through keys
	for i := uint16(0); i < nkeys; i++ {
		cmp := bytes.Compare(node.getKey(i), key)
		if cmp == 0 {
			return i // Exact match
		}
		if cmp > 0 {
			return i - 1 // Found first key greater than target
		}
	}

	return nkeys - 1 // All keys are less than target
}

// Utility Functions

// assert panics if the condition is false
// Used for runtime validation of node operations
func assert(b bool) {
	if !b {
		panic("assertion failed")
	}
}
