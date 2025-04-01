package btree

import (
	"bytes"
	"encoding/binary"
)

// BTree represents a B+ tree structure for efficient key-value storage
// The tree maintains data in sorted order and supports efficient insertions,
// deletions, and range queries
type BTree struct {
	// Root is a pointer to the root node (nonzero page number)
	// Value of 0 indicates an empty tree
	Root uint64

	// Storage interface callbacks for managing on-disk pages
	Get func(uint64) []byte // Reads data from a page number
	New func([]byte) uint64 // Allocates a new page and returns its number
	Del func(uint64)        // Deallocates a page by its number
}

// nodeAppendKV appends a key-value pair to a node at the specified index
// Parameters:
// - new: target node to append to
// - idx: position where to insert
// - ptr: child pointer (used in internal nodes)
// - key: key to insert
// - val: value to insert
func nodeAppendKV(new BNode, idx uint16, ptr uint64, key []byte, val []byte) {
	// Set child pointer (used for internal nodes)
	new.setPtr(idx, ptr)

	// Calculate position for key-value data
	pos := new.kvPos(idx)

	// Write key and value lengths (2 bytes each)
	binary.LittleEndian.PutUint16(new[pos+0:], uint16(len(key)))
	binary.LittleEndian.PutUint16(new[pos+2:], uint16(len(val)))

	// Write actual key and value data
	copy(new[pos+4:], key)
	copy(new[pos+4+uint16(len(key)):], val)

	// Update offset for the next entry
	new.setOffset(idx+1, new.getOffset(idx)+4+uint16((len(key)+len(val))))
}

// nodeAppendRange copies a range of key-value pairs from one node to another
// Used during node splits and merges
func nodeAppendRange(new BNode, old BNode, dstNew uint16, srcOld uint16, n uint16) {
	for i := uint16(0); i < n; i++ {
		dst, src := dstNew+i, srcOld+i
		nodeAppendKV(new, dst, old.getPtr(src), old.getKey(src), old.getVal(src))
	}
}

// leafInsert inserts a new key-value pair into a leaf node
// Creates a new node with the inserted pair at the specified position
func leafInsert(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys()+1)
	nodeAppendRange(new, old, 0, 0, idx)                   // copy the keys before 'idx'
	nodeAppendKV(new, idx, 0, key, val)                    // the new key
	nodeAppendRange(new, old, idx+1, idx, old.nkeys()-idx) // keys from 'idx'
}

// leafUpdate updates an existing key's value in a leaf node
// Creates a new node with the updated value
func leafUpdate(new BNode, old BNode, idx uint16, key []byte, val []byte) {
	new.setHeader(BNODE_LEAF, old.nkeys())
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, 0, key, val)
	nodeAppendRange(new, old, idx+1, idx+1, old.nkeys()-(idx+1))
}

// nodeSplit2 splits a node into two nodes (left and right)
// Ensures that both resulting nodes fit within page size limits
func nodeSplit2(left BNode, right BNode, old BNode) {
	assert(old.nkeys() >= 2)

	// the initial guess
	nleft := old.nkeys() / 2

	// try to fit the left half
	left_bytes := func() uint16 {
		return 4 + 8*nleft + 2*nleft + old.getOffset(nleft)
	}

	for left_bytes() > BTREE_PAGE_SIZE {
		nleft--
	}

	assert(nleft >= 1)

	// try to fit the right half
	right_bytes := func() uint16 {
		return old.nbytes() - left_bytes() + 4
	}

	for right_bytes() > BTREE_PAGE_SIZE {
		nleft++
	}

	assert(nleft < old.nkeys())

	nright := old.nkeys() - nleft

	// new nodes
	left.setHeader(old.btype(), nleft)
	right.setHeader(old.btype(), nright)
	nodeAppendRange(left, old, 0, 0, nleft)
	nodeAppendRange(right, old, 0, nleft, nright)

	// NOTE: the left half may be still too big
	assert(right.nbytes() <= BTREE_PAGE_SIZE)
}

// nodeSplit3 splits a node into up to three nodes if necessary
// Returns the number of resulting nodes and the nodes themselves
func nodeSplit3(old BNode) (uint16, [3]BNode) {
	if old.nbytes() <= BTREE_PAGE_SIZE {
		old = old[:BTREE_PAGE_SIZE]
		return 1, [3]BNode{old} // not split
	}

	left := BNode(make([]byte, 2*BTREE_PAGE_SIZE)) // might be split later
	right := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(left, right, old)

	if left.nbytes() <= BTREE_PAGE_SIZE {
		left = left[:BTREE_PAGE_SIZE]
		return 2, [3]BNode{left, right} // split into 2 nodes
	}

	leftleft := BNode(make([]byte, BTREE_PAGE_SIZE))
	middle := BNode(make([]byte, BTREE_PAGE_SIZE))
	nodeSplit2(leftleft, middle, left)

	assert(leftleft.nbytes() <= BTREE_PAGE_SIZE)
	return 3, [3]BNode{leftleft, middle, right} // 3 nodes
}

// treeInsert handles recursive insertion into the B+ tree
// Returns the modified node after insertion
func treeInsert(tree *BTree, node BNode, key []byte, val []byte) BNode {
	// The extra size allows it to exceed 1 page temporarily
	new := BNode(make([]byte, 2*BTREE_PAGE_SIZE))

	// where to insert the key
	idx := nodeLookupLE(node, key) // node.getKey(idx) <= key

	switch node.btype() {
	case BNODE_LEAF: // leaf node
		if bytes.Equal(key, node.getKey(idx)) {
			leafUpdate(new, node, idx, key, val) // found, update it
		} else {
			leafInsert(new, node, idx+1, key, val) // not found, insert
		}

	case BNODE_NODE: // internal node, walk into the child node
		// recursive insertion to the kid node
		kptr := node.getPtr(idx)
		knode := treeInsert(tree, tree.Get(kptr), key, val)

		// after insertion, split the result
		nsplit, split := nodeSplit3(knode)

		// deallocate the old kid node
		tree.Del(kptr)

		// update the kid links
		nodeReplaceKidN(tree, new, node, idx, split[:nsplit]...)
	}

	return new
}

// nodeReplaceKidN replaces a child node with multiple nodes (after split)
func nodeReplaceKidN(tree *BTree, new BNode, old BNode, idx uint16, kids ...BNode) {
	inc := uint16(len(kids))

	new.setHeader(BNODE_NODE, old.nkeys()+inc-1)
	nodeAppendRange(new, old, 0, 0, idx)

	for i, node := range kids {
		nodeAppendKV(new, idx+uint16(i), tree.New(node), node.getKey(0), nil)
	}

	nodeAppendRange(new, old, idx+inc, idx+1, old.nkeys()-(idx+1))
}

// Insert adds or updates a key-value pair in the tree
func (tree *BTree) Insert(key []byte, val []byte) {
	if tree.Root == 0 {
		// create the first node
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_LEAF, 2)

		// a dummy (sentinel) key, this makes the tree cover the whole key space.
		// thus a lookup can always find a containing node.
		nodeAppendKV(root, 0, 0, nil, nil)
		// insert the actual key-value pair
		nodeAppendKV(root, 1, 0, key, val)
		tree.Root = tree.New(root)
		return
	}

	node := treeInsert(tree, tree.Get(tree.Root), key, val)
	nsplit, split := nodeSplit3(node)
	tree.Del(tree.Root)
	if nsplit > 1 {
		// the root was split, add a new level.
		root := BNode(make([]byte, BTREE_PAGE_SIZE))
		root.setHeader(BNODE_NODE, nsplit)

		for i, knode := range split[:nsplit] {
			ptr, key := tree.New(knode), knode.getKey(0)
			nodeAppendKV(root, uint16(i), ptr, key, nil)
		}

		tree.Root = tree.New(root)
	} else {
		tree.Root = tree.New(split[0])
	}
}

func (tree *BTree) Search(key []byte) ([]byte, bool) {
	if tree.Root == 0 {
		return nil, false
	}
	return treeSearch(tree, tree.Get(tree.Root), key)
}

func treeSearch(tree *BTree, node BNode, key []byte) ([]byte, bool) {
	idx := nodeLookupLE(node, key)

	switch node.btype() {
	case BNODE_LEAF:
		if idx < node.nkeys() && bytes.Equal(node.getKey(idx), key) {
			return node.getVal(idx), true
		}
		return nil, false

	case BNODE_NODE:
		return treeSearch(tree, tree.Get(node.getPtr(idx)), key)
	}

	return nil, false
}

func (tree *BTree) Delete(key []byte) {
	if tree.Root == 0 {
		return
	}
	node := treeDelete(tree, tree.Get(tree.Root), key)
	if len(node) > 0 {
		tree.Root = tree.New(node)
	}
}

func shouldMerge(tree *BTree, node BNode, idx uint16, updated BNode) (int, BNode) {
	if updated.nbytes() > BTREE_PAGE_SIZE/4 {
		return 0, BNode{}
	}

	if idx > 0 {
		sibling := BNode(tree.Get(node.getPtr(idx - 1)))
		merged := sibling.nbytes() + updated.nbytes() - 4 // 4 is HEADER
		if merged <= BTREE_PAGE_SIZE {
			return -1, sibling // left
		}
	}

	if idx+1 < node.nkeys() {
		sibling := BNode(tree.Get(node.getPtr(idx + 1)))
		merged := sibling.nbytes() + updated.nbytes() - 4 // 4 is HEADER
		if merged <= BTREE_PAGE_SIZE {
			return +1, sibling // right
		}
	}

	return 0, BNode{}
}

func nodeMerge(dest BNode, left BNode, right BNode) {
	dest.setHeader(left.btype(), left.nkeys()+right.nkeys())
	nodeAppendRange(dest, left, 0, 0, left.nkeys())
	nodeAppendRange(dest, right, left.nkeys(), 0, right.nkeys())
}

func nodeReplace2Kid(new BNode, old BNode, idx uint16, ptr uint64, key []byte) {
	new.setHeader(BNODE_NODE, old.nkeys()-1)
	nodeAppendRange(new, old, 0, 0, idx)
	nodeAppendKV(new, idx, ptr, key, nil)
	nodeAppendRange(new, old, idx+1, idx+2, old.nkeys()-(idx+1))
}

func nodeDelete(tree *BTree, node BNode, idx uint16, key []byte) BNode {
	// recurse into the kid
	kptr := node.getPtr(idx)
	updated := treeDelete(tree, tree.Get(kptr), key)
	if len(updated) == 0 {
		return BNode{} // not found
	}
	tree.Del(kptr)

	new := BNode(make([]byte, BTREE_PAGE_SIZE))
	// check for merging
	mergeDir, sibling := shouldMerge(tree, node, idx, updated)
	switch {
	case mergeDir < 0: // left
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, sibling, updated)
		tree.Del(node.getPtr(idx - 1))
		nodeReplace2Kid(new, node, idx-1, tree.New(merged), merged.getKey(0))

	case mergeDir > 0: // right
		merged := BNode(make([]byte, BTREE_PAGE_SIZE))
		nodeMerge(merged, updated, sibling)
		tree.Del(node.getPtr(idx + 1))
		nodeReplace2Kid(new, node, idx, tree.New(merged), merged.getKey(0))

	case mergeDir == 0 && updated.nkeys() == 0:
		assert(node.nkeys() == 1 && idx == 0) // 1 empty child but no sibling
		new.setHeader(BNODE_NODE, 0)          // the parent becomes empty too

	case mergeDir == 0 && updated.nkeys() > 0: // no merge
		nodeReplaceKidN(tree, new, node, idx, updated)
	}

	return new
}

func treeDelete(tree *BTree, node BNode, key []byte) BNode {
	idx := nodeLookupLE(node, key)

	switch node.btype() {
	case BNODE_LEAF:
		if idx < node.nkeys() && bytes.Equal(node.getKey(idx), key) {
			new := BNode(make([]byte, BTREE_PAGE_SIZE))
			new.setHeader(BNODE_LEAF, node.nkeys()-1)
			nodeAppendRange(new, node, 0, 0, idx)
			nodeAppendRange(new, node, idx, idx+1, node.nkeys()-idx-1)
			return new
		}
		return BNode{}

	case BNODE_NODE:
		updated := nodeDelete(tree, node, idx, key)
		if len(updated) == 0 {
			return BNode{}
		}
		return updated
	}

	return BNode{}
}

func (tree *BTree) Traverse(visit func(key, val []byte)) {
	if tree.Root == 0 {
		return
	}
	treeTraverse(tree, tree.Get(tree.Root), visit)
}

func treeTraverse(tree *BTree, node BNode, visit func(key, val []byte)) {
	switch node.btype() {
	case BNODE_LEAF:
		for i := uint16(1); i < node.nkeys(); i++ {
			visit(node.getKey(i), node.getVal(i))
		}
	case BNODE_NODE:
		for i := uint16(0); i < node.nkeys(); i++ {
			treeTraverse(tree, tree.Get(node.getPtr(i)), visit)
		}
	}
}
