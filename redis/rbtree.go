package redis

import "cmp"

type node[k cmp.Ordered, v any] struct {
	key   k
	value v
	left  *node[k, v]
	right *node[k, v]
}

type tree[k cmp.Ordered, v any] struct {
	root *node[k, v]
	size int
}

func NewTree[k cmp.Ordered, v any]() *tree[k, v] {
	return &tree[k, v]{
		size: 0,
		root: nil,
	}
}

func (t tree[k, v]) Get(key k) v {
	x := t.root

	for x != nil && key != x.key {
		if key < x.key {
			x = x.left
		} else {
			x = x.right
		}
	}

	if x == nil {
		var result v
		return result
	}

	return x.value
}

func (t *tree[k, v]) Put(key k, val v) {
	var p *node[k, v]
	x := t.root

	for x != nil {
		p = x

		if key < x.key {
			x = x.left
		} else {
			x = x.right
		}
	}

	newNode := &node[k, v]{key: key, value: val}
	if p == nil {
		t.root = newNode
	} else if key < p.key {
		p.left = newNode
	} else {
		p.right = newNode
	}
	t.size++
}

func (t *tree[k, v]) Remove(key k) v {
	removed := t.remove(key, t.root, t.root)

	var result v
	if removed == nil {
		return result
	}

	return removed.value
}

func (t *tree[k, v]) remove(key k, n *node[k, v], parent *node[k, v]) *node[k, v] {
	if n == nil {
		return nil
	}

	if n.key > key {
		return t.remove(key, n.left, n)
	}
	if n.key < key {
		return t.remove(key, n.right, n)
	}

	markedForDeletion := &node[k, v]{key: n.key, value: n.value}
	t.size -= 1

	// if the node being deleted has no children, simply delete it
	if n.right == nil && n.left == nil {
		if parent.left.key == n.key {
			parent.left = nil
		}
		if parent.right.key == n.key {
			parent.right = nil
		}
		return markedForDeletion
	}

	/* if the node being deleted has one child, delete the node and
	plug the child in the spot where the deleted node was */
	if n.right != nil && n.left == nil {
		n.key = n.right.key
		n.value = n.right.value
		n.right = nil
		return markedForDeletion
	}

	if n.left != nil && n.right == nil {
		n.key = n.left.key
		n.value = n.left.value
		n.left = nil
		return markedForDeletion
	}

	// if the node has two children, replace the node with the successor node
	successorAndParent := t.findSuccessorWithParent(n.right, n)
	successor := successorAndParent[0]
	successorParent := successorAndParent[1]
	n.key = successor.key
	n.value = successor.value
	if successorParent.left != nil && successorParent.left.key == successor.key {
		successorParent.left = nil
	}
	if successorParent.right != nil && successorParent.right.key == successor.key {
		successorParent.right = nil
	}

	/* if the successor node has a right child, after plugging the
	successor node into the spot of the deleted node, take the former
	child of the successor node and turn into the left child of
	the former parent of the  successor node
	*/
	if successor.right != nil {
		successorParent.left = successor.right
	}

	return markedForDeletion
}

// Keep visiting the left child until there is no more child left, then return the childless node
func (t tree[k, v]) findSuccessorWithParent(n *node[k, v], parent *node[k, v]) []*node[k, v] {
	result := []*node[k, v]{}
	if n.left == nil {
		result = append(result, n)
		result = append(result, parent)
		return result
	}

	return t.findSuccessorWithParent(n.left, n)
}

func (t tree[k, v]) GetKeySet() []k {
	keys := make([]k, 0)
	t.inOrderTraversal(t.root, &keys)
	return keys
}

func (t tree[key, v]) Size() int {
	return t.size
}

func (t tree[k, v]) inOrderTraversal(n *node[k, v], collector *[]k) {
	if n == nil {
		return
	}

	t.inOrderTraversal(n.left, collector)
	*collector = append(*collector, n.key)
	t.inOrderTraversal(n.right, collector)
}
