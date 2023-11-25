package redis

import "cmp"

type color string

const (
	RED   = "R"
	BLACK = "B"
)

type node[k cmp.Ordered, v any] struct {
	key      k
	value    v
	parent   *node[k, v]
	left     *node[k, v]
	right    *node[k, v]
	color    color
	numNodes int
}

type tree[k cmp.Ordered, v any] struct {
	root *node[k, v]
}

func NewTree[k cmp.Ordered, v any]() *tree[k, v] {
	return &tree[k, v]{
		root: nil,
	}
}

func (t tree[k, v]) Get(key k) v {
	n := t.get(key, t.root)
	var result v
	if n == nil {
		return result
	}
	return n.value
}

func (t tree[k, v]) get(key k, n *node[k, v]) (result *node[k, v]) {
	if n == nil {
		return
	}

	if n.key == key {
		result = n
		return
	}

	if key < n.key {
		return t.get(key, n.left)
	}

	return t.get(key, n.right)
}

func (t tree[k, v]) Min() k {
	return t.min(t.root).key
}

func (t tree[k, v]) min(n *node[k, v]) *node[k, v] {
	if n.left == nil {
		return n
	}
	return t.min(n.left)
}

func (t tree[k, v]) Max() k {
	return t.max(t.root).key
}

func (t tree[k, v]) max(n *node[k, v]) *node[k, v] {
	if n.right == nil {
		return n
	}
	return t.max(n.right)
}

func (t *tree[k, v]) Put(key k, val v) {
	t.root = t.put(key, val, t.root, t.root)
	t.root.color = BLACK
}

func (t *tree[k, v]) put(key k, val v, n *node[k, v], parent *node[k, v]) *node[k, v] {
	if n == nil {
		return &node[k, v]{
			key:      key,
			parent:   parent,
			value:    val,
			color:    RED,
			numNodes: 1,
		}
	}

	if n.key > key {
		n.left = t.put(key, val, n.left, n)
	} else if n.key < key {
		n.right = t.put(key, val, n.right, n)
	} else {
		n.value = val
	}

	if isRed(n.right) && !isRed(n.left) {
		n = rotateLeft(n)
	}

	if isRed(n.left) && n.left != nil && isRed(n.left.left) {
		n = rotateRight(n)
	}

	if isRed(n.left) && isRed(n.right) {
		flipColors(n)
	}

	n.numNodes = size(n.left) + size(n.right) + 1
	return n
}

func (t *tree[k, v]) Remove(key k) {
	t.root = t.remove(key, t.root)
}

func (t *tree[k, v]) deleteMin(n *node[k, v]) *node[k, v] {
	if n.left == nil {
		return n.right
	}

	n.left = t.deleteMin(n.left)
	n.numNodes = size(n.left) + size(n.right) + 1
	return n
}

func (t *tree[k, v]) remove(key k, n *node[k, v]) *node[k, v] {
	if n == nil {
		return nil
	}

	if n.key > key {
		n.left = t.remove(key, n.left)
	} else if n.key < key {
		n.right = t.remove(key, n.right)
	} else {
		if n.right == nil {
			return n.left
		}
		if n.left == nil {
			return n.right
		}

		z := n
		n = t.min(z.right)
		n.right = t.deleteMin(z.right)
		n.left = z.left
	}
	n.numNodes = size(n.left) + size(n.right) + 1
	return n
}

func (t tree[k, v]) GetKeySet() []k {
	keys := make([]k, 0)
	t.inOrderTraversal(t.root, &keys)
	return keys
}

func (t tree[k, v]) Size() int {
	return size(t.root)
}

func size[k cmp.Ordered, v any](n *node[k, v]) int {
	if n == nil {
		return 0
	}
	return n.numNodes
}

func (t tree[k, v]) inOrderTraversal(n *node[k, v], collector *[]k) {
	if n == nil {
		return
	}

	t.inOrderTraversal(n.left, collector)
	*collector = append(*collector, n.key)
	t.inOrderTraversal(n.right, collector)
}

func isRed[k cmp.Ordered, v any](n *node[k, v]) bool {
	if n == nil {
		return false
	}
	return n.color == RED
}

func rotateLeft[k cmp.Ordered, v any](h *node[k, v]) *node[k, v] {
	x := h.right

	h.right = x.left
	if h.right != nil {
		h.right.parent = h // parent link update
	}
	x.left = h
	x.color = h.color
	h.color = RED

	hP := h.parent
	h.parent = x
	x.parent = hP

	x.numNodes = h.numNodes
	h.numNodes = size(h.left) + size(h.right) + 1
	return x
}

func rotateRight[k cmp.Ordered, v any](h *node[k, v]) *node[k, v] {
	x := h.left
	h.left = x.right
	if h.left != nil {
		h.left.parent = h // parent link update
	}
	x.right = h
	x.color = h.color
	h.color = RED

	hP := h.parent
	h.parent = x
	x.parent = hP

	x.numNodes = h.numNodes
	h.numNodes = size(h.left) + size(h.right) + 1
	return x
}

func flipColors[k cmp.Ordered, v any](h *node[k, v]) {
	h.color = RED
	h.left.color = BLACK
	h.right.color = BLACK
}
