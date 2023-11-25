package redis

import "cmp"

type color string

const (
	RED   = "R"
	BLACK = "B"
)

type node[k cmp.Ordered, v any] struct {
	key    k
	value  v
	parent *node[k, v]
	left   *node[k, v]
	right  *node[k, v]
	color  color
}

func (n *node[k, v]) sibling() *node[k, v] {
	if n == nil || n.parent == nil {
		return nil
	}
	if n == n.parent.left {
		return n.parent.right
	}
	return n.parent.left
}

func (n *node[k, v]) uncle() *node[k, v] {
	if n == nil || n.parent == nil || n.parent.parent == nil {
		return nil
	}
	return n.parent.sibling()
}

func (n *node[k, v]) grandparent() *node[k, v] {
	if n != nil && n.parent != nil {
		return n.parent.parent
	}
	return nil
}

/*
	deletion and insertion algorithms source:

https://github.com/emirpasic/gods/blob/10d6c5b4f2d254fd8c1a2de3e6230a3645a50cd9/trees/redblacktree/redblacktree.go#L1
*/
type tree[k cmp.Ordered, v any] struct {
	root *node[k, v]
	size int
}

func NewTree[k cmp.Ordered, v any]() *tree[k, v] {
	return &tree[k, v]{
		root: nil,
	}
}

func (t tree[k, v]) Get(key k) v {
	n := t.get(key)
	var result v
	if n == nil {
		return result
	}
	return n.value
}

func (t *tree[k, v]) get(key k) *node[k, v] {
	var result *node[k, v]
	p := t.root

	for p != nil {
		if key > p.key {
			p = p.right
		} else if key < p.key {
			p = p.left
		} else {
			result = p
			break
		}
	}

	return result
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
	var newNode *node[k, v]
	if t.root == nil {
		newNode = &node[k, v]{
			key:   key,
			value: val,
			color: RED,
		}
		t.root = newNode
	} else {
		p := t.root
		var y *node[k, v]
		for p != nil {
			y = p

			if key > p.key {
				p = p.right
			} else if key < p.key {
				p = p.left
			} else {
				break
			}
		}

		newNode = &node[k, v]{
			key:    key,
			value:  val,
			parent: y,
			color:  RED,
		}
		if y != nil {
			if key > y.key {
				y.right = newNode
			} else if key < y.key {
				y.left = newNode
			} else {
				y.value = val
				y.color = RED
			}
		}
	}

	t.insertCase1(newNode)
	t.size++
}

func (t *tree[k, v]) insertCase1(n *node[k, v]) {
	if n.parent == nil {
		// n is root, keep it black
		n.color = BLACK
	} else {
		t.insertCase2(n)
	}
}

func (t *tree[k, v]) insertCase2(n *node[k, v]) {
	if isRed(n.parent) {
		t.insertCase3(n)
	}
}

func (t *tree[k, v]) insertCase3(n *node[k, v]) {
	/*
		If the color of the right child of grandparent of n is RED,
		set the color of both the children of grandparent as BLACK and
		the color of grandparent as RED.
	*/
	uncle := n.uncle()
	if isRed(uncle) {
		n.parent.color = BLACK
		uncle.color = BLACK
		gp := n.grandparent()
		if gp != nil {
			gp.color = RED
		}
		t.insertCase1(gp)
	} else {
		t.insertCase4(n)
	}
}

func (t *tree[k, v]) insertCase4(n *node[k, v]) {
	gp := n.grandparent()
	if n == n.parent.right && n.parent == gp.left {
		t.rotateLeft(n.parent)
		n = n.left
	} else if n == n.parent.left && n.parent == gp.right {
		t.rotateRight(n.parent)
		n = n.right
	}
	t.insertCase5(n)
}

func (t *tree[k, v]) insertCase5(n *node[k, v]) {
	n.parent.color = BLACK
	gp := n.grandparent()
	gp.color = RED
	if n == n.parent.left && n.parent == gp.left {
		t.rotateRight(gp)
	} else if n == n.parent.right && n.parent == gp.right {
		t.rotateLeft(gp)
	}
}

// Replaces old node with new node
func (t *tree[k, val]) replace(o *node[k, val], n *node[k, val]) {
	if o.parent == nil {
		t.root = n
	} else if o == o.parent.left {
		o.parent.left = n
	} else {
		o.parent.right = n
	}
	if n != nil {
		n.parent = o.parent
	}
}

func (t *tree[k, v]) Remove(key k) {
	n := t.get(key)
	t.remove(n)
}

func (t *tree[k, v]) remove(n *node[k, v]) {
	if n == nil {
		return
	}

	if n.left != nil && n.right != nil {
		/* node to be deleted has both children */
		predecessor := t.max(n.left)
		n.key = predecessor.key
		n.value = predecessor.value
		n = predecessor
	}
	var c *node[k, v]
	if n.left == nil || n.right == nil {
		/* node to be deleted has one or no child */
		if n.right == nil {
			c = n.left
		} else {
			c = n.right
		}

		if !isRed(n) {
			if c != nil {
				n.color = c.color
			}
			t.deleteCase1(n)
		}
		// if the node is a red leaf, just remove it
		t.replace(n, c)
		if n.parent == nil && c != nil {
			c.color = BLACK
		}
	}
	t.size--
}

func (t *tree[k, v]) deleteCase1(n *node[k, v]) {
	if n.parent == nil {
		return
	}
	t.deleteCase2(n)
}

func (t *tree[k, v]) deleteCase2(n *node[k, v]) {
	sibling := n.sibling()
	if isRed(sibling) {
		n.parent.color = RED
		sibling.color = BLACK
		if n == n.parent.left {
			t.rotateLeft(n.parent)
		} else {
			t.rotateRight(n.parent)
		}
	}
	t.deleteCase3(n)
}

func (t *tree[k, v]) deleteCase3(n *node[k, v]) {
	sibling := n.sibling()
	if !isRed(n.parent) &&
		!isRed(sibling) &&
		!isRed(sibling.left) &&
		!isRed(sibling.right) {
		sibling.color = RED
		t.deleteCase1(n.parent)
	} else {
		t.deleteCase4(n)
	}
}

func (t *tree[k, v]) deleteCase4(n *node[k, v]) {
	sibling := n.sibling()
	if isRed(n.parent) &&
		!isRed(sibling) &&
		!isRed(sibling.left) &&
		!isRed(sibling.right) {
		sibling.color = RED
		n.parent.color = BLACK
	} else {
		t.deleteCase5(n)
	}
}

func (t *tree[k, v]) deleteCase5(n *node[k, v]) {
	sibling := n.sibling()
	if n == n.parent.left &&
		!isRed(sibling) &&
		isRed(sibling.left) &&
		!isRed(sibling.right) {
		sibling.color = RED
		sibling.left.color = BLACK
		t.rotateRight(sibling)
	} else if n == n.parent.right &&
		!isRed(sibling) &&
		isRed(sibling.right) &&
		!isRed(sibling.left) {
		sibling.color = RED
		sibling.right.color = BLACK
		t.rotateLeft(sibling)
	}
	t.deleteCase6(n)
}

func (t *tree[k, v]) deleteCase6(n *node[k, v]) {
	sibling := n.sibling()
	sibling.color = n.parent.color
	n.parent.color = BLACK
	if n == n.parent.left && isRed(sibling.right) {
		sibling.right.color = BLACK
		t.rotateLeft(n.parent)
	} else if isRed(sibling.left) {
		sibling.left.color = BLACK
		t.rotateRight(n.parent)
	}
}

func (t tree[k, v]) GetKeySet() []k {
	return t.RangeGetKeys(t.Min(), t.Max())
}

func (t tree[k, v]) Size() int {
	return t.size
}

func (t tree[k, v]) InOrderTraversal(visitor func(*node[k, v])) {
	t.inOrderTraversal(t.root, visitor)
}

func (t tree[k, v]) inOrderTraversal(n *node[k, v], visitor func(*node[k, v])) {
	if n == nil {
		return
	}

	t.inOrderTraversal(n.left, visitor)
	visitor(n)
	t.inOrderTraversal(n.right, visitor)
}

func (t tree[k, v]) PreOrderTraversal(visitor func(*node[k, v])) {
	t.preOrderTraversal(t.root, visitor)
}

func (t tree[k, v]) preOrderTraversal(n *node[k, v], visitor func(*node[k, v])) {
	if n == nil {
		return
	}

	visitor(n)
	t.preOrderTraversal(n.left, visitor)
	t.preOrderTraversal(n.right, visitor)
}

func (t tree[k, v]) PostOrderTraversal(visitor func(*node[k, v])) {
	t.postOrderTraversal(t.root, visitor)
}

func (t tree[k, v]) postOrderTraversal(n *node[k, v], visitor func(*node[k, v])) {
	if n == nil {
		return
	}

	t.postOrderTraversal(n.left, visitor)
	t.postOrderTraversal(n.right, visitor)
	visitor(n)
}

func isRed[k cmp.Ordered, v any](n *node[k, v]) bool {
	if n == nil {
		return false
	}
	return n.color == RED
}

func (t *tree[k, v]) rotateLeft(h *node[k, v]) {
	x := h.right
	t.replace(h, x)
	h.right = x.left
	if h.right != nil {
		h.right.parent = h // parent link update
	}
	x.left = h
	h.parent = x
}

func (t *tree[k, v]) rotateRight(h *node[k, v]) {
	x := h.left
	t.replace(h, x)
	h.left = x.right
	if h.left != nil {
		h.left.parent = h // parent link update
	}
	x.right = h
	h.parent = x
}

func (t *tree[k, v]) RangeGetKeys(lo k, hi k) []k {
	results := make([]k, 0)
	t.rangeGetKeys(t.root, lo, hi, &results)
	return results
}

func (t tree[k, v]) rangeGetKeys(n *node[k, v], lo k, hi k, collector *[]k) {
	if n == nil {
		return
	}

	if n.key > lo {
		t.rangeGetKeys(n.left, lo, hi, collector)
	}

	if n.key >= lo && n.key <= hi {
		*collector = append(*collector, n.key)
	}

	if n.key < hi {
		t.rangeGetKeys(n.right, lo, hi, collector)
	}
}
