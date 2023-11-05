package redis

type listnode struct {
	value string
	next  *listnode
}

type list struct {
	head *listnode
	tail *listnode
	size int
}

func (l *list) AppendToTail(value string) {
	node := &listnode{value: value}

	if l.size == 0 {
		l.tail = node
		l.head = node
	} else {
		tail := l.tail
		tail.next = node
		l.tail = node
	}

	l.size += 1
}

func (l *list) AppendSliceToTail(values []string) {
	for _, v := range values {
		l.AppendToTail(v)
	}
}

func (l *list) ToSlice() []string {
	result := []string{}

	p := l.head
	for p != nil {
		result = append(result, p.value)
		p = p.next
	}

	return result
}

func (l *list) AppendToHead(value string) {
	node := &listnode{value: value}

	if l.size == 0 {
		l.tail = node
		l.head = node
	} else {
		head := l.head
		node.next = head
		l.head = node
	}

	l.size += 1
}

func (l *list) AppendSliceToHead(values []string) {
	for _, v := range values {
		l.AppendToHead(v)
	}
}

func NewListFromSlice(values []string) list {
	l := list{}
	l.AppendSliceToTail(values)
	return l
}
