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
	for p.next != nil {
		result = append(result, p.value)
		p = p.next
	}

	return result
}

func NewListFromSlice(values []string) list {
	l := list{}
	for _, v := range values {
		l.AppendToTail(v)
	}
	return l
}
