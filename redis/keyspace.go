package redis

import (
	"fmt"
	"strconv"
	"sync"
	"time"
)

type keyspaceEntry struct {
	group   string
	expires *time.Time
}

type keyspace struct {
	clock         ClockTimer
	mutex         *sync.RWMutex
	keys          map[string]keyspaceEntry
	stringMap     map[string]string
	listMap       map[string]list
	sortedSetMap  map[string]rbtree[float64, string]
	modifications int
}

type KeyResult struct {
	str *string
	arr []string
}

func (kr KeyResult) IsValid() bool {
	hasString := kr.str != nil
	hasArr := kr.arr != nil
	return hasString || hasArr
}

func (kr KeyResult) IsString() bool {
	hasString := kr.str != nil
	return kr.IsValid() && hasString
}

func newKeyspace(clock ClockTimer, m *sync.RWMutex) *keyspace {
	return &keyspace{
		mutex:         m,
		clock:         clock,
		keys:          make(map[string]keyspaceEntry),
		stringMap:     make(map[string]string),
		listMap:       make(map[string]list),
		sortedSetMap:  make(map[string]rbtree[float64, string]),
		modifications: 0,
	}
}

func (ks *keyspace) Get(key string) KeyResult {
	ks.mutex.RLock()
	ke, ok := ks.keys[key]
	ks.mutex.RUnlock()

	if !ok {
		return KeyResult{}
	}

	if ke.expires != nil && ks.clock.Now().After(*ke.expires) {
		ks.mutex.Lock()
		switch ke.group {
		case "string":
			delete(ks.stringMap, key)

		case "list":
			delete(ks.listMap, key)
		}

		delete(ks.keys, key)
		ks.modifications += 1
		ks.mutex.Unlock()

		return KeyResult{}
	}

	var kr KeyResult
	ks.mutex.RLock()
	switch ke.group {
	default:
		kr = KeyResult{}
	case "string":
		v := ks.stringMap[key]
		kr = KeyResult{str: &v}
	case "list":
		v := ks.listMap[key]
		kr = KeyResult{arr: v.ToSlice()}
	}
	ks.mutex.RUnlock()

	return kr
}

func (ks *keyspace) Expire(key string, duration int64) bool {
	ks.mutex.Lock()
	defer ks.mutex.Unlock()

	ke, ok := ks.keys[key]
	if !ok {
		return false
	}

	var final time.Time
	if ke.expires == nil {
		final = ks.clock.Now().Add(time.Duration(duration) * time.Second)
	} else {
		// update by adding time to key expiry
		final = ke.expires.Add(time.Duration(duration) * time.Second)
	}

	ke.expires = &final
	ks.keys[key] = ke
	ks.modifications += 1

	return true
}

func (ks *keyspace) ExpireAt(key string, deadline time.Time) bool {
	ks.mutex.Lock()
	defer ks.mutex.Unlock()

	ke, ok := ks.keys[key]
	if !ok {
		return false
	}

	ke.expires = &deadline
	ks.keys[key] = ke
	ks.modifications += 1

	return true
}

func (ks *keyspace) Exists(key string) bool {
	return ks.Get(key).IsValid()
}

func (ks *keyspace) BulkExists(keys []string) map[string]int {
	ks.mutex.RLock()
	defer ks.mutex.RUnlock()

	keyCount := map[string]int{}
	for _, key := range keys {
		_, ok := ks.keys[key]
		_, kcOk := keyCount[key]
		if ok {
			if kcOk {
				keyCount[key] += 1
			} else {
				keyCount[key] = 1
			}
		} else {
			keyCount[key] = 0
		}
	}
	return keyCount
}

func (ks *keyspace) BulkDelete(keys []string) map[string]int {
	ks.mutex.Lock()
	defer ks.mutex.Unlock()

	keyCount := map[string]int{}
	for _, key := range keys {
		ke, ok := ks.keys[key]
		_, kcOk := keyCount[key]
		if ok {

			switch ke.group {
			case "string":
				delete(ks.stringMap, key)
			case "list":
				delete(ks.listMap, key)
			}

			delete(ks.keys, key)
			ks.modifications += 1

			if kcOk {
				keyCount[key] += 1
			} else {
				keyCount[key] = 1
			}
		} else {
			if !kcOk {
				keyCount[key] = 0
			}
		}
	}
	return keyCount
}

type ExpiryDuration struct {
	magnitude  int64
	resolution time.Duration
}

func (ks *keyspace) SetStringKey(key string, value string, exp *ExpiryDuration) {
	ks.mutex.Lock()
	defer ks.mutex.Unlock()

	ke, ok := ks.keys[key]
	if ok && ke.group == "list" {
		delete(ks.listMap, key)
	}
	ks.stringMap[key] = value
	newKey := keyspaceEntry{group: "string", expires: nil}

	if exp != nil {
		final := ks.clock.Now().Add(time.Duration(exp.magnitude) * exp.resolution)
		newKey.expires = &final
	}

	ks.keys[key] = newKey
	ks.modifications += 1
}

func (ks *keyspace) SetListKey(key string, value []string, exp *ExpiryDuration) {
	ks.mutex.Lock()
	defer ks.mutex.Unlock()

	ke, ok := ks.keys[key]
	if ok && ke.group == "string" {
		delete(ks.stringMap, key)
	}
	ks.listMap[key] = NewListFromSlice(value)
	newKey := keyspaceEntry{group: "string", expires: nil}

	if exp != nil {
		final := ks.clock.Now().Add(time.Duration(exp.magnitude) * exp.resolution)
		newKey.expires = &final
	}

	ks.keys[key] = newKey
	ks.modifications += 1
}

func (ks *keyspace) SetKey(key string, value interface{}, exp *ExpiryDuration) {
	switch v := value.(type) {
	case string:
		ks.SetStringKey(key, v, exp)
	case []string:
		ks.SetListKey(key, v, exp)
	}
}

func (ks *keyspace) IncrementBy(key string, value int) (int, error) {
	ks.mutex.Lock()
	defer ks.mutex.Unlock()

	ke, ok := ks.keys[key]
	if !ok {
		ks.keys[key] = keyspaceEntry{group: "string", expires: nil}
		ks.stringMap[key] = "0"
		return 0, nil
	}

	if ke.group != "string" {
		return 0, fmt.Errorf("key '%s' does not support this operation", key)
	}

	strVal, ok := ks.stringMap[key]
	if !ok {
		// if this happens, then it means the key is not in the correct keyspace
		// and there is a synchronization bug in the keyspace
		// TODO: good luck fixing this
		return 0, fmt.Errorf("key '%s' not found", key)
	}

	intVal, err := strconv.ParseInt(strVal, 10, 0)
	if err != nil {
		return 0, fmt.Errorf("key '%s' cannot be parsed to integer", key)
	}

	newVal := int(intVal) + value
	ks.stringMap[key] = fmt.Sprintf("%d", newVal)

	ks.modifications += 1
	return newVal, nil
}

func (ks *keyspace) PushToTail(key string, values []string) (int, error) {
	ks.mutex.Lock()
	defer ks.mutex.Unlock()

	ke, ok := ks.keys[key]
	if !ok {
		ks.listMap[key] = NewListFromSlice(values)
		ks.keys[key] = keyspaceEntry{group: "list", expires: nil}
		return len(values), nil
	}

	if ke.group != "list" {
		return 0, fmt.Errorf("key '%s' does not support this operation", key)
	}

	listVal, ok := ks.listMap[key]
	if !ok {
		// if this happens, then it means the key is not in the correct keyspace
		// and there is a synchronization bug in the keyspace
		// TODO: good luck fixing this
		return 0, fmt.Errorf("key '%s' not found", key)
	}

	listVal.AppendSliceToTail(values)

	ks.listMap[key] = listVal
	ks.modifications += 1
	return listVal.size, nil
}

func (ks *keyspace) PushToHead(key string, values []string) (int, error) {
	ks.mutex.Lock()
	defer ks.mutex.Unlock()

	ke, ok := ks.keys[key]
	if !ok {
		ks.listMap[key] = NewListFromSlice(values)
		ks.keys[key] = keyspaceEntry{group: "list", expires: nil}
		return len(values), nil
	}

	if ke.group != "list" {
		return 0, fmt.Errorf("key '%s' does not support this operation", key)
	}

	listVal, ok := ks.listMap[key]
	if !ok {
		// if this happens, then it means the key is not in the correct keyspace
		// and there is a synchronization bug in the keyspace
		// TODO: good luck fixing this
		return 0, fmt.Errorf("key '%s' not found", key)
	}

	listVal.AppendSliceToHead(values)

	ks.listMap[key] = listVal
	ks.modifications += 1
	return listVal.size, nil
}

func (ks *keyspace) PutInSortedSet(key string, values []string) (int, error) {
	ks.mutex.Lock()
	defer ks.mutex.Unlock()

	ke, ok := ks.keys[key]
	if !ok {
		tree := NewTree[float64, string]()
		ks.sortedSetMap[key] = *tree
		ke = keyspaceEntry{group: "sorted-set", expires: nil}
		ks.keys[key] = ke
	}

	if ke.group != "sorted-set" {
		return 0, fmt.Errorf("key '%s' does not support this operation", key)
	}

	setVal, ok := ks.sortedSetMap[key]
	if !ok {
		return 0, fmt.Errorf("key '%s' not found", key)
	}

	added := 0
	for i := 0; i < len(values); i += 2 {
		rawScore := values[i]
		member := values[i+1]
		score, err := strconv.ParseFloat(rawScore, 64)
		if err != nil {
			continue
		}

		setVal.Put(score, member)
		added++
	}

	ks.sortedSetMap[key] = setVal
	ks.modifications += 1
	return added, nil
}

func (ks *keyspace) GetSortedSetValuesByRange(key string, start int64, stop int64) ([]string, error) {
	ks.mutex.RLock()
	defer ks.mutex.RUnlock()

	result := make([]string, 0)
	ke, ok := ks.keys[key]
	if !ok {
		return result, fmt.Errorf("key '%s' does not support this operation", key)
	}

	if ke.group != "sorted-set" {
		return result, fmt.Errorf("key '%s' does not support this operation", key)
	}

	setVal, ok := ks.sortedSetMap[key]
	if !ok {
		return result, fmt.Errorf("key '%s' not found", key)
	}

	if stop < 0 {
		stop = setVal.Size() + stop + 1
	}

	// FIXME: this takes O(N)
	allValues := setVal.GetValueSet()
	values := allValues[start:stop]
	return values, nil
}

func CheckIsExpired(c ClockTimer, ke keyspaceEntry) bool {
	if ke.expires == nil {
		return false
	}

	expires := *ke.expires
	return c.Now().After(expires)
}
