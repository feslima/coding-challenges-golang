package redis

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

func TestInsertion(t *testing.T) {
	tree := NewTree[int, int]()
	tree.Put(50, 50)
	tree.Put(25, 25)
	tree.Put(75, 75)
	tree.Put(10, 10)
	tree.Put(33, 33)
	tree.Put(56, 56)
	tree.Put(89, 89)

	wantSize := 7
	gotSize := tree.Size()
	if gotSize != wantSize {
		t.Fatalf("got %d - want %d", gotSize, wantSize)
	}

	want := []int{10, 25, 33, 50, 56, 75, 89}
	got := tree.GetKeySet()

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got keyset %v | want keyset %v", got, want)
	}
}

func TestSearch(t *testing.T) {
	tree := NewTree[int, int]()
	tree.Put(50, 50)
	tree.Put(25, 25)
	tree.Put(75, 75)
	tree.Put(10, 10)
	tree.Put(33, 33)
	tree.Put(56, 56)
	tree.Put(89, 89)

	want := 89
	got := tree.Get(want)

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got key %v | want key %v", got, want)
	}
}

func TestShouldRemoveLeftLeafWithoutChildCorrectly(t *testing.T) {
	tree := NewTree[int, int]()
	tree.Put(50, 50)
	tree.Put(25, 25)
	tree.Put(75, 75)
	tree.Put(10, 10)
	tree.Put(33, 33)
	tree.Put(56, 56)
	tree.Put(89, 89)

	tree.Remove(10)

	wantSize := 6
	gotSize := tree.Size()
	if gotSize != wantSize {
		t.Fatalf("got %d - want %d", gotSize, wantSize)
	}

	want := []int{25, 33, 50, 56, 75, 89}
	got := tree.GetKeySet()

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got keyset %v | want keyset %v", got, want)
	}
}

func TestShouldRemoveNodeWithSingleRightChildCorrectly(t *testing.T) {
	tree := NewTree[int, int]()
	tree.Put(50, 50)
	tree.Put(25, 25)
	tree.Put(75, 75)
	tree.Put(10, 10)
	tree.Put(33, 33)
	tree.Put(56, 56)
	tree.Put(89, 89)

	tree.Remove(10)

	wantSize := 6
	gotSize := tree.Size()
	if gotSize != wantSize {
		t.Fatalf("got %d - want %d", gotSize, wantSize)
	}

	want := []int{25, 33, 50, 56, 75, 89}
	got := tree.GetKeySet()

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got keyset %v | want keyset %v", got, want)
	}

	tree.Remove(25)
	wantSize = 5
	gotSize = tree.Size()
	if gotSize != wantSize {
		t.Fatalf("got %d - want %d", gotSize, wantSize)
	}

	want = []int{33, 50, 56, 75, 89}
	got = tree.GetKeySet()
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got keyset %v | want keyset %v", got, want)
	}
}

func TestShouldRemoveNodeWithTwoChildrenCorrectly(t *testing.T) {
	tree := NewTree[int, int]()
	tree.Put(50, 50)
	tree.Put(25, 25)
	tree.Put(75, 75)
	tree.Put(11, 11)
	tree.Put(33, 33)
	tree.Put(56, 56)
	tree.Put(89, 89)
	tree.Put(30, 30)
	tree.Put(40, 40)
	tree.Put(52, 52)
	tree.Put(61, 61)
	tree.Put(82, 82)
	tree.Put(95, 95)

	tree.Remove(56)

	wantSize := 12
	gotSize := tree.Size()
	if gotSize != wantSize {
		t.Fatalf("got %d - want %d", gotSize, wantSize)
	}

	want := []int{11, 25, 30, 33, 40, 50, 52, 61, 75, 82, 89, 95}
	got := tree.GetKeySet()

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got keyset %v | want keyset %v", got, want)
	}

}

func TestShouldRemoveNodeWithTwoChildrenAndLeftSuccessorNodeCorrectly(t *testing.T) {
	tree := NewTree[int, int]()
	tree.Put(50, 50)
	tree.Put(25, 25)
	tree.Put(75, 75)
	tree.Put(11, 11)
	tree.Put(33, 33)
	tree.Put(61, 61)
	tree.Put(89, 89)
	tree.Put(30, 30)
	tree.Put(40, 40)
	tree.Put(52, 52)
	tree.Put(82, 82)
	tree.Put(95, 95)

	tree.Remove(50)

	wantSize := 11
	gotSize := tree.Size()
	if gotSize != wantSize {
		t.Fatalf("got %d - want %d", gotSize, wantSize)
	}

	want := []int{11, 25, 30, 33, 40, 52, 61, 75, 82, 89, 95}
	got := tree.GetKeySet()

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got keyset %v | want keyset %v", got, want)
	}
}

func TestShouldRemoveNodeWithTwoChildrenAndSuccessorNodeWithRightChildCorrectly(t *testing.T) {
	tree := NewTree[int, int]()
	tree.Put(50, 50)
	tree.Put(25, 25)
	tree.Put(75, 75)
	tree.Put(11, 11)
	tree.Put(33, 33)
	tree.Put(61, 61)
	tree.Put(89, 89)
	tree.Put(30, 30)
	tree.Put(40, 40)
	tree.Put(52, 52)
	tree.Put(82, 82)
	tree.Put(95, 95)
	tree.Put(55, 55)

	tree.Remove(50)

	wantSize := 12
	gotSize := tree.Size()
	if gotSize != wantSize {
		t.Fatalf("got %d - want %d", gotSize, wantSize)
	}

	want := []int{11, 25, 30, 33, 40, 52, 55, 61, 75, 82, 89, 95}
	got := tree.GetKeySet()

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got keyset %v | want keyset %v", got, want)
	}
}

func createRandomSlice(n int) []int {
	elements := make([]int, n)
	for i := 0; i < n; i++ {
		elements[i] = i
	}
	rand.New(rand.NewSource(time.Now().UnixNano()))
	rand.Shuffle(n, func(i, j int) { elements[i], elements[j] = elements[j], elements[i] })

	return elements
}

func BenchmarkInsertion(b *testing.B) {
	for _, v := range []int{10, 100, 1000, 10000, 100000, 1000000, 10000000} {
		b.Run(fmt.Sprintf("insertion with %d elements", v), func(b *testing.B) {
			elements := createRandomSlice(v)

			for i := 0; i < b.N; i++ {
				tree := NewTree[int, int]()

				for _, e := range elements {
					tree.Put(e, e)
				}
			}
		})
	}
}
