package redis

import (
	"reflect"
	"testing"
)

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
