// Copyright (c) 2015, Emir Pasic. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package redblacktree

import (
	"fmt"
	"strings"
	"testing"
	"unsafe"
)

func TestRedBlackTreeGet(t *testing.T) {
	t.Parallel()
	tree := New()

	if actualValue := tree.Size(); actualValue != 0 {
		t.Errorf("Got %v expected %v", actualValue, 0)
	}

	if actualValue := tree.GetNode([]byte{2}).Size(); actualValue != 0 {
		t.Errorf("Got %v expected %v", actualValue, 0)
	}

	tree.Put([]byte{1}, []byte("x")) // 1->x
	tree.Put([]byte{2}, []byte("b")) // 1->x, 2->b (in order)
	tree.Put([]byte{1}, []byte("a")) // 1->a, 2->b (in order, replacement)
	tree.Put([]byte{3}, []byte("c")) // 1->a, 2->b, 3->c (in order)
	tree.Put([]byte{4}, []byte("d")) // 1->a, 2->b, 3->c, 4->d (in order)
	tree.Put([]byte{5}, []byte("e")) // 1->a, 2->b, 3->c, 4->d, 5->e (in order)
	tree.Put([]byte{6}, []byte("f")) // 1->a, 2->b, 3->c, 4->d, 5->e, 6->f (in order)

	fmt.Println(tree)
	//
	//  RedBlackTree
	//  │           ┌── 6
	//  │       ┌── 5
	//  │   ┌── 4
	//  │   │   └── 3
	//  └── 2
	//       └── 1

	if actualValue := tree.Size(); actualValue != 6 {
		t.Errorf("Got %v expected %v", actualValue, 6)
	}

	if actualValue := tree.GetNode([]byte{4}).Size(); actualValue != 4 {
		t.Errorf("Got %v expected %v", actualValue, 4)
	}

	if actualValue := tree.GetNode([]byte{2}).Size(); actualValue != 6 {
		t.Errorf("Got %v expected %v", actualValue, 6)
	}

	if actualValue := tree.GetNode([]byte{8}).Size(); actualValue != 0 {
		t.Errorf("Got %v expected %v", actualValue, 0)
	}
}

func TestRedBlackTreePut(t *testing.T) {
	t.Parallel()
	tree := New()
	tree.Put([]byte{5}, []byte("e"))
	tree.Put([]byte{6}, []byte("f"))
	tree.Put([]byte{7}, []byte("g"))
	tree.Put([]byte{3}, []byte("c"))
	tree.Put([]byte{4}, []byte("d"))
	tree.Put([]byte{1}, []byte("x"))
	tree.Put([]byte{2}, []byte("b"))
	tree.Put([]byte{1}, []byte("a")) //overwrite

	if actualValue := tree.Size(); actualValue != 7 {
		t.Errorf("Got %v expected %v", actualValue, 7)
	}

	tests1 := [][]interface{}{
		{[]byte{1}, "a", true},
		{[]byte{2}, "b", true},
		{[]byte{3}, "c", true},
		{[]byte{4}, "d", true},
		{[]byte{5}, "e", true},
		{[]byte{6}, "f", true},
		{[]byte{7}, "g", true},
		{[]byte{8}, "", false},
	}

	for _, test := range tests1 {
		// retrievals
		actualValue, actualFound := tree.Get(test[0].([]byte))
		if string(actualValue) != test[1] || actualFound != test[2] {
			t.Errorf("Got %v expected %v", actualValue, test[1])
		}
	}
	// do more check nil for not found key
	actualValue, actualFound := tree.Get([]byte{8})
	if actualFound || actualValue != nil {
		t.Errorf("Got %v expected %v", actualValue, nil)
	}
}

func TestRedBlackTreeCeilingAndFloor(t *testing.T) {
	t.Parallel()
	tree := New()

	if node, found := tree.Floor([]byte{0}); node != nil || found {
		t.Errorf("Got %v expected %v", node, "<nil>")
	}
	if node, found := tree.Ceiling([]byte{0}); node != nil || found {
		t.Errorf("Got %v expected %v", node, "<nil>")
	}

	tree.Put([]byte{5}, []byte("e"))
	tree.Put([]byte{6}, []byte("f"))
	tree.Put([]byte{7}, []byte("g"))
	tree.Put([]byte{3}, []byte("c"))
	tree.Put([]byte{4}, []byte("d"))
	tree.Put([]byte{1}, []byte("x"))
	tree.Put([]byte{2}, []byte("b"))

	if node, found := tree.Floor([]byte{4}); int(node.Key[0]) != 4 || !found {
		t.Errorf("Got %v expected %v", int(node.Key[0]), 4)
	}
	if node, found := tree.Floor([]byte{0}); node != nil || found {
		t.Errorf("Got %v expected %v", node, "<nil>")
	}

	if node, found := tree.Ceiling([]byte{4}); int(node.Key[0]) != 4 || !found {
		t.Errorf("Got %v expected %v", int(node.Key[0]), 4)
	}
	if node, found := tree.Ceiling([]byte{8}); node != nil || found {
		t.Errorf("Got %v expected %v", node, "<nil>")
	}
}

func TestRedBlackTreeIteratorNextOnEmpty(t *testing.T) {
	t.Parallel()
	tree := New()
	it := tree.Iterator()
	for it.Next() {
		t.Errorf("Shouldn't iterate on empty tree")
	}
}

func TestRedBlackTreeIteratorPrevOnEmpty(t *testing.T) {
	t.Parallel()
	tree := New()
	it := tree.Iterator()
	for it.Prev() {
		t.Errorf("Shouldn't iterate on empty tree")
	}
}

func TestRedBlackTreeIterator1Next(t *testing.T) {
	t.Parallel()
	tree := New()
	tree.Put([]byte{5}, []byte("e"))
	tree.Put([]byte{6}, []byte("f"))
	tree.Put([]byte{7}, []byte("g"))
	tree.Put([]byte{3}, []byte("c"))
	tree.Put([]byte{4}, []byte("d"))
	tree.Put([]byte{1}, []byte("x"))
	tree.Put([]byte{2}, []byte("b"))
	tree.Put([]byte{1}, []byte("a")) //overwrite

	fmt.Println(tree)
	// │   ┌── 7
	// └── 6
	//     │   ┌── 5
	//     └── 4
	//         │   ┌── 3
	//         └── 2
	//             └── 1
	it := tree.Iterator()
	count := 0
	for it.Next() {
		count++
		key := it.Key()
		if actualValue, expectedValue := key, count; int(actualValue[0]) != expectedValue {
			t.Errorf("Got %v expected %v", actualValue, expectedValue)
		}
	}
	if actualValue, expectedValue := count, tree.Size(); actualValue != expectedValue {
		t.Errorf("Size different. Got %v expected %v", actualValue, expectedValue)
	}
}

func TestRedBlackTreeIterator1Prev(t *testing.T) {
	t.Parallel()
	tree := New()
	tree.Put([]byte{5}, []byte("e"))
	tree.Put([]byte{6}, []byte("f"))
	tree.Put([]byte{7}, []byte("g"))
	tree.Put([]byte{3}, []byte("c"))
	tree.Put([]byte{4}, []byte("d"))
	tree.Put([]byte{1}, []byte("x"))
	tree.Put([]byte{2}, []byte("b"))
	tree.Put([]byte{1}, []byte("a")) //overwrite

	fmt.Println(tree)
	// │   ┌── 7
	// └── 6
	//     │   ┌── 5
	//     └── 4
	//         │   ┌── 3
	//         └── 2
	//             └── 1
	it := tree.Iterator()
	for it.Next() {
	}
	countDown := tree.size
	for it.Prev() {
		key := it.Key()
		if actualValue, expectedValue := key, countDown; int(actualValue[0]) != expectedValue {
			t.Errorf("Got %v expected %v", actualValue, expectedValue)
		}
		countDown--
	}
	if actualValue, expectedValue := countDown, 0; actualValue != expectedValue {
		t.Errorf("Size different. Got %v expected %v", actualValue, expectedValue)
	}
}

func TestRedBlackTreeIterator4Next(t *testing.T) {
	t.Parallel()
	tree := New()
	tree.Put([]byte{13}, []byte{5})
	tree.Put([]byte{8}, []byte{3})
	tree.Put([]byte{17}, []byte{7})
	tree.Put([]byte{1}, []byte{1})
	tree.Put([]byte{11}, []byte{4})
	tree.Put([]byte{15}, []byte{6})
	tree.Put([]byte{25}, []byte{9})
	tree.Put([]byte{6}, []byte{2})
	tree.Put([]byte{22}, []byte{8})
	tree.Put([]byte{27}, []byte{10})

	fmt.Println(tree)
	// │           ┌── 27
	// │       ┌── 25
	// │       │   └── 22
	// │   ┌── 17
	// │   │   └── 15
	// └── 13
	//     │   ┌── 11
	//     └── 8
	//         │   ┌── 6
	//         └── 1
	it := tree.Iterator()
	count := 0
	for it.Next() {
		count++
		value := it.Value()
		if actualValue, expectedValue := value, count; int(actualValue[0]) != expectedValue {
			t.Errorf("Got %v expected %v", actualValue, expectedValue)
		}
	}
	if actualValue, expectedValue := count, tree.Size(); actualValue != expectedValue {
		t.Errorf("Size different. Got %v expected %v", actualValue, expectedValue)
	}
}

func TestRedBlackTreeIterator4Prev(t *testing.T) {
	t.Parallel()
	tree := New()
	tree.Put([]byte{13}, []byte{5})
	tree.Put([]byte{8}, []byte{3})
	tree.Put([]byte{17}, []byte{7})
	tree.Put([]byte{1}, []byte{1})
	tree.Put([]byte{11}, []byte{4})
	tree.Put([]byte{15}, []byte{6})
	tree.Put([]byte{25}, []byte{9})
	tree.Put([]byte{6}, []byte{2})
	tree.Put([]byte{22}, []byte{8})
	tree.Put([]byte{27}, []byte{10})

	fmt.Println(tree)
	// │           ┌── 27
	// │       ┌── 25
	// │       │   └── 22
	// │   ┌── 17
	// │   │   └── 15
	// └── 13
	//     │   ┌── 11
	//     └── 8
	//         │   ┌── 6
	//         └── 1
	it := tree.Iterator()
	count := tree.Size()
	for it.Next() {
	}
	for it.Prev() {
		value := it.Value()
		if actualValue, expectedValue := value, count; int(actualValue[0]) != expectedValue {
			t.Errorf("Got %v expected %v", actualValue, expectedValue)
		}
		count--
	}
	if actualValue, expectedValue := count, 0; actualValue != expectedValue {
		t.Errorf("Size different. Got %v expected %v", actualValue, expectedValue)
	}
}

func TestRedBlackTreeIteratorBegin(t *testing.T) {
	t.Parallel()
	tree := New()
	tree.Put([]byte{3}, []byte("c"))
	tree.Put([]byte{1}, []byte("a"))
	tree.Put([]byte{2}, []byte("b"))
	it := tree.Iterator()

	if it.node != nil {
		t.Errorf("Got %v expected %v", it.node, nil)
	}

	it.Begin()

	if it.node != nil {
		t.Errorf("Got %v expected %v", it.node, nil)
	}

	for it.Next() {
	}

	it.Begin()

	if it.node != nil {
		t.Errorf("Got %v expected %v", it.node, nil)
	}

	it.Next()
	if key, value := it.Key(), it.Value(); int(key[0]) != 1 || string(value) != "a" {
		t.Errorf("Got %v,%v expected %v,%v", key, value, 1, "a")
	}
}

func TestRedBlackTreeIteratorEnd(t *testing.T) {
	t.Parallel()
	tree := New()
	it := tree.Iterator()

	if it.node != nil {
		t.Errorf("Got %v expected %v", it.node, nil)
	}

	it.End()
	if it.node != nil {
		t.Errorf("Got %v expected %v", it.node, nil)
	}

	tree.Put([]byte{3}, []byte("c"))
	tree.Put([]byte{1}, []byte("a"))
	tree.Put([]byte{2}, []byte("b"))
	it.End()
	if it.node != nil {
		t.Errorf("Got %v expected %v", it.node, nil)
	}

	it.Prev()
	if key, value := it.Key(), it.Value(); int(key[0]) != 3 || string(value) != "c" {
		t.Errorf("Got %v,%v expected %v,%v", key, value, 3, "c")
	}
}

func TestRedBlackTreeIteratorFirst(t *testing.T) {
	t.Parallel()
	tree := New()
	tree.Put([]byte{3}, []byte("c"))
	tree.Put([]byte{1}, []byte("a"))
	tree.Put([]byte{2}, []byte("b"))
	it := tree.Iterator()
	if actualValue, expectedValue := it.First(), true; actualValue != expectedValue {
		t.Errorf("Got %v expected %v", actualValue, expectedValue)
	}
	if key, value := it.Key(), it.Value(); int(key[0]) != 1 || string(value) != "a" {
		t.Errorf("Got %v,%v expected %v,%v", key, value, 1, "a")
	}
}

func TestRedBlackTreeIteratorLast(t *testing.T) {
	t.Parallel()
	tree := New()
	tree.Put([]byte{3}, []byte("c"))
	tree.Put([]byte{1}, []byte("a"))
	tree.Put([]byte{2}, []byte("b"))
	it := tree.Iterator()
	if actualValue, expectedValue := it.Last(), true; actualValue != expectedValue {
		t.Errorf("Got %v expected %v", actualValue, expectedValue)
	}
	if key, value := it.Key(), it.Value(); int(key[0]) != 3 || string(value) != "c" {
		t.Errorf("Got %v,%v expected %v,%v", key, value, 3, "c")
	}
}

func TestRedBlackTreeIteratorNextTo(t *testing.T) {
	t.Parallel()
	// Sample seek function, i.e. string starting with "b"
	seek := func(index []byte, value []byte) bool {
		return strings.HasSuffix(string(value), "b")
	}

	// NextTo (empty)
	{
		tree := New()
		it := tree.Iterator()
		for it.NextTo(seek) {
			t.Errorf("Shouldn't iterate on empty tree")
		}
	}

	// NextTo (not found)
	{
		tree := New()
		tree.Put([]byte{0}, []byte("xx"))
		tree.Put([]byte{1}, []byte("yy"))
		it := tree.Iterator()
		for it.NextTo(seek) {
			t.Errorf("Shouldn't iterate on empty tree")
		}
	}

	// NextTo (found)
	{
		tree := New()
		tree.Put([]byte{2}, []byte("cc"))
		tree.Put([]byte{0}, []byte("aa"))
		tree.Put([]byte{1}, []byte("bb"))
		it := tree.Iterator()
		it.Begin()
		if !it.NextTo(seek) {
			t.Errorf("Shouldn't iterate on empty tree")
		}
		if index, value := it.Key(), it.Value(); int(index[0]) != 1 || string(value) != "bb" {
			t.Errorf("Got %v,%v expected %v,%v", index, value, 1, "bb")
		}
		if !it.Next() {
			t.Errorf("Should go to first element")
		}
		if index, value := it.Key(), it.Value(); int(index[0]) != 2 || string(value) != "cc" {
			t.Errorf("Got %v,%v expected %v,%v", index, value, 2, "cc")
		}
		if it.Next() {
			t.Errorf("Should not go past last element")
		}
	}
}

func TestRedBlackTreeIteratorPrevTo(t *testing.T) {
	t.Parallel()
	// Sample seek function, i.e. string starting with "b"
	seek := func(index []byte, value []byte) bool {
		return strings.HasSuffix(string(value), "b")
	}

	// PrevTo (empty)
	{
		tree := New()
		it := tree.Iterator()
		it.End()
		for it.PrevTo(seek) {
			t.Errorf("Shouldn't iterate on empty tree")
		}
	}

	// PrevTo (not found)
	{
		tree := New()
		tree.Put([]byte{0}, []byte("xx"))
		tree.Put([]byte{1}, []byte("yy"))
		it := tree.Iterator()
		it.End()
		for it.PrevTo(seek) {
			t.Errorf("Shouldn't iterate on empty tree")
		}
	}

	// PrevTo (found)
	{
		tree := New()
		tree.Put([]byte{2}, []byte("cc"))
		tree.Put([]byte{0}, []byte("aa"))
		tree.Put([]byte{1}, []byte("bb"))
		it := tree.Iterator()
		it.End()
		if !it.PrevTo(seek) {
			t.Errorf("Shouldn't iterate on empty tree")
		}
		if index, value := it.Key(), it.Value(); int(index[0]) != 1 || string(value) != "bb" {
			t.Errorf("Got %v,%v expected %v,%v", index, value, 1, "bb")
		}
		if !it.Prev() {
			t.Errorf("Should go to first element")
		}
		if index, value := it.Key(), it.Value(); int(index[0]) != 0 || string(value) != "aa" {
			t.Errorf("Got %v,%v expected %v,%v", index, value, 0, "aa")
		}
		if it.Prev() {
			t.Errorf("Should not go before first element")
		}
	}
}

func BenchmarkRedBlackTreeGet100(b *testing.B) {
	b.StopTimer()
	size := 100
	tree := New()
	for n := 0; n < size; n++ {
		tree.Put(IntToByteArray(n), IntToByteArray(n))
	}
	b.StartTimer()
	benchmarkGet(b, tree, size)
}

func BenchmarkRedBlackTreeGet1000(b *testing.B) {
	b.StopTimer()
	size := 1000
	tree := New()
	for n := 0; n < size; n++ {
		tree.Put(IntToByteArray(n), IntToByteArray(n))
	}
	b.StartTimer()
	benchmarkGet(b, tree, size)
}

func BenchmarkRedBlackTreeGet10000(b *testing.B) {
	b.StopTimer()
	size := 10000
	tree := New()
	for n := 0; n < size; n++ {
		tree.Put(IntToByteArray(n), IntToByteArray(n))
	}
	b.StartTimer()
	benchmarkGet(b, tree, size)
}

func BenchmarkRedBlackTreeGet100000(b *testing.B) {
	b.StopTimer()
	size := 100000
	tree := New()
	for n := 0; n < size; n++ {
		tree.Put(IntToByteArray(n), IntToByteArray(n))
	}
	b.StartTimer()
	benchmarkGet(b, tree, size)
}

func BenchmarkRedBlackTreePut100(b *testing.B) {
	b.StopTimer()
	size := 100
	tree := New()
	b.StartTimer()
	benchmarkPut(b, tree, size)
}

func BenchmarkRedBlackTreePut1000(b *testing.B) {
	b.StopTimer()
	size := 1000
	tree := New()
	b.StartTimer()
	benchmarkPut(b, tree, size)
}

func BenchmarkRedBlackTreePut10000(b *testing.B) {
	b.StopTimer()
	size := 10000
	tree := New()
	b.StartTimer()
	benchmarkPut(b, tree, size)
}

func BenchmarkRedBlackTreePut100000(b *testing.B) {
	b.StopTimer()
	size := 100000
	tree := New()
	b.StartTimer()
	benchmarkPut(b, tree, size)
}

func BenchmarkRedBlackTreeRemove100(b *testing.B) {
	b.StopTimer()
	size := 100
	tree := New()
	for n := 0; n < size; n++ {
		tree.Put(IntToByteArray(n), IntToByteArray(n))
	}
	b.StartTimer()
	benchmarkRemove(b, tree, size)
}

func BenchmarkRedBlackTreeRemove1000(b *testing.B) {
	b.StopTimer()
	size := 1000
	tree := New()
	for n := 0; n < size; n++ {
		tree.Put(IntToByteArray(n), IntToByteArray(n))
	}
	b.StartTimer()
	benchmarkRemove(b, tree, size)
}

func BenchmarkRedBlackTreeRemove10000(b *testing.B) {
	b.StopTimer()
	size := 10000
	tree := New()
	for n := 0; n < size; n++ {
		tree.Put(IntToByteArray(n), IntToByteArray(n))
	}
	b.StartTimer()
	benchmarkRemove(b, tree, size)
}

func BenchmarkRedBlackTreeRemove100000(b *testing.B) {
	b.StopTimer()
	size := 100000
	tree := New()
	for n := 0; n < size; n++ {
		tree.Put(IntToByteArray(n), IntToByteArray(n))
	}
	b.StartTimer()
	benchmarkRemove(b, tree, size)
}

func benchmarkGet(b *testing.B, tree *Tree, size int) {
	for i := 0; i < b.N; i++ {
		for n := 0; n < size; n++ {
			tree.Get(IntToByteArray(n))
		}
	}
}

func benchmarkPut(b *testing.B, tree *Tree, size int) {
	for i := 0; i < b.N; i++ {
		for n := 0; n < size; n++ {
			tree.Put(IntToByteArray(n), IntToByteArray(n))
		}
	}
}

func benchmarkRemove(b *testing.B, tree *Tree, size int) {
	for i := 0; i < b.N; i++ {
		for n := 0; n < size; n++ {
			tree.Remove(IntToByteArray(n))
		}
	}
}

func IntToByteArray(num int) []byte {
	size := int(unsafe.Sizeof(num))
	arr := make([]byte, size)
	for i := 0; i < size; i++ {
		byt := *(*uint8)(unsafe.Pointer(uintptr(unsafe.Pointer(&num)) + uintptr(i)))
		arr[size-i-1] = byt
	}
	return arr
}
