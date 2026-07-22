package main

type set[T comparable] map[T]struct{}

func (values set[T]) add(value T) bool {
	if _, exists := values[value]; exists {
		return false
	}
	values[value] = struct{}{}
	return true
}
