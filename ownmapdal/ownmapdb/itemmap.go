package ownmapdb

type ItemMap[T any] struct {
	itemMap                     map[int64]T
	itemNotInDataset, wantedIDs map[int64]struct{}
}

func NewItemMap[T any]() ItemMap[T] {
	return ItemMap[T]{
		itemMap:          make(map[int64]T),
		itemNotInDataset: map[int64]struct{}{},
		wantedIDs:        map[int64]struct{}{},
	}
}

func (m ItemMap[T]) ShouldScanForItem(key int64) bool {
	_, ok := m.itemNotInDataset[key]
	if ok {
		return false
	}

	_, ok = m.wantedIDs[key]
	if ok {
		return true
	}

	return false
}

func (m ItemMap[T]) AddItem(key int64, value T) {
	m.itemMap[key] = value

	delete(m.wantedIDs, key)
}

func (m ItemMap[T]) MarkItemAsNotInDataset(key int64) {
	m.itemNotInDataset[key] = struct{}{}

	delete(m.wantedIDs, key)
}

func (m ItemMap[T]) MarkItemForScanning(key int64) {
	m.wantedIDs[key] = struct{}{}

	delete(m.wantedIDs, key)
}
