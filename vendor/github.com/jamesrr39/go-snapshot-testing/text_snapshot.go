package snapshot

func NewTextSnapshot(text string) *SnapshotType {
	return &SnapshotType{
		DataType: SnapshotDataTypeText,
		Value:    text,
	}
}
