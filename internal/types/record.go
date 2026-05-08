package types

// Record wraps the internal DB row representation of a thing.
type Record struct {
	// RowID is the internal database SERIAL/BIGSERIAL for the record.
	RowID int64
	// Generation is the latest known generation for the record.
	Generation int64
}

// RecordWithGeneration describes the DB row representation of a thing that can
// have generations (i.e. it's desired state can be modified over time).
type RecordWithGeneration struct {
	Record
	// Generation is the latest known generation for the record.
	Generation int64
}
