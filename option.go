package drifterdb

type Option struct {
	// MemtableSize is the max bytes size of the memtable.
	// levels is the max level of the lsm-tree.
	// amplificationRatio is the ratio how many times the lower level should larger than the upper level in lsm-tree.
	// synchronousWAL controls whether the WAL is always flushed to the disk synchronously or flushed asynchronously.
	// separateKV is a option to control whether the WiscKey mode is on.
	// noCompaction would make db block all compaction job and improve write performance significantly.
	MemtableSize       int  `json:"memtable_size"`
	Levels             int  `json:"levels"`
	AmplificationRatio int  `json:"amplification_ratio"`
	SynchronousWAL     bool `json:"synchronous_wal"`
	SeparateKV         bool `json:"separate_kv"`
	NoCompaction       bool `json:"no_compaction"`
}

const (
	KB                        = 1 << 10
	MB                        = 1 << 20
	GB                        = 1 << 30
	TB                        = 1 << 40
	DefaultLevels             = 7
	DefaultAmplificationRatio = 1 << 3
)

func DefaultOption() *Option {
	return &Option{
		MemtableSize:       1 * MB,
		Levels:             DefaultLevels,
		AmplificationRatio: DefaultAmplificationRatio,
		SynchronousWAL:     true,
		SeparateKV:         true,
		NoCompaction:       false,
	}
}
