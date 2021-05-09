package drifterdb

type StatisticsCounter struct {
	memComp       int // The cumulative number of memory compaction
	level0Comp    int // The cumulative number of level0 compaction
	nonLevel0Comp int // The cumulative number of non-level0 compaction
	seekComp      int // The cumulative number of seek compaction
}
