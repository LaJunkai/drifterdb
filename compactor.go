package drifterdb

type Compactor interface {
	Compact(s *Storage, level int) *Version
	ChooseTable(s *Storage, level int) (tablesToDelete []*Table)
}

type BaseCompactor struct {}

func (b BaseCompactor) Compact(s *Storage, level int) *Version {
	return nil
}

func (b BaseCompactor) ChooseTable(s *Storage, level int) (tablesToDelete []*Table) {
	panic("implement me")
}
