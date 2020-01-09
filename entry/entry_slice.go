package entry

type Slice []*Entry

func (s Slice) Len() int           { return len(s) }
func (s Slice) Less(i, j int) bool { return s[i].GetClock().Compare(s[j].GetClock()) }
func (s Slice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
