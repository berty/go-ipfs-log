package ipfslog


type GLog interface {
	Append (interface{})
	Merge (GLog)
	Get (interface{}) interface{}
	Has (interface{}) bool
	Values ()
	Length () uint64
}
