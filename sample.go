package pgsimple

import (
	"fmt"
)

//
type IntRow struct {
	i int32
}

func (ir *IntRow) WriteTo(f *FrameBuffer) {
	var temp FrameBuffer
	junk := "abcdefg"
	m := fmt.Sprintf("%d", ir.i)
	temp.AddInt32(int32(len(m)))
	temp.AddBytes([]byte(m))
	temp.AddInt32(int32(len(junk)))
	temp.AddBytes([]byte(junk))
	extra := temp.Bytes()
	f.AddByte('D')
	f.AddInt32(int32(len(extra) + 4 + 2))
	f.AddInt16(2)
	f.AddBytes(extra)
}
func BuildThowAwayResult() ResultSet {
	r := ResultSet{}
	r.addColumn(RowDescriptionMessage{
		fieldName:    "intfield",
		tableID:      0,
		fieldID:      0,
		typeID:       23, //type INT4OID
		typeLen:      4,
		typeModifier: -1,
		mode:         0,
	})

	r.addColumn(RowDescriptionMessage{
		fieldName:    "charfield",
		tableID:      0,
		fieldID:      0,
		typeID:       18, //type CHAROID
		typeLen:      4,
		typeModifier: -1,
		mode:         0,
	})
	//just playing with the idea of using iterators
	i := int32(0)
	var iterator DataRowIterator
	iterator = func() (row DataRowMaker, next DataRowIterator) {
		if i >= 6 {
			return nil, nil
		}
		i += 2
		return &IntRow{i}, iterator
	}
	r.iterator = iterator
	return r
}
