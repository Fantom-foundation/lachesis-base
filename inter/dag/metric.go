package dag

import (
	"fmt"

	"github.com/Fantom-foundation/lachesis-base/inter/idx"
)

type Metric struct {
	Num  idx.Event
	Size uint64
}

func (m Metric) String() string {
	return fmt.Sprintf("{Num=%d,Size=%d}", m.Num, m.Size)
}
