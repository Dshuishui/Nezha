package lattices

/*
	Lattice结构，kvs间传输的数据结构
*/

import (
	config "github.com/JasonLou99/Hybrid_KV_Store/config"
	"github.com/JasonLou99/Hybrid_KV_Store/util"
)

// Lattice接口，用来实现Merge方法的多态
type Lattice interface {

	// 返回lattice结构包裹的原始数据
	Reveal() config.Log

	// Lattice的Merge操作
	Merge(other Lattice)
}

// 基于VectorClock实现的ValueLattice
type ValueLattice struct {
	Log config.Log
	// Vector Clock
	VectorClock map[string]int32
}

type HybridLattice struct {
	Key string
	Vl  ValueLattice
}

func (vl ValueLattice) Reveal() config.Log {
	return vl.Log
}

func (vl *ValueLattice) Merge(other ValueLattice) {
	// 根据vectorClock决定合并策略
	if util.IsUpper(util.BecomeSyncMap(other.VectorClock), util.BecomeSyncMap(vl.VectorClock)) {
		// other >= vl
		// vl.value = other.value
		vl.VectorClock = other.VectorClock
	}
}

func (ml HybridLattice) Reveal() config.Log {
	return ml.Vl.Log
}

func (ml *HybridLattice) Merge(other HybridLattice) {
	if util.IsUpper(util.BecomeSyncMap(other.Vl.VectorClock), util.BecomeSyncMap(ml.Vl.VectorClock)) {
		// other >= vl
		// ml.vl.value = other.vl.value
		ml.Vl.VectorClock = other.Vl.VectorClock
	}
}
