package promql

import (
	"fmt"
	"math"
	"strings"
	"unsafe"

	"github.com/VictoriaMetrics/metricsql"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/netstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/atomicutil"
)

// callbacks for optimized incremental calculations for aggregate functions
// over rollups over metricsql.MetricExpr.
//
// These calculations save RAM for aggregates over big number of time series.
var incrementalAggrFuncCallbacksMap = map[string]*incrementalAggrFuncCallbacks{
	"sum": {
		updateAggrFunc:   updateAggrSum,
		mergeAggrFunc:    mergeAggrSum,
		finalizeAggrFunc: finalizeAggrCommon,
	},
	"min": {
		updateAggrFunc:   updateAggrMin,
		mergeAggrFunc:    mergeAggrMin,
		finalizeAggrFunc: finalizeAggrCommon,
	},
	"max": {
		updateAggrFunc:   updateAggrMax,
		mergeAggrFunc:    mergeAggrMax,
		finalizeAggrFunc: finalizeAggrCommon,
	},
	"avg": {
		updateAggrFunc:   updateAggrAvg,
		mergeAggrFunc:    mergeAggrAvg,
		finalizeAggrFunc: finalizeAggrAvg,
	},
	"count": {
		updateAggrFunc:   updateAggrCount,
		mergeAggrFunc:    mergeAggrCount,
		finalizeAggrFunc: finalizeAggrCount,
	},
	"sum2": {
		updateAggrFunc:   updateAggrSum2,
		mergeAggrFunc:    mergeAggrSum2,
		finalizeAggrFunc: finalizeAggrCommon,
	},
	"geomean": {
		updateAggrFunc:   updateAggrGeomean,
		mergeAggrFunc:    mergeAggrGeomean,
		finalizeAggrFunc: finalizeAggrGeomean,
	},
	"any": {
		updateAggrFunc:   updateAggrAny,
		mergeAggrFunc:    mergeAggrAny,
		finalizeAggrFunc: finalizeAggrCommon,

		keepOriginal: true,
	},
	"group": {
		updateAggrFunc:   updateAggrCount,
		mergeAggrFunc:    mergeAggrCount,
		finalizeAggrFunc: finalizeAggrGroup,
	},
}

type incrementalAggrContextMap struct {
	m map[string]*incrementalAggrContext

	// The padding prevents false sharing
	_ [atomicutil.CacheLineSize - unsafe.Sizeof(map[string]*incrementalAggrContext{})%atomicutil.CacheLineSize]byte
}

type incrementalAggrFuncContext struct {
	ae *metricsql.AggrFuncExpr

	byWorkerID []incrementalAggrContextMap

	callbacks *incrementalAggrFuncCallbacks

	// Account for every worker-local group before allocating its state.
	memoryLimiter       memoryLimiter
	rollupMemoryLimiter *memoryLimiter
}

func (iafc *incrementalAggrFuncContext) resetState() {
	byWorkerID := iafc.byWorkerID
	for i := range byWorkerID {
		byWorkerID[i].m = nil
	}
	if iafc.rollupMemoryLimiter != nil {
		iafc.rollupMemoryLimiter.Put(iafc.memoryLimiter.usage)
		iafc.memoryLimiter.Put(iafc.memoryLimiter.usage)
	}
}

func newIncrementalAggrFuncContext(ae *metricsql.AggrFuncExpr, callbacks *incrementalAggrFuncCallbacks) *incrementalAggrFuncContext {
	return &incrementalAggrFuncContext{
		ae:         ae,
		byWorkerID: make([]incrementalAggrContextMap, netstorage.MaxWorkers()),
		callbacks:  callbacks,
	}
}

func (iafc *incrementalAggrFuncContext) updateTimeseries(tsOrig *timeseries, workerID uint) error {
	v := &iafc.byWorkerID[workerID]
	if v.m == nil {
		v.m = make(map[string]*incrementalAggrContext, 1)
	}
	m := v.m

	ts := tsOrig
	keepOriginal := iafc.callbacks.keepOriginal
	if keepOriginal {
		var dst timeseries
		dst.CopyFromMetricNames(tsOrig)
		ts = &dst
	}
	removeGroupTags(&ts.MetricName, &iafc.ae.Modifier)
	bb := bbPool.Get()
	bb.B = marshalMetricNameSorted(bb.B[:0], &ts.MetricName)
	k := bb.B
	iac := m[string(k)]
	if iac == nil {
		if iafc.ae.Limit > 0 && len(m) >= iafc.ae.Limit {
			// Skip this time series, since the limit on the number of output time series has been already reached.
			bbPool.Put(bb)
			return nil
		}
		if keepOriginal {
			ts = tsOrig
		}
		if iafc.rollupMemoryLimiter != nil {
			// Two float64 arrays, the map entry and the copied metric name.
			// Timestamps are shared with the rollup and already accounted for.
			memorySize := uint64(len(ts.Values))*16 + 1000 + uint64(len(k)+len(ts.MetricName.MetricGroup))
			for _, tag := range ts.MetricName.Tags {
				memorySize += uint64(unsafe.Sizeof(tag)) + uint64(len(tag.Key)+len(tag.Value))
			}
			if !iafc.memoryLimiter.Get(memorySize) {
				bbPool.Put(bb)
				return fmt.Errorf("not enough memory for incremental aggregation %s according to -search.maxMemoryPerQuery; memory budget for groups: %d bytes; requested memory for a new group: %d bytes",
					iafc.ae.AppendString(nil), iafc.memoryLimiter.MaxSize, memorySize)
			}
			if !iafc.rollupMemoryLimiter.Get(memorySize) {
				iafc.memoryLimiter.Put(memorySize)
				bbPool.Put(bb)
				return fmt.Errorf("not enough memory for incremental aggregation %s; total available memory for concurrent requests: %d bytes; requested memory for a new group: %d bytes",
					iafc.ae.AppendString(nil), iafc.rollupMemoryLimiter.MaxSize, memorySize)
			}
		}
		tsAggr := &timeseries{
			Values:     make([]float64, len(ts.Values)),
			Timestamps: ts.Timestamps,
			denyReuse:  true,
		}
		tsAggr.MetricName.CopyFrom(&ts.MetricName)
		iac = &incrementalAggrContext{
			ts:     tsAggr,
			values: make([]float64, len(ts.Values)),
		}
		m[string(k)] = iac
	}
	bbPool.Put(bb)
	iafc.callbacks.updateAggrFunc(iac, ts.Values)
	return nil
}

func (iafc *incrementalAggrFuncContext) finalizeTimeseries() []*timeseries {
	mGlobal := make(map[string]*incrementalAggrContext)
	mergeAggrFunc := iafc.callbacks.mergeAggrFunc
	byWorkerID := iafc.byWorkerID
	for i := range byWorkerID {
		for k, iac := range byWorkerID[i].m {
			iacGlobal := mGlobal[k]
			if iacGlobal == nil {
				if iafc.ae.Limit > 0 && len(mGlobal) >= iafc.ae.Limit {
					// Skip this time series, since the limit on the number of output time series has been already reached.
					continue
				}
				mGlobal[k] = iac
				continue
			}
			mergeAggrFunc(iacGlobal, iac)
		}
	}
	tss := make([]*timeseries, 0, len(mGlobal))
	finalizeAggrFunc := iafc.callbacks.finalizeAggrFunc
	for _, iac := range mGlobal {
		finalizeAggrFunc(iac)
		tss = append(tss, iac.ts)
	}
	return tss
}

type incrementalAggrFuncCallbacks struct {
	updateAggrFunc   func(iac *incrementalAggrContext, values []float64)
	mergeAggrFunc    func(dst, src *incrementalAggrContext)
	finalizeAggrFunc func(iac *incrementalAggrContext)

	// Whether to keep the original MetricName for every time series during aggregation
	keepOriginal bool
}

func getIncrementalAggrFuncCallbacks(name string) *incrementalAggrFuncCallbacks {
	name = strings.ToLower(name)
	return incrementalAggrFuncCallbacksMap[name]
}

type incrementalAggrContext struct {
	ts     *timeseries
	values []float64
}

func finalizeAggrCommon(iac *incrementalAggrContext) {
	counts := iac.values
	dstValues := iac.ts.Values
	_ = dstValues[len(counts)-1]
	for i, v := range counts {
		if v == 0 {
			dstValues[i] = nan
		}
	}
}

func updateAggrSum(iac *incrementalAggrContext, values []float64) {
	dstValues := iac.ts.Values
	dstCounts := iac.values
	_ = dstValues[len(values)-1]
	_ = dstCounts[len(values)-1]
	for i, v := range values {
		if math.IsNaN(v) {
			continue
		}
		if dstCounts[i] == 0 {
			dstValues[i] = v
			dstCounts[i] = 1
			continue
		}
		dstValues[i] += v
	}
}

func mergeAggrSum(dst, src *incrementalAggrContext) {
	srcValues := src.ts.Values
	dstValues := dst.ts.Values
	srcCounts := src.values
	dstCounts := dst.values
	_ = srcCounts[len(srcValues)-1]
	_ = dstCounts[len(srcValues)-1]
	_ = dstValues[len(srcValues)-1]
	for i, v := range srcValues {
		if srcCounts[i] == 0 {
			continue
		}
		if dstCounts[i] == 0 {
			dstValues[i] = v
			dstCounts[i] = 1
			continue
		}
		dstValues[i] += v
	}
}

func updateAggrMin(iac *incrementalAggrContext, values []float64) {
	dstValues := iac.ts.Values
	dstCounts := iac.values
	_ = dstValues[len(values)-1]
	_ = dstCounts[len(values)-1]
	for i, v := range values {
		if math.IsNaN(v) {
			continue
		}
		if dstCounts[i] == 0 {
			dstValues[i] = v
			dstCounts[i] = 1
			continue
		}
		if v < dstValues[i] {
			dstValues[i] = v
		}
	}
}

func mergeAggrMin(dst, src *incrementalAggrContext) {
	srcValues := src.ts.Values
	dstValues := dst.ts.Values
	srcCounts := src.values
	dstCounts := dst.values
	_ = srcCounts[len(srcValues)-1]
	_ = dstCounts[len(srcValues)-1]
	_ = dstValues[len(srcValues)-1]
	for i, v := range srcValues {
		if srcCounts[i] == 0 {
			continue
		}
		if dstCounts[i] == 0 {
			dstValues[i] = v
			dstCounts[i] = 1
			continue
		}
		if v < dstValues[i] {
			dstValues[i] = v
		}
	}
}

func updateAggrMax(iac *incrementalAggrContext, values []float64) {
	dstValues := iac.ts.Values
	dstCounts := iac.values
	_ = dstValues[len(values)-1]
	_ = dstCounts[len(values)-1]
	for i, v := range values {
		if math.IsNaN(v) {
			continue
		}
		if dstCounts[i] == 0 {
			dstValues[i] = v
			dstCounts[i] = 1
			continue
		}
		if v > dstValues[i] {
			dstValues[i] = v
		}
	}
}

func mergeAggrMax(dst, src *incrementalAggrContext) {
	srcValues := src.ts.Values
	dstValues := dst.ts.Values
	srcCounts := src.values
	dstCounts := dst.values
	_ = srcCounts[len(srcValues)-1]
	_ = dstCounts[len(srcValues)-1]
	_ = dstValues[len(srcValues)-1]
	for i, v := range srcValues {
		if srcCounts[i] == 0 {
			continue
		}
		if dstCounts[i] == 0 {
			dstValues[i] = v
			dstCounts[i] = 1
			continue
		}
		if v > dstValues[i] {
			dstValues[i] = v
		}
	}
}

func updateAggrAvg(iac *incrementalAggrContext, values []float64) {
	// Do not use `Rapid calculation methods` at https://en.wikipedia.org/wiki/Standard_deviation,
	// since it is slower and has no obvious benefits in increased precision.
	dstValues := iac.ts.Values
	dstCounts := iac.values
	_ = dstValues[len(values)-1]
	_ = dstCounts[len(values)-1]
	for i, v := range values {
		if math.IsNaN(v) {
			continue
		}
		if dstCounts[i] == 0 {
			dstValues[i] = v
			dstCounts[i] = 1
			continue
		}
		dstValues[i] += v
		dstCounts[i]++
	}
}

func mergeAggrAvg(dst, src *incrementalAggrContext) {
	srcValues := src.ts.Values
	dstValues := dst.ts.Values
	srcCounts := src.values
	dstCounts := dst.values
	_ = srcCounts[len(srcValues)-1]
	_ = dstCounts[len(srcValues)-1]
	_ = dstValues[len(srcValues)-1]
	for i, v := range srcValues {
		if srcCounts[i] == 0 {
			continue
		}
		if dstCounts[i] == 0 {
			dstValues[i] = v
			dstCounts[i] = srcCounts[i]
			continue
		}
		dstValues[i] += v
		dstCounts[i] += srcCounts[i]
	}
}

func finalizeAggrAvg(iac *incrementalAggrContext) {
	dstValues := iac.ts.Values
	counts := iac.values
	_ = dstValues[len(counts)-1]
	for i, v := range counts {
		if v == 0 {
			dstValues[i] = nan
			continue
		}
		dstValues[i] /= v
	}
}

func updateAggrCount(iac *incrementalAggrContext, values []float64) {
	dstValues := iac.ts.Values
	_ = dstValues[len(values)-1]
	for i, v := range values {
		if math.IsNaN(v) {
			continue
		}
		dstValues[i]++
	}
}

func mergeAggrCount(dst, src *incrementalAggrContext) {
	srcValues := src.ts.Values
	dstValues := dst.ts.Values
	_ = dstValues[len(srcValues)-1]
	for i, v := range srcValues {
		dstValues[i] += v
	}
}

func finalizeAggrCount(iac *incrementalAggrContext) {
	dstValues := iac.ts.Values
	for i, v := range dstValues {
		if v == 0 {
			dstValues[i] = nan
		}
	}
}

func finalizeAggrGroup(iac *incrementalAggrContext) {
	dstValues := iac.ts.Values
	for i, v := range dstValues {
		if v == 0 {
			dstValues[i] = nan
		} else {
			dstValues[i] = 1
		}
	}
}

func updateAggrSum2(iac *incrementalAggrContext, values []float64) {
	dstValues := iac.ts.Values
	dstCounts := iac.values
	_ = dstValues[len(values)-1]
	_ = dstCounts[len(values)-1]
	for i, v := range values {
		if math.IsNaN(v) {
			continue
		}
		if dstCounts[i] == 0 {
			dstValues[i] = v * v
			dstCounts[i] = 1
			continue
		}
		dstValues[i] += v * v
	}
}

func mergeAggrSum2(dst, src *incrementalAggrContext) {
	srcValues := src.ts.Values
	dstValues := dst.ts.Values
	srcCounts := src.values
	dstCounts := dst.values
	_ = srcCounts[len(srcValues)-1]
	_ = dstCounts[len(srcValues)-1]
	_ = dstValues[len(srcValues)-1]
	for i, v := range srcValues {
		if srcCounts[i] == 0 {
			continue
		}
		if dstCounts[i] == 0 {
			dstValues[i] = v
			dstCounts[i] = 1
			continue
		}
		dstValues[i] += v
	}
}

func updateAggrGeomean(iac *incrementalAggrContext, values []float64) {
	dstValues := iac.ts.Values
	dstCounts := iac.values
	_ = dstValues[len(values)-1]
	_ = dstCounts[len(values)-1]
	for i, v := range values {
		if math.IsNaN(v) {
			continue
		}
		if dstCounts[i] == 0 {
			dstValues[i] = v
			dstCounts[i] = 1
			continue
		}
		dstValues[i] *= v
		dstCounts[i]++
	}
}

func mergeAggrGeomean(dst, src *incrementalAggrContext) {
	srcValues := src.ts.Values
	dstValues := dst.ts.Values
	srcCounts := src.values
	dstCounts := dst.values
	_ = srcCounts[len(srcValues)-1]
	_ = dstCounts[len(srcValues)-1]
	_ = dstValues[len(srcValues)-1]
	for i, v := range srcValues {
		if srcCounts[i] == 0 {
			continue
		}
		if dstCounts[i] == 0 {
			dstValues[i] = v
			dstCounts[i] = srcCounts[i]
			continue
		}
		dstValues[i] *= v
		dstCounts[i] += srcCounts[i]
	}
}

func finalizeAggrGeomean(iac *incrementalAggrContext) {
	dstValues := iac.ts.Values
	counts := iac.values
	_ = dstValues[len(counts)-1]
	for i, v := range counts {
		if v == 0 {
			dstValues[i] = nan
			continue
		}
		dstValues[i] = math.Pow(dstValues[i], 1/v)
	}
}

func updateAggrAny(iac *incrementalAggrContext, values []float64) {
	dstCounts := iac.values
	if dstCounts[0] > 0 {
		return
	}
	for i := range values {
		dstCounts[i] = 1
	}
	iac.ts.Values = append(iac.ts.Values[:0], values...)
}

func mergeAggrAny(dst, src *incrementalAggrContext) {
	srcValues := src.ts.Values
	srcCounts := src.values
	dstCounts := dst.values
	if dstCounts[0] > 0 {
		return
	}
	dstCounts[0] = srcCounts[0]
	dst.ts.Values = append(dst.ts.Values[:0], srcValues...)
}
