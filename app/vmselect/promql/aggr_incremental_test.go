package promql

import (
	"fmt"
	"math"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/app/vmselect/netstorage"
	"github.com/VictoriaMetrics/metricsql"
)

func TestIncrementalAggrMemoryLimits(t *testing.T) {
	newTS := func(pod string) *timeseries {
		ts := &timeseries{Values: make([]float64, 32)}
		ts.MetricName.AddTag("pod", pod)
		return ts
	}
	for _, limit := range []string{"query", "concurrent"} {
		t.Run(limit, func(t *testing.T) {
			expr, err := metricsql.Parse("max by (pod) (x)")
			if err != nil {
				t.Fatal(err)
			}
			iafc := newIncrementalAggrFuncContext(expr.(*metricsql.AggrFuncExpr), getIncrementalAggrFuncCallbacks("max"))
			iafc.memoryLimiter.MaxSize = 3000
			global := &memoryLimiter{MaxSize: 100000}
			if limit == "concurrent" {
				iafc.memoryLimiter.MaxSize, global.MaxSize = global.MaxSize, iafc.memoryLimiter.MaxSize
			}
			iafc.rollupMemoryLimiter = global
			if err := iafc.updateTimeseries(newTS("a"), 0); err != nil {
				t.Fatal(err)
			}
			usage := global.usage
			for range 100 {
				if err := iafc.updateTimeseries(newTS("a"), 0); err != nil {
					t.Fatal(err)
				}
			}
			if global.usage != usage || iafc.memoryLimiter.usage != usage {
				t.Fatal("repeated group consumed additional memory")
			}
			if err := iafc.updateTimeseries(newTS("b"), 0); err == nil {
				t.Fatal("expected memory limit error for a new group")
			}
			if len(iafc.byWorkerID[0].m) != 1 || global.usage != usage || iafc.memoryLimiter.usage != usage {
				t.Fatal("rejected group allocated state or leaked a reservation")
			}
			iafc.resetState()
			if global.usage != 0 || iafc.memoryLimiter.usage != 0 || iafc.byWorkerID[0].m != nil {
				t.Fatal("memory not released after failure")
			}
			if err := iafc.updateTimeseries(newTS("a"), 0); err != nil {
				t.Fatal(err)
			}
			iafc.finalizeTimeseries()
			iafc.resetState()
			if global.usage != 0 || iafc.memoryLimiter.usage != 0 {
				t.Fatal("memory not released after success")
			}
		})
	}

	t.Run("workers", func(t *testing.T) {
		expr, err := metricsql.Parse("max by (pod) (x)")
		if err != nil {
			t.Fatal(err)
		}
		iafc := newIncrementalAggrFuncContext(expr.(*metricsql.AggrFuncExpr), getIncrementalAggrFuncCallbacks("max"))
		iafc.byWorkerID = make([]incrementalAggrContextMap, 4)
		iafc.memoryLimiter.MaxSize = 3000
		iafc.rollupMemoryLimiter = &memoryLimiter{MaxSize: 100000}
		var wg sync.WaitGroup
		errs := make(chan error, 4)
		for workerID := range 4 {
			wg.Go(func() {
				errs <- iafc.updateTimeseries(newTS("a"), uint(workerID))
			})
		}
		wg.Wait()
		close(errs)
		successes := 0
		for err := range errs {
			if err == nil {
				successes++
			}
		}
		if successes != 1 {
			t.Fatalf("worker-local copies must share the query budget; successful allocations: %d; want 1", successes)
		}
		iafc.resetState()
		if iafc.memoryLimiter.usage != 0 || iafc.rollupMemoryLimiter.usage != 0 {
			t.Fatal("memory not released after concurrent failure")
		}
	})

	t.Run("any", func(t *testing.T) {
		expr, err := metricsql.Parse("any by (pod) (x)")
		if err != nil {
			t.Fatal(err)
		}
		iafc := newIncrementalAggrFuncContext(expr.(*metricsql.AggrFuncExpr), getIncrementalAggrFuncCallbacks("any"))
		iafc.memoryLimiter.MaxSize = 3000
		iafc.rollupMemoryLimiter = &memoryLimiter{MaxSize: 100000}
		ts := newTS("a")
		ts.MetricName.AddTag("long_label", strings.Repeat("x", 3000))
		if err := iafc.updateTimeseries(ts, 0); err == nil {
			t.Fatal("expected memory limit error for the original labels retained by any")
		}
		if len(iafc.byWorkerID[0].m) != 0 || iafc.memoryLimiter.usage != 0 || iafc.rollupMemoryLimiter.usage != 0 {
			t.Fatal("rejected group allocated state")
		}
		iafc.resetState()
	})
}

func TestIncrementalAggr(t *testing.T) {
	defaultTimestamps := []int64{100e3, 200e3, 300e3, 400e3}
	values := [][]float64{
		{1, nan, 2, nan},
		{3, nan, nan, 4},
		{nan, nan, 5, 6},
		{7, nan, 8, 9},
		{4, nan, nan, nan},
		{2, nan, 3, 2},
		{0, nan, 1, 1},
	}
	tssSrc := make([]*timeseries, len(values))
	for i, vs := range values {
		ts := &timeseries{
			Timestamps: defaultTimestamps,
			Values:     vs,
		}
		tssSrc[i] = ts
	}

	copyTimeseries := func(tssSrc []*timeseries) []*timeseries {
		tssDst := make([]*timeseries, len(tssSrc))
		for i, tsSrc := range tssSrc {
			var tsDst timeseries
			tsDst.CopyFromShallowTimestamps(tsSrc)
			tssDst[i] = &tsDst
		}
		return tssDst
	}

	f := func(name string, valuesExpected []float64) {
		t.Helper()
		callbacks := getIncrementalAggrFuncCallbacks(name)
		ae := &metricsql.AggrFuncExpr{
			Name: name,
		}
		tssExpected := []*timeseries{{
			Timestamps: defaultTimestamps,
			Values:     valuesExpected,
		}}
		// run the test multiple times to make sure there are no side effects on concurrency
		for i := range 10 {
			iafc := newIncrementalAggrFuncContext(ae, callbacks)
			tssSrcCopy := copyTimeseries(tssSrc)
			if err := testIncrementalParallelAggr(iafc, tssSrcCopy, tssExpected); err != nil {
				t.Fatalf("unexpected error on iteration %d: %s", i, err)
			}
		}
	}

	t.Run("sum", func(t *testing.T) {
		t.Parallel()
		valuesExpected := []float64{17, nan, 19, 22}
		f("sum", valuesExpected)
	})
	t.Run("min", func(t *testing.T) {
		t.Parallel()
		valuesExpected := []float64{0, nan, 1, 1}
		f("min", valuesExpected)
	})
	t.Run("max", func(t *testing.T) {
		t.Parallel()
		valuesExpected := []float64{7, nan, 8, 9}
		f("max", valuesExpected)
	})
	t.Run("avg", func(t *testing.T) {
		t.Parallel()
		valuesExpected := []float64{2.8333333333333335, nan, 3.8, 4.4}
		f("avg", valuesExpected)
	})
	t.Run("count", func(t *testing.T) {
		t.Parallel()
		valuesExpected := []float64{6, nan, 5, 5}
		f("count", valuesExpected)
	})
	t.Run("sum2", func(t *testing.T) {
		t.Parallel()
		valuesExpected := []float64{79, nan, 103, 138}
		f("sum2", valuesExpected)
	})
	t.Run("geomean", func(t *testing.T) {
		t.Parallel()
		valuesExpected := []float64{0, nan, 2.9925557394776896, 3.365865436338599}
		f("geomean", valuesExpected)
	})
}

func testIncrementalParallelAggr(iafc *incrementalAggrFuncContext, tssSrc, tssExpected []*timeseries) error {
	defer iafc.resetState()
	workersCount := netstorage.MaxWorkers()
	tsCh := make(chan *timeseries)
	var wg sync.WaitGroup
	for workerID := range workersCount {
		wg.Go(func() {
			for ts := range tsCh {
				runtime.Gosched() // allow other goroutines performing the work
				if err := iafc.updateTimeseries(ts, uint(workerID)); err != nil {
					panic(err)
				}
			}
		})
	}
	for _, ts := range tssSrc {
		tsCh <- ts
	}
	close(tsCh)
	wg.Wait()
	tssActual := iafc.finalizeTimeseries()
	if err := expectTimeseriesEqual(tssActual, tssExpected); err != nil {
		return fmt.Errorf("%w; tssActual=%v, tssExpected=%v", err, tssActual, tssExpected)
	}
	return nil
}

func expectTimeseriesEqual(actual, expected []*timeseries) error {
	if len(actual) != len(expected) {
		return fmt.Errorf("unexpected number of time series; got %d; want %d", len(actual), len(expected))
	}
	mActual := timeseriesToMap(actual)
	mExpected := timeseriesToMap(expected)
	if len(mActual) != len(mExpected) {
		return fmt.Errorf("unexpected number of time series after converting to map; got %d; want %d", len(mActual), len(mExpected))
	}
	for k, tsExpected := range mExpected {
		tsActual := mActual[k]
		if tsActual == nil {
			return fmt.Errorf("missing time series for key=%q", k)
		}
		if err := expectTsEqual(tsActual, tsExpected); err != nil {
			return err
		}
	}
	return nil
}

func timeseriesToMap(tss []*timeseries) map[string]*timeseries {
	m := make(map[string]*timeseries, len(tss))
	for _, ts := range tss {
		k := ts.MetricName.Marshal(nil)
		m[string(k)] = ts
	}
	return m
}

func expectTsEqual(actual, expected *timeseries) error {
	mnActual := actual.MetricName.Marshal(nil)
	mnExpected := expected.MetricName.Marshal(nil)
	if string(mnActual) != string(mnExpected) {
		return fmt.Errorf("unexpected metric name; got %q; want %q", mnActual, mnExpected)
	}
	if !reflect.DeepEqual(actual.Timestamps, expected.Timestamps) {
		return fmt.Errorf("unexpected timestamps; got %v; want %v", actual.Timestamps, expected.Timestamps)
	}
	if err := compareValues(actual.Values, expected.Values); err != nil {
		return fmt.Errorf("%w; actual %v; expected %v", err, actual.Values, expected.Values)
	}
	return nil
}

func compareValues(vs1, vs2 []float64) error {
	if len(vs1) != len(vs2) {
		return fmt.Errorf("unexpected number of values; got %d; want %d", len(vs1), len(vs2))
	}
	for i, v1 := range vs1 {
		v2 := vs2[i]
		if math.IsNaN(v1) {
			if !math.IsNaN(v2) {
				return fmt.Errorf("unexpected value; got %v; want %v", v1, v2)
			}
			continue
		}
		eps := math.Abs(v1 - v2)
		if eps > 1e-14 {
			return fmt.Errorf("unexpected value; got %v; want %v", v1, v2)
		}
	}
	return nil
}
