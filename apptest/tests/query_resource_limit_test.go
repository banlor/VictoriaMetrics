package tests

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"

	"github.com/VictoriaMetrics/VictoriaMetrics/apptest"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/fs"
)

func TestSingleGroupedAggregationMaxMemoryPerQuery(t *testing.T) {
	// Make the old fixed estimate of 1,000 groups deterministic.
	t.Setenv("GOMAXPROCS", "1")
	cli := apptest.NewClient()
	defer cli.CloseConnections()
	dir := t.TempDir()

	const seriesCount = 2500
	const pointsCount = 61
	const start = 1652169600
	samples := make([]string, 0, seriesCount*pointsCount)
	for pod := range seriesCount {
		for point := range pointsCount {
			samples = append(samples, fmt.Sprintf(`grouped_memory{pod="%d",resource="memory"} 1 %d`, pod, (start+point*60)*1000))
		}
	}
	opts := apptest.QueryOpts{
		Start:   fmt.Sprint(start),
		End:     fmt.Sprint(start + (pointsCount-1)*60),
		Step:    "60s",
		NoCache: "1",
	}
	query := `sum(max by (pod) (grouped_memory{resource="memory"}))`
	for _, budget := range []string{"3MiB", "16MiB"} {
		t.Run(budget, func(t *testing.T) {
			vm, err := apptest.StartVmsingle("vmsingle-"+budget, []string{
				"-storageDataPath=" + dir + "/" + budget,
				"-retentionPeriod=100y",
				"-search.maxMemoryPerQuery=" + budget,
			}, cli, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer vm.Stop()
			vm.PrometheusAPIV1ImportPrometheus(t, samples, apptest.QueryOpts{})
			vm.ForceFlush(t)
			res := vm.PrometheusAPIV1QueryRange(t, query, opts)
			if budget == "3MiB" {
				if res.ErrorType != "422" || !strings.Contains(res.Error, "-search.maxMemoryPerQuery") {
					t.Fatalf("expected memory limit error; got status=%q, errorType=%q, error=%q", res.Status, res.ErrorType, res.Error)
				}
				t.Logf("grouped aggregation rejected: %s", res.Error)
			} else {
				if res.Status != "success" || len(res.Data.Result) != 1 || len(res.Data.Result[0].Samples) != pointsCount {
					t.Fatalf("unexpected grouped aggregation result: %+v", res)
				}
				for _, sample := range res.Data.Result[0].Samples {
					if sample.Value != seriesCount {
						t.Fatalf("unexpected sum: got %g; want %d", sample.Value, seriesCount)
					}
				}
			}
			for _, query := range []string{
				"sum(max by (resource) (grouped_memory))",
				"sum(max without (pod) (grouped_memory))",
				"sum(max by (pod) (grouped_memory) limit 1)",
			} {
				res = vm.PrometheusAPIV1QueryRange(t, query, opts)
				if res.Status != "success" || len(res.Data.Result) != 1 || len(res.Data.Result[0].Samples) != pointsCount {
					t.Fatalf("unexpected result for %s: %+v", query, res)
				}
				for _, sample := range res.Data.Result[0].Samples {
					if sample.Value != 1 {
						t.Fatalf("unexpected value for %s: got %g; want 1", query, sample.Value)
					}
				}
			}
			// The input cardinality alone must not reject an aggregation with few groups.
			// Run repeatedly after a rejected query to catch leaked reservations.
			for range 3 {
				res = vm.PrometheusAPIV1QueryRange(t, "sum(grouped_memory)", opts)
				if res.Status != "success" || len(res.Data.Result) != 1 || len(res.Data.Result[0].Samples) != pointsCount {
					t.Fatalf("unexpected ungrouped aggregation result: %+v", res)
				}
				for _, sample := range res.Data.Result[0].Samples {
					if sample.Value != seriesCount {
						t.Fatalf("unexpected sum: got %g; want %d", sample.Value, seriesCount)
					}
				}
			}
		})
	}
}

func TestSingleGroupedAggregationLogQueryMemoryUsage(t *testing.T) {
	// Keep the initial rollup estimate below the dynamic-state logging threshold.
	t.Setenv("GOMAXPROCS", "1")
	cli := apptest.NewClient()
	defer cli.CloseConnections()
	dir := t.TempDir()

	const seriesCount = 100
	const pointsCount = 61
	const start = 1652169600
	samples := make([]string, 0, seriesCount*pointsCount)
	for pod := range seriesCount {
		for point := range pointsCount {
			samples = append(samples, fmt.Sprintf(`logged_groups{pod="%d"} 1 %d`, pod, (start+point*60)*1000))
		}
	}
	opts := apptest.QueryOpts{
		Start:   fmt.Sprint(start),
		End:     fmt.Sprint(start + (pointsCount-1)*60),
		Step:    "60s",
		NoCache: "1",
	}
	for _, tt := range []struct {
		name         string
		logThreshold string
		memoryLimit  string
		wantError    bool
		wantLogged   float64
	}{
		{name: "dynamic_state", logThreshold: "64KiB", memoryLimit: "1MiB", wantLogged: 1},
		{name: "dynamic_state_error", logThreshold: "64KiB", memoryLimit: "128KiB", wantError: true, wantLogged: 1},
		{name: "initial_and_dynamic_state", logThreshold: "1KiB", memoryLimit: "1MiB", wantLogged: 1},
		{name: "initial_estimate_error", logThreshold: "1KiB", memoryLimit: "1KiB", wantError: true, wantLogged: 1},
		{name: "below_threshold", logThreshold: "1MiB", memoryLimit: "1MiB"},
		{name: "disabled", logThreshold: "0", memoryLimit: "1MiB"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			vm, err := apptest.StartVmsingle("vmsingle-"+tt.name, []string{
				"-storageDataPath=" + dir + "/" + tt.name,
				"-retentionPeriod=100y",
				"-search.logQueryMemoryUsage=" + tt.logThreshold,
				"-search.maxMemoryPerQuery=" + tt.memoryLimit,
			}, cli, nil)
			if err != nil {
				t.Fatal(err)
			}
			defer vm.Stop()
			vm.PrometheusAPIV1ImportPrometheus(t, samples, apptest.QueryOpts{})
			vm.ForceFlush(t)
			before := vm.GetMetric(t, "vm_memory_intensive_queries_total")
			res := vm.PrometheusAPIV1QueryRange(t, "max by (pod) (logged_groups)", opts)
			if tt.wantError {
				if res.ErrorType != "422" || !strings.Contains(res.Error, "-search.maxMemoryPerQuery") {
					t.Fatalf("expected memory limit error; got %+v", res)
				}
			} else if res.Status != "success" || len(res.Data.Result) != seriesCount || len(res.Data.Result[0].Samples) != pointsCount {
				t.Fatalf("unexpected grouped aggregation result: %+v", res)
			}
			// /metrics caches its response for one second, including the scrape above.
			time.Sleep(1100 * time.Millisecond)
			if got := vm.GetMetric(t, "vm_memory_intensive_queries_total") - before; got != tt.wantLogged {
				t.Fatalf("unexpected memory-intensive query count: got %g; want %g", got, tt.wantLogged)
			}
		})
	}
}

func TestClusterMaxUniqueTimeseries(t *testing.T) {
	fs.MustRemoveDir(t.Name())

	cmpOpt := cmpopts.IgnoreFields(apptest.PrometheusAPIV1QueryResponse{}, "Status", "Data.ResultType")

	tc := apptest.NewTestCase(t)
	defer tc.Stop()
	vmstorage := tc.MustStartVmstorage("vmstorage", []string{
		"-storageDataPath=" + tc.Dir() + "/vmstorage",
		"-retentionPeriod=100y",
		"-search.maxUniqueTimeseries=2",
	})
	vminsert := tc.MustStartVminsert("vminsert", []string{
		"-storageNode=" + vmstorage.VminsertAddr(),
	})
	vmselectNoLimit := tc.MustStartVmselect("vmselect1", []string{
		"-storageNode=" + vmstorage.VmselectAddr(),
		"-search.tenantCacheExpireDuration=0",
	})
	vmselectSmallLimit := tc.MustStartVmselect("vmselect2", []string{
		"-storageNode=" + vmstorage.VmselectAddr(),
		"-search.tenantCacheExpireDuration=0",
		"-search.maxUniqueTimeseries=1",
	})
	vmselectBigLimit := tc.MustStartVmselect("vmselect3", []string{
		"-storageNode=" + vmstorage.VmselectAddr(),
		"-search.tenantCacheExpireDuration=0",
		"-search.maxUniqueTimeseries=3",
	})

	var commonSamples = []string{
		`foo_bar1{instance="a"} 1.00 1652169660000`,

		`foo_bar2{instance="a"} 1.00 1652169660000`,
		`foo_bar2{instance="b"} 2.00 1652169660000`,

		`foo_bar3{instance="a"} 1.00 1652169660000`,
		`foo_bar3{instance="b"} 2.00 1652169660000`,
		`foo_bar3{instance="c"} 3.00 1652169660000`,
	}

	// write data to two tenants
	tenantIDs := []string{"0:0", "1:15"}
	for _, tenantID := range tenantIDs {
		vminsert.PrometheusAPIV1ImportPrometheus(t, commonSamples, apptest.QueryOpts{Tenant: tenantID})
		vmstorage.ForceFlush(t)
	}

	instantCT := "2022-05-10T08:05:00.000Z"

	// success - `/api/v1/query`
	want := apptest.NewPrometheusAPIV1QueryResponse(t,
		`{"data":
       {"result":[
          {"metric":{"__name__":"foo_bar1","instance":"a"},"value":[1652169900,"1"]}
        ]
       }
     }`,
	)
	queryRes := vmselectSmallLimit.PrometheusAPIV1Query(t, "foo_bar1", apptest.QueryOpts{
		Time: instantCT,
	})
	if diff := cmp.Diff(want, queryRes, cmpOpt); diff != "" {
		t.Fatalf("unexpected response (-want, +got):\n%s", diff)
	}

	// success - multitenant `/api/v1/query`
	// query is split into two queries for each tenant, so the final result can exceed the limit.
	want = apptest.NewPrometheusAPIV1QueryResponse(t,
		`{"data":
       {"result":[
          {"metric":{"__name__":"foo_bar1","instance":"a","vm_account_id":"0","vm_project_id":"0"},"value":[1652169900,"1"]},
          {"metric":{"__name__":"foo_bar1","instance":"a","vm_account_id":"1","vm_project_id":"15"},"value":[1652169900,"1"]}
        ]
       }
     }`,
	)
	queryRes = vmselectSmallLimit.PrometheusAPIV1Query(t, "foo_bar1", apptest.QueryOpts{
		Time:   instantCT,
		Tenant: "multitenant",
	})
	if diff := cmp.Diff(want, queryRes, cmpOpt); diff != "" {
		t.Fatalf("unexpected response (-want, +got):\n%s", diff)
	}

	// fail - `/api/v1/query`, exceed vmselect `maxUniqueTimeseries`
	queryRes = vmselectSmallLimit.PrometheusAPIV1Query(t, "foo_bar2", apptest.QueryOpts{
		Time: instantCT,
	})
	if queryRes.ErrorType != "422" {
		t.Fatalf("unexpected status code, got %s, want %d, error message is: %v", queryRes.ErrorType, 422, queryRes.Error)
	}

	// fail - `/api/v1/query`, exceed vmstorage `maxUniqueTimeseries`
	queryRes = vmselectNoLimit.PrometheusAPIV1Query(t, "foo_bar3", apptest.QueryOpts{
		Time: instantCT,
	})
	if queryRes.ErrorType != "422" {
		t.Fatalf("unexpected status code, got %s, want %d, error message is: %v", queryRes.ErrorType, 422, queryRes.Error)
	}

	// fail - `/api/v1/query`, vmselect `maxUniqueTimeseries` cannot exceed vmstorage `maxUniqueTimeseries`
	queryRes = vmselectBigLimit.PrometheusAPIV1Query(t, "foo_bar3", apptest.QueryOpts{
		Time: instantCT,
	})
	if queryRes.ErrorType != "422" {
		t.Fatalf("unexpected status code, got %s, want %d, error message is: %v", queryRes.ErrorType, 422, queryRes.Error)
	}
}

func TestClusterMaxSeries(t *testing.T) {
	fs.MustRemoveDir(t.Name())

	cmpSROpt := cmpopts.IgnoreFields(apptest.PrometheusAPIV1SeriesResponse{}, "Status", "IsPartial")

	tc := apptest.NewTestCase(t)
	defer tc.Stop()
	vmstorage := tc.MustStartVmstorage("vmstorage", []string{
		"-storageDataPath=" + tc.Dir() + "/vmstorage",
		"-retentionPeriod=100y",
		"-search.maxUniqueTimeseries=2",
	})
	vminsert := tc.MustStartVminsert("vminsert", []string{
		"-storageNode=" + vmstorage.VminsertAddr(),
	})
	apptest.EnsureBlockingIngestion(t, vminsert, []*apptest.Vmstorage{vmstorage})
	vmselectBigLimit := tc.MustStartVmselect("vmselect2", []string{
		"-storageNode=" + vmstorage.VmselectAddr(),
		"-search.tenantCacheExpireDuration=0",
		"-search.maxSeries=3",
	})
	vmselectSmallLimit := tc.MustStartVmselect("vmselect1", []string{
		"-storageNode=" + vmstorage.VmselectAddr(),
		"-search.tenantCacheExpireDuration=0",
		"-search.maxSeries=1",
	})

	var commonSamples = []string{
		`foo_bar3{instance="a"} 1.00 1652169660000`,
		`foo_bar3{instance="b"} 2.00 1652169660000`,
		`foo_bar3{instance="c"} 3.00 1652169660000`,
	}

	// write data
	vminsert.PrometheusAPIV1ImportPrometheus(t, commonSamples, apptest.QueryOpts{})
	vmstorage.ForceFlush(t)

	// success - `/api/v1/series`, vmselect `maxLabelsAPISeries` can exceed vmstorage `maxLabelsAPISeries``
	wantSR := apptest.NewPrometheusAPIV1SeriesResponse(t,
		`{"data": [
			{"__name__":"foo_bar3","instance":"a"},
			{"__name__":"foo_bar3","instance":"b"},
			{"__name__":"foo_bar3","instance":"c"}
			]
		 }`)
	seriesRes := vmselectBigLimit.PrometheusAPIV1Series(t, "foo_bar3", apptest.QueryOpts{
		Start: "2022-05-10T08:03:00.000Z",
	})
	if diff := cmp.Diff(wantSR.Sort(), seriesRes.Sort(), cmpSROpt); diff != "" {
		t.Fatalf("unexpected response (-want, +got):\n%s", diff)
	}

	// fail - `/api/v1/series`, exceed vmselect `maxSeries`
	seriesRes1 := vmselectSmallLimit.PrometheusAPIV1Series(t, "foo_bar3", apptest.QueryOpts{
		Start: "2022-05-10T08:03:00.000Z",
	})
	if seriesRes1.ErrorType != "422" {
		t.Fatalf("unexpected status code, got %s, want %d, error message is: %v", seriesRes.ErrorType, 422, seriesRes1.Error)
	}

}
