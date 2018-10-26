package kube

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/montanaflynn/stats"
	api "github.com/prometheus/client_golang/api"
	apiv1 "github.com/prometheus/client_golang/api/prometheus/v1"
	"github.com/prometheus/common/model"
	"golang.org/x/net/context"
	"k8s.io/api/core/v1"
)

type PrometheusClient struct {
	ctx     context.Context
	client  api.Client
	address string
}

func NewPrometheusClient(address string) (*PrometheusClient, error) {
	ctx := context.Background()
	c, err := api.NewClient(api.Config{Address: address})
	if err != nil {
		return nil, err
	}

	return &PrometheusClient{
		ctx:     ctx,
		client:  c,
		address: address,
	}, nil

}

type PrometheusMetricJob struct {
	ContainerName  string
	PodName        string
	PodUID         string
	ContainerIndex int
	Duration       time.Duration
	MetricType     v1.ResourceName
	jobs           <-chan *PrometheusMetricJob
	collector      chan<- *ContainerMetrics
}

func sortPrometheusPointsAsc(points []model.SamplePair) {
	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp > points[j].Timestamp
	})
}

func evaluatePromMemMetrics(it *model.SampleStream) *ContainerMetrics {

	if len(it.Values) == 0 {
		return &ContainerMetrics{
			MetricType: v1.ResourceMemory,

			MemoryMetrics: MemoryMetrics{
				MemoryLast: NewMemoryResource(0),
				MemoryMin:  NewMemoryResource(0),
				MemoryMax:  NewMemoryResource(0),
				MemoryAvg:  NewMemoryResource(0),
			},

			DataPoints: int64(0),
		}
	}

	var data []float64
	for i := 0; i < len(it.Values); i++ {
		data = append(data, float64(it.Values[i].Value))
	}

	sortPrometheusPointsAsc(it.Values)

	last := data[0]
	mode, _ := stats.Mode(data)
	if len(mode) == 0 {
		mode = append(mode, 0)
	}
	min, _ := stats.Min(data)
	max, _ := stats.Max(data)
	return &ContainerMetrics{
		MetricType: v1.ResourceMemory,
		MemoryMetrics: MemoryMetrics{
			MemoryLast: NewMemoryResource(int64(last)),
			MemoryMin:  NewMemoryResource(int64(min)),
			MemoryMax:  NewMemoryResource(int64(max)),
			MemoryAvg:  NewMemoryResource(int64(mode[0])),
		},
		DataPoints: int64(len(it.Values)),
	}
}

func evaluatePromCpuMetrics(it *model.SampleStream) *ContainerMetrics {

	if len(it.Values) == 0 {
		return &ContainerMetrics{
			MetricType: v1.ResourceCPU,
			CPUMetrics: CPUMetrics{
				CpuLast: NewCpuResource(0),
				CpuMin:  NewCpuResource(0),
				CpuMax:  NewCpuResource(0),
				CpuAvg:  NewCpuResource(0),
			},
			DataPoints: int64(0),
		}
	}

	var data []float64
	for i := 0; i < len(it.Values); i++ {
		data = append(data, float64(it.Values[i].Value))
	}

	sortPrometheusPointsAsc(it.Values)
	last := data[0]
	mean, _ := stats.Mean(data)
	min, _ := stats.Min(data)
	max, _ := stats.Max(data)

	return &ContainerMetrics{
		MetricType: v1.ResourceCPU,
		CPUMetrics: CPUMetrics{
			CpuLast: NewCpuResource(int64(last * 1000)),
			CpuMin:  NewCpuResource(int64(min * 1000)),
			CpuMax:  NewCpuResource(int64(max * 1000)),
			CpuAvg:  NewCpuResource(int64(mean * 1000)),
		},
		DataPoints: int64(len(it.Values)),
	}
}

func buildPromSQLFromFilter(m map[string]string) string {

	// buffer := make([]string, len(m))
	var buffer []string

	metric := m["metric"]
	delete(m, "metric")

	for k, v := range m {
		buffer = append(buffer, fmt.Sprintf("%s=\"%s\"", k, v))
	}
	filterbuffer := strings.Join(buffer, ",")

	return fmt.Sprintf("sum(rate(%s{%s}[5m]))", metric, filterbuffer)
}

func (s *PrometheusClient) ListTimeSeries(query string, duration time.Duration) (*model.SampleStream, error) {
	var queryResult model.Value
	var matrixVal model.Matrix
	var timeRange apiv1.Range
	var retries int = 5
	var err error
	//query := buildPromSQLFromFilter(filter_map)

	end := time.Now().UTC()
	start := end.Add(-duration)
	apiclient := apiv1.NewAPI(s.client)

	for attempt := 1; queryResult == nil && retries > 0; attempt++ {
		step := time.Duration(30 * attempt)
		timeRange = apiv1.Range{Start: start, End: end, Step: time.Second * step}
		queryResult, err = apiclient.QueryRange(s.ctx, query, timeRange)
		if err != nil {
			switch err.(type) {
			case *apiv1.Error:
				if err.(*apiv1.Error).Type == "bad_data" {
					// exceeded maximum resolution. decreasing the query resolution
					log.Warn("decreasing the query resolution")
					retries--
				}
			default:
				panic(err.Error())
			}
		}
	}

	switch {
	case queryResult.Type() == model.ValMatrix:
		matrixVal = queryResult.(model.Matrix)
	default:
		return &model.SampleStream{}, fmt.Errorf("Unknown type %T", queryResult.Type())
	}
	if matrixVal.Len() != 1 {
		return &model.SampleStream{}, fmt.Errorf("Unexpected response length %d: %s", matrixVal.Len(), query)
	}
	sampleStream := matrixVal[0]

	return sampleStream, nil
}

func (s *PrometheusClient) ContainerMetrics(container_name string, pod_name string, duration time.Duration, metric_type v1.ResourceName) (*ContainerMetrics, error) {

	var m *ContainerMetrics
	var query string
	switch metric_type {
	case v1.ResourceMemory:
		query = fmt.Sprintf("sum(container_memory_usage_bytes{container_name=\"%s\", pod_name=\"%s\"})", container_name, pod_name)
		it, _ := s.ListTimeSeries(query, duration)
		m = evaluatePromMemMetrics(it)
	case v1.ResourceCPU:
		query = fmt.Sprintf("sum(rate(container_cpu_usage_seconds_total{container_name=\"%s\", pod_name=\"%s\"}[5m]))", container_name, pod_name)
		it, _ := s.ListTimeSeries(query, duration)
		m = evaluatePromCpuMetrics(it)
	}

	return m, nil
}

func (s *PrometheusClient) Allocate(jobs chan<- *PrometheusMetricJob, wg *sync.WaitGroup, containerData []*ContainerResourcesMetrics, duration time.Duration) {
	// for _, pod := range pods {
	// 	for _, container := range pod.Spec.Containers {
	for i, crm := range containerData {
		jobs <- &PrometheusMetricJob{
			ContainerName:  crm.ContainerName,
			PodName:        crm.PodName,
			Duration:       duration,
			MetricType:     v1.ResourceCPU,
			ContainerIndex: i,
		}
		jobs <- &PrometheusMetricJob{
			ContainerName:  crm.ContainerName,
			PodName:        crm.PodName,
			Duration:       duration,
			MetricType:     v1.ResourceMemory,
			ContainerIndex: i,
		}
		log.Debugf("Dispatched job for %s/%s", crm.PodName, crm.ContainerName)
	}
	// 	}
	// }
	close(jobs)
}

func (s *PrometheusClient) Worker(jobs <-chan *PrometheusMetricJob, collector chan<- *ContainerMetrics, wg *sync.WaitGroup) {

	for job := range jobs {

		m, _ := s.ContainerMetrics(job.ContainerName, job.PodName, job.Duration, job.MetricType)
		m.ContainerIndex = job.ContainerIndex
		collector <- m
		log.Debugf("Completed job for %s/%s. Retrieved %d datapoints", job.PodName, job.ContainerName, m.DataPoints)
	}
	wg.Done()
}

func (s *PrometheusClient) Collect(collector <-chan *ContainerMetrics, resultsCh chan<- []*ContainerMetrics) {
	var metrics []*ContainerMetrics
	for result := range collector {
		metrics = append(metrics, result)
	}
	resultsCh <- metrics
}

func (k *KubeClient) GetPrometheusHistorical(prometheus_url string, containerData []*ContainerResourcesMetrics, workers int, cpu bool, mem bool, duration time.Duration) ([]*ContainerResourcesMetrics, error) {
	var metrics []*ContainerMetrics

	prometheus, err := NewPrometheusClient(prometheus_url)
	if err != nil {
		return nil, err
	}

	// Create dispatcher and collector channels
	jobs := make(chan *PrometheusMetricJob, workers)
	collector := make(chan *ContainerMetrics)
	// Create WaitGroup
	var wg sync.WaitGroup

	log.Debug("Starting dispatcher routine")
	go prometheus.Allocate(jobs, &wg, containerData, duration)

	resultCh := make(chan []*ContainerMetrics)
	log.Debug("Starting collector routine")
	go prometheus.Collect(collector, resultCh)

	for i := 0; i <= workers; i++ {
		log.Debugf("Starting worker %d", i)
		wg.Add(1)
		go prometheus.Worker(jobs, collector, &wg)
	}

	// Wait for completion
	wg.Wait()
	log.Debug("All workers have terminated")
	close(collector)
	metrics = <-resultCh

	outputData := containerData
	for _, metric := range metrics {
		if metric.MetricType == v1.ResourceMemory {
			outputData[metric.ContainerIndex].MemoryMetrics = metric.MemoryMetrics
		}
		if metric.MetricType == v1.ResourceCPU {
			outputData[metric.ContainerIndex].CPUMetrics = metric.CPUMetrics
		}
	}

	return outputData, nil
}
