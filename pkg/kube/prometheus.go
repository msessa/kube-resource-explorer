package kube

import (
	"fmt"
	"sort"
	"strings"
	"time"

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

func NewPrometheusClient(address string) *PrometheusClient {
	ctx := context.Background()
	c, err := api.NewClient(api.Config{Address: address})
	if err != nil {
		panic(err.Error())
	}

	return &PrometheusClient{
		ctx:     ctx,
		client:  c,
		address: address,
	}

}

type PrometheusMetricJob struct {
	ContainerName string
	PodName       string
	PodUID        string
	Duration      time.Duration
	MetricType    v1.ResourceName
	jobs          <-chan *MetricJob
	collector     chan<- *ContainerMetrics
}

func sortPrometheusPointsAsc(points []model.SamplePair) {
	sort.Slice(points, func(i, j int) bool {
		return points[i].Timestamp > points[j].Timestamp
	})
}

func evaluatePromMemMetrics(it *model.SampleStream) *ContainerMetrics {

	//var points []*monitoringpb.Point
	//set := make(map[int64]int)

	// for {
	// 	resp, err := it.Next()
	// 	// This doesn't work
	// 	if err == iterator.Done {
	// 		break
	// 	}
	// 	if err != nil {
	// 		log.WithError(err).Debug("iterating")
	// 		break
	// 	}

	// 	log.Debug(resp.Metric)
	// 	log.Debug(resp.Resource)

	// 	for _, point := range resp.Points {
	// 		value := int64(point.Value.GetInt64Value())
	// 		if _, ok := set[value]; ok {
	// 			set[value] += 1
	// 		} else {
	// 			set[value] = 1
	// 		}
	// 		points = append(points, point)
	// 	}
	// }
	var data []int64
	for i := 1; i < len(it.Values); i++ {
		data = append(data, int64(it.Values[i].Value))
	}

	// for k, _ := range set {
	// 	data = append(data, k)
	// }

	sortPrometheusPointsAsc(it.Values)

	min, max := MinMax_int64(data)
	return &ContainerMetrics{
		MetricType: v1.ResourceMemory,
		MemoryLast: NewMemoryResource(data[0]),
		MemoryMin:  NewMemoryResource(min),
		MemoryMax:  NewMemoryResource(max),
		MemoryMode: NewMemoryResource(1),
		DataPoints: int64(len(it.Values)),
	}
}

func evaluatePromCpuMetrics(it *model.SampleStream) *ContainerMetrics {
	// var points []*monitoringpb.Point

	// for {
	// 	resp, err := it.Next()
	// 	// This doesn't work
	// 	if err == iterator.Done {
	// 		break
	// 	}
	// 	if err != nil {
	// 		// probably isn't a critical error, see above
	// 		log.WithError(err).Debug("iterating")
	// 		break
	// 	}

	// 	log.Debug(resp.Metric)
	// 	log.Debug(resp.Resource)

	// 	for _, point := range resp.Points {
	// 		points = append(points, point)
	// 	}
	// }

	sortPrometheusPointsAsc(it.Values)

	var data []float64

	for i := 1; i < len(it.Values); i++ {
		cur := it.Values[i]
		//prev := it.Values[i-1]

		//interval := cur.Timestamp - prev.Timestamp

		//delta := float64(cur.Value) - float64(prev.Value)
		//data = append(data, int64((delta/float64(interval))*1000))
		data = append(data, float64(cur.Value))
	}

	min, max := MinMax_float64(data)
	var last float64
	if len(data) == 0 {
		last = 0
	} else {
		last = data[0]
	}
	return &ContainerMetrics{
		MetricType: v1.ResourceCPU,
		CpuLast:    NewCpuResource(int64(last * 1000)),
		CpuMin:     NewCpuResource(int64(min * 1000)),
		CpuMax:     NewCpuResource(int64(max * 1000)),
		CpuAvg:     NewCpuResource(int64(average_float64(data) * 1000)),
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
			// case *apiv1.Error:
			// 	if err.(*apiv1.Error).Type == "bad_data" {
			// 		// exceeded maximum resolution. decreasing the query resolution
			// 		log.Warn("decreasing the query resolution")
			// 		retries--
			// 	}
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
	m.ContainerName = container_name
	return m, nil
}

func (s *PrometheusClient) Run(jobs chan<- *PrometheusMetricJob, collector <-chan *ContainerMetrics, pods []v1.Pod, duration time.Duration, metric_type v1.ResourceName) (metrics []*ContainerMetrics) {

	var jobcount int = 0
	var resultcount int = 0
	go func() {
		for _, pod := range pods {
			for _, container := range pod.Spec.Containers {
				jobs <- &PrometheusMetricJob{
					ContainerName: container.Name,
					PodName:       pod.GetName(),
					PodUID:        string(pod.ObjectMeta.UID),
					Duration:      duration,
					MetricType:    metric_type,
				}
				jobcount++
			}
		}
		close(jobs)
	}()

	for job := range collector {
		metrics = append(metrics, job)
		resultcount++
		if resultcount == jobcount {
			break
		}
	}

	return
}

func (s *PrometheusClient) Worker(jobs <-chan *PrometheusMetricJob, collector chan<- *ContainerMetrics) {

	for job := range jobs {
		m, _ := s.ContainerMetrics(job.ContainerName, job.PodName, job.Duration, job.MetricType)
		m.PodName = job.PodName
		collector <- m
	}
}

func (k *KubeClient) PrometheusHistorical(address, namespace string, workers int, resourceName v1.ResourceName, duration time.Duration, sort string, reverse bool, csv bool) {

	prometheus := NewPrometheusClient(
		address,
	)

	activePods, err := k.ActivePods(namespace, "")
	if err != nil {
		panic(err.Error())
	}

	jobs := make(chan *PrometheusMetricJob, workers)
	collector := make(chan *ContainerMetrics)

	for i := 0; i <= workers; i++ {
		go prometheus.Worker(jobs, collector)
	}

	metrics := prometheus.Run(jobs, collector, activePods, duration, resourceName)
	close(collector)
	rows, dataPoints := FormatContainerMetrics(metrics, resourceName, duration, sort, reverse)
	PrintContainerMetrics(rows, duration, dataPoints)
}
