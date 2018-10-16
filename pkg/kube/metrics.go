package kube

import (
	"k8s.io/api/core/v1"
)

type ContainerMetrics struct {
	ContainerIndex int

	MetricType v1.ResourceName
	DataPoints int64

	MemoryMetrics MemoryMetrics
	CPUMetrics    CPUMetrics
}

func (m ContainerMetrics) Validate(field string) bool {
	for _, v := range GetFields(&m) {
		if field == v {
			return true
		}
	}
	return false
}
