package kube

import (
	"fmt"

	"k8s.io/apimachinery/pkg/api/resource"
)

func calcPercentage(dividend, divisor int64) int64 {
	return int64(float64(dividend) / float64(divisor) * 100)
}

type MemoryResource struct {
	*resource.Quantity
}

func NewMemoryResource(value int64) *MemoryResource {
	return &MemoryResource{resource.NewQuantity(value, resource.BinarySI)}
}

func (r *MemoryResource) calcPercentage(divisor *resource.Quantity) int64 {
	return calcPercentage(r.Value(), divisor.Value())
}

func (r *MemoryResource) String() string {
	// XXX: Support more units
	return fmt.Sprintf("%vMi", r.Value()/(1024*1024))
}

func (r *MemoryResource) ToQuantity() *resource.Quantity {
	return resource.NewQuantity(r.Value(), resource.BinarySI)
}

type CpuResource struct {
	*resource.Quantity
}

func NewCpuResource(value int64) *CpuResource {
	r := resource.NewMilliQuantity(value, resource.DecimalSI)
	return &CpuResource{r}
}

func (r *CpuResource) String() string {
	// XXX: Support more units
	return fmt.Sprintf("%vm", r.MilliValue())
}

func (r *CpuResource) calcPercentage(divisor *resource.Quantity) int64 {
	return calcPercentage(r.MilliValue(), divisor.MilliValue())
}

func (r *CpuResource) ToQuantity() *resource.Quantity {
	return resource.NewMilliQuantity(r.MilliValue(), resource.DecimalSI)
}

type MemoryMetrics struct {
	MemoryMin  *MemoryResource
	MemoryMax  *MemoryResource
	MemoryAvg  *MemoryResource
	MemoryLast *MemoryResource
}

type MemoryResources struct {
	MemReq             *MemoryResource
	MemLimit           *MemoryResource
	PercentMemoryReq   int64
	PercentMemoryLimit int64
}

type CPUResources struct {
	CpuReq          *CpuResource
	CpuLimit        *CpuResource
	PercentCpuReq   int64
	PercentCpuLimit int64
}
type CPUMetrics struct {
	CpuMin  *CpuResource
	CpuMax  *CpuResource
	CpuAvg  *CpuResource
	CpuLast *CpuResource
}

type ContainerResources struct {
	Name               string
	ContainerName      string
	PodName            string
	Namespace          string
	OwnerName          string
	OwnerKind          string
	CpuReq             *CpuResource
	CpuLimit           *CpuResource
	PercentCpuReq      int64
	PercentCpuLimit    int64
	MemReq             *MemoryResource
	MemLimit           *MemoryResource
	PercentMemoryReq   int64
	PercentMemoryLimit int64
}

type ContainerResourcesMetrics struct {
	ContainerName   string
	PodName         string
	Namespace       string
	OwnerName       string
	OwnerKind       string
	CPUResources    CPUResources
	MemoryResources MemoryResources
	CPUMetrics      CPUMetrics
	MemoryMetrics   MemoryMetrics
}

func (r ContainerResources) Validate(field string) bool {
	for _, v := range GetFields(&r) {
		if field == v {
			return true
		}
	}
	return false
}
