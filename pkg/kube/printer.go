package kube

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/olekukonko/tablewriter"
	"github.com/ryanuber/columnize"
	"golang.org/x/text/language"
	"golang.org/x/text/message"
	"k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
)

func _cmp(f1, f2 interface{}, reverse bool, field string) bool {

	if q1, ok := f1.(*resource.Quantity); ok {
		q2 := f2.(*resource.Quantity)
		if reverse {
			return q1.Cmp(*q2) < 0
		}
		return q1.Cmp(*q2) > 0
	}

	if r1, ok := f1.(*CpuResource); ok {
		r2 := f2.(*CpuResource)
		v := r2.ToQuantity()
		if reverse {
			return r1.Cmp(*v) < 0
		}
		return r1.Cmp(*v) > 0
	}

	if m1, ok := f1.(*MemoryResource); ok {
		m2 := f2.(*MemoryResource)
		v := m2.ToQuantity()
		if reverse {
			return m1.Cmp(*v) < 0
		}
		return m1.Cmp(*v) > 0
	}

	if v1, ok := f1.(int64); ok {
		v2 := f2.(int64)
		if reverse {
			return v1 < v2
		}
		return v1 > v2

	}

	if s1, ok := f1.(string); ok {
		s2 := f2.(string)
		if reverse {
			return strings.Compare(s1, s2) > 0
		}
		return strings.Compare(s1, s2) < 0
	}

	panic(fmt.Sprintf("Unknown type: _cmp %s", field))
}

func cmp(t interface{}, field string, i, j int, reverse bool) bool {

	if ra, ok := t.([]*ContainerResources); ok {
		return _cmp(GetField(ra[i], field), GetField(ra[j], field), reverse, field)
	}

	if cm, ok := t.([]*ContainerMetrics); ok {
		return _cmp(GetField(cm[i], field), GetField(cm[j], field), reverse, field)
	}

	panic("Unknown type: cmp")
}

func fmtPercent(p int64) string {
	return fmt.Sprintf("%d%%", p)
}

func FormatResourceUsage(capacity v1.ResourceList, resources []*ContainerResources, field string, reverse bool) (rows [][]string) {

	sort.Slice(resources, func(i, j int) bool {
		return cmp(resources, field, i, j, reverse)
	})

	rows = append(rows, [][]string{
		{"Namespace", "Name", "CpuReq", "CpuReq%", "CpuLimit", "CpuLimit%", "MemReq", "MemReq%", "MemLimit", "MemLimit%"},
		{"---------", "----", "------", "-------", "--------", "---------", "------", "-------", "--------", "---------"},
	}...)

	totalCpuReq, totalCpuLimit := NewCpuResource(0), NewCpuResource(0)
	totalMemoryReq, totalMemoryLimit := NewMemoryResource(0), NewMemoryResource(0)

	for _, u := range resources {
		totalCpuReq.Add(*u.CpuReq.ToQuantity())
		totalCpuLimit.Add(*u.CpuLimit.ToQuantity())
		totalMemoryReq.Add(*u.MemReq.ToQuantity())
		totalMemoryLimit.Add(*u.MemLimit.ToQuantity())

		rows = append(rows, []string{
			u.Namespace,
			u.Name,
			u.CpuReq.String(),
			fmtPercent(u.PercentCpuReq),
			u.CpuLimit.String(),
			fmtPercent(u.PercentCpuLimit),
			u.MemReq.String(),
			fmtPercent(u.PercentMemoryReq),
			u.MemLimit.String(),
			fmtPercent(u.PercentMemoryLimit),
		})
	}

	rows = append(rows, []string{"---------", "----", "------", "-------", "--------", "---------", "------", "-------", "--------", "---------"})

	cpuCapacity := NewCpuResource(capacity.Cpu().MilliValue())
	memoryCapacity := NewMemoryResource(capacity.Memory().Value())

	rows = append(rows, []string{
		"Total",
		"",
		fmt.Sprintf("%s/%s", totalCpuReq.String(), cpuCapacity.String()),
		fmtPercent(totalCpuReq.calcPercentage(capacity.Cpu())),
		fmt.Sprintf("%s/%s", totalCpuLimit.String(), cpuCapacity.String()),
		fmtPercent(totalCpuLimit.calcPercentage(capacity.Cpu())),
		fmt.Sprintf("%s/%s", totalMemoryReq.String(), memoryCapacity.String()),
		fmtPercent(totalMemoryReq.calcPercentage(capacity.Memory())),
		fmt.Sprintf("%s/%s", totalMemoryLimit.String(), memoryCapacity.String()),
		fmtPercent(totalMemoryLimit.calcPercentage(capacity.Memory())),
	})

	return rows
}

func ExportCSV(prefix string, rows [][]string) string {

	now := time.Now()

	filename := fmt.Sprintf("%s-%02d%02d%02d%02d%02d.csv", prefix, now.Year(), now.Month(), now.Day(), now.Hour(), now.Minute())

	f, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		panic(err.Error())
	}

	w := csv.NewWriter(f)
	w.WriteAll(rows)

	if err := w.Error(); err != nil {
		log.Fatalln("error writing csv:", err)
	}

	if err := f.Close(); err != nil {
		panic(err.Error())
	}

	return filename
}

func PrintResourceUsage(rows [][]string) {
	var formatted []string

	for _, row := range rows {
		formatted = append(formatted, strings.Join(row, " | "))
	}

	fmt.Println(columnize.SimpleFormat(formatted))
}

func PrintContainerMetrics(rows [][]string, duration time.Duration, total int64) {

	p := message.NewPrinter(language.English)

	var table []string
	for _, row := range rows {
		table = append(table, strings.Join(row, " | "))
	}

	fmt.Println(columnize.SimpleFormat(table))
	fmt.Printf("\nResults shown are for a period of %s. %s data points were evaluted.\n", duration.String(), p.Sprintf("%d", total))
}

// func getMetricsForPodContainerAndType(metrics []*ContainerMetrics, name string, resource_type v1.ResourceName) *ContainerMetrics {
// 	for _, u := range metrics {
// 		if u.FullName == name && u.MetricType == resource_type {
// 			return u
// 		}
// 	}
// 	return nil
// }

func (k *KubeClient) PrintData(resources []*ContainerResourcesMetrics, capacity v1.ResourceList, byowner bool) {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Pod/Container", "CPU Req", "CPU Lim", "CPU Avg", "CPU Max", "Mem Req", "Mem Lim", "Mem Avg", "Mem Max"})

	// Group resources by owner controller
	//rs := make(map[string][]*ContainerResources)
	//var outputResources []*ContainerResources
	// for _, v := range resources {
	// 	k := fmt.Sprintf("%s/%s/%s", v.OwnerKind, v.OwnerName, v.ContainerName)
	// 	rs[k] = append(rs[k], v)
	// }
	// spew.Dump(rs)

	for _, u := range resources {
		row := []string{fmt.Sprintf("%s/%s/%s", u.OwnerKind, u.OwnerName, u.ContainerName)}

		row = append(row, u.CPUResources.CpuReq.String(), u.CPUResources.CpuLimit.String())
		if u.CPUMetrics != (CPUMetrics{}) {
			//CpuStats := getMetricsForPodContainerAndType(metrics, u.Name, v1.ResourceCPU)
			//if u.CPUMetrics != nil {
			row = append(row, u.CPUMetrics.CpuAvg.String(), u.CPUMetrics.CpuMax.String())
			//} else {
			//	row = append(row, "NoData", "NoData")
			//}
		} else {
			row = append(row, "NoData", "NoData")
		}

		row = append(row, u.MemoryResources.MemReq.String(), u.MemoryResources.MemLimit.String())
		if u.MemoryMetrics != (MemoryMetrics{}) {
			//MemStats := getMetricsForPodContainerAndType(metrics, u.Name, v1.ResourceMemory)
			//if MemStats != nil {
			row = append(row, u.MemoryMetrics.MemoryAvg.String(), u.MemoryMetrics.MemoryMax.String())
			//} else {
			//	row = append(row, "NoData", "NoData")
			//}
		} else {
			row = append(row, "NoData", "NoData")
		}
		table.Append(row)
	}
	table.Render()

}
