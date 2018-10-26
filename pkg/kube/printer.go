package kube

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/fatih/color"
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

type PrintableData struct {
	Rows    [][]string
	RawRows [][]string
	Header  []string
}

func (k *KubeClient) PrepareForPrinting(resources []*ContainerResourcesMetrics, byowner bool, csv bool) PrintableData {
	var outputData PrintableData
	if byowner {
		outputData.Header = []string{"Entity", "# Pods", "CPU Req", "CPU Lim", "CPU Avg", "CPU Max", "CPU Waste", "Mem Req", "Mem Lim", "Mem Avg", "Mem Max", "Mem Waste"}

		// Aggregate metrics by owner name. Slice -> Map
		aggregatedData := make(map[string][]*ContainerResourcesMetrics)
		for _, v := range resources {
			entityId := fmt.Sprintf("%s/%s/%s", v.OwnerKind, v.OwnerName, v.ContainerName)
			aggregatedData[entityId] = append(aggregatedData[entityId], v)
		}

		// For each owner, average all metrics across pods
		for k, v := range aggregatedData {
			var row []string
			var rawRow []string
			row = append(row, k)
			row = append(row, fmt.Sprintf("%d", len(v)))
			rawRow = append(rawRow, k)
			rawRow = append(rawRow, fmt.Sprintf("%d", len(v)))

			var AvgCPUReq, AvgCPULim, AvgMemReq, AvgMemLim int64
			var AvgCPUAvg, AvgCPUMax, AvgMemAvg, AvgMemMax int64
			for _, m := range v {
				AvgCPUReq += m.CPUResources.CpuReq.MilliValue()
				AvgCPULim += m.CPUResources.CpuLimit.MilliValue()
				AvgMemReq += m.MemoryResources.MemReq.Value()
				AvgMemLim += m.MemoryResources.MemLimit.Value()
				AvgCPUAvg += m.CPUMetrics.CpuAvg.MilliValue()
				AvgCPUMax += m.CPUMetrics.CpuMax.MilliValue()
				AvgMemAvg += m.MemoryMetrics.MemoryAvg.Value()
				AvgMemMax += m.MemoryMetrics.MemoryMax.Value()
			}
			AvgCPUReq /= int64(len(v))
			AvgCPULim /= int64(len(v))
			AvgMemReq /= int64(len(v))
			AvgMemLim /= int64(len(v))
			AvgCPUAvg /= int64(len(v))
			AvgCPUMax /= int64(len(v))
			AvgMemAvg /= int64(len(v))
			AvgMemMax /= int64(len(v))

			row = append(row, NewCpuResource(AvgCPUReq).String())
			row = append(row, NewCpuResource(AvgCPULim).String())
			row = append(row, NewCpuResource(AvgCPUAvg).String())
			row = append(row, NewCpuResource(AvgCPUMax).String())

			rawRow = append(rawRow,
				strconv.FormatInt(AvgCPUReq, 10),
				strconv.FormatInt(AvgCPULim, 10),
				strconv.FormatInt(AvgCPUAvg, 10),
				strconv.FormatInt(AvgCPUMax, 10),
				strconv.FormatInt(AvgCPUReq-AvgCPUAvg, 10),
			)

			// if AvgCPUReq > 0 && AvgCPUAvg > 0 {
			// 	row = append(row, strconv.FormatInt(calcPercentage(AvgCPUReq, AvgCPUAvg), 10))
			// } else {
			// 	row = append(row, "0")
			// }
			if !csv {
				if (AvgCPUReq - AvgCPUAvg) > 500 {
					row = append(row, color.RedString(NewCpuResource(AvgCPUReq-AvgCPUAvg).String()))
				} else if (AvgCPUReq - AvgCPUAvg) > 200 {
					row = append(row, color.YellowString(NewCpuResource(AvgCPUReq-AvgCPUAvg).String()))
				} else {
					row = append(row, color.GreenString(NewCpuResource(AvgCPUReq-AvgCPUAvg).String()))
				}

			} else {
				row = append(row, NewCpuResource(AvgCPUReq-AvgCPUAvg).String())
			}

			row = append(row, NewMemoryResource(AvgMemReq).String())
			row = append(row, NewMemoryResource(AvgMemLim).String())
			row = append(row, NewMemoryResource(AvgMemAvg).String())
			row = append(row, NewMemoryResource(AvgMemMax).String())

			rawRow = append(rawRow,
				strconv.FormatInt(AvgMemReq, 10),
				strconv.FormatInt(AvgMemLim, 10),
				strconv.FormatInt(AvgMemAvg, 10),
				strconv.FormatInt(AvgMemMax, 10),
				strconv.FormatInt(AvgMemReq-AvgMemAvg, 10),
			)
			if !csv {
				if (AvgMemReq - AvgMemAvg) > 500000000 {
					row = append(row, color.RedString(NewMemoryResource(AvgMemReq-AvgMemAvg).String()))
				} else if (AvgMemReq - AvgMemAvg) > 200000000 {
					row = append(row, color.YellowString(NewMemoryResource(AvgMemReq-AvgMemAvg).String()))
				} else {
					row = append(row, color.GreenString(NewMemoryResource(AvgMemReq-AvgMemAvg).String()))
				}
			} else {
				row = append(row, NewMemoryResource(AvgMemReq-AvgMemAvg).String())
			}
			outputData.RawRows = append(outputData.RawRows, rawRow)
			outputData.Rows = append(outputData.Rows, row)
		}
	}
	return outputData
}

func (k *KubeClient) PrintData(printdata PrintableData, capacity v1.ResourceList, export_as_csv bool) {

	if export_as_csv {
		writer := csv.NewWriter(os.Stdout)
		defer writer.Flush()
		writer.Write(printdata.Header)
		writer.WriteAll(printdata.RawRows)

	} else {
		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader(printdata.Header)
		for _, u := range printdata.Rows {
			table.Append(u)
		}
		table.SetRowLine(true)
		table.Render()
	}
}
