package kube

import (
	"fmt"
	"os"

	"github.com/davecgh/go-spew/spew"
	"github.com/olekukonko/tablewriter"
	"k8s.io/api/core/v1"
)

func (k *KubeClient) AggregateData(resources []*ContainerResources, capacity v1.ResourceList, metrics []*ContainerMetrics, byowner bool) {
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"Pod/Container", "CPU Req", "CPU Lim", "CPU Avg", "CPU Max", "Mem Req", "Mem Lim", "Mem Avg", "Mem Max"})

	// Group resources by owner controller
	rs := make(map[string][]*ContainerResources)
	//var outputResources []*ContainerResources
	for _, v := range resources {
		k := fmt.Sprintf("%s/%s/%s", v.OwnerKind, v.OwnerName, v.ContainerName)
		rs[k] = append(rs[k], v)
	}
	spew.Dump(rs)
}
