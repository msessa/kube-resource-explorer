package main

import (
	"flag"
	"os"
	"path/filepath"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/dpetzold/kube-resource-explorer/pkg/kube"
	"k8s.io/client-go/kubernetes"
	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
	"k8s.io/client-go/tools/clientcmd"
)

var GitCommit string

func homeDir() string {
	if h := os.Getenv("HOME"); h != "" {
		return h
	}
	return os.Getenv("USERPROFILE") // windows
}

func main() {

	default_duration, err := time.ParseDuration("4h")
	if err != nil {
		panic(err.Error())
	}

	var (
		namespace = flag.String("namespace", "default", "filter by namespace (defaults to all)")
		//sort           = flag.String("sort", "CpuReq", "field to sort by")
		//reverse        = flag.Bool("reverse", false, "reverse sort output")
		//historical     = flag.Bool("historical", false, "show historical info")
		byowner        = flag.Bool("byowner", false, "aggregate results by owner (eg. Deployment or DaemonSet)")
		duration       = flag.Duration("duration", default_duration, "specify the duration")
		historycal_mem = flag.Bool("historical_mem", false, "show historical memory info")
		historycal_cpu = flag.Bool("historical_cpu", false, "show historical cpu info")
		prometheusurl  = flag.String("prometheusurl", "", "Prometheus API URL for historical data")
		workers        = flag.Int("workers", 5, "Number of workers for historical")
		csv            = flag.Bool("csv", false, "Export results in CSV format")
		kubeconfig     *string
	)

	if home := homeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	log.SetLevel(log.InfoLevel)
	flag.Parse()

	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		panic(err.Error())
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	k := kube.NewKubeClient(clientset)

	if *prometheusurl == "" && (*historycal_cpu || *historycal_mem) {
		log.Error(" -prometheusurl is required for historical data")
		os.Exit(1)
	}

	containerData, clusterCapacity, err := k.GetKubeResourceUsage(*namespace)
	if err != nil {
		log.WithError(err).Error("Unable to retrieve cluster resource data")
		os.Exit(1)
	}

	if *historycal_cpu || *historycal_mem {
		log.Info("Retrieving historical data")
		containerData, err = k.GetPrometheusHistorical(*prometheusurl, containerData, *workers, *historycal_cpu, *historycal_mem, *duration)
		if err != nil {
			log.WithError(err).Error("Unable to retrieve historical memory data")
			os.Exit(1)
		}
	}

	printData := k.PrepareForPrinting(containerData, *byowner, *csv)

	k.PrintData(printData, clusterCapacity, *csv)
}
