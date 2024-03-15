package main

import (
	"path"
	"time"

	"github.com/cilium/hive"
	"github.com/cilium/hive/cell"
	"github.com/cilium/hive/job"
	"github.com/cilium/statedb"
	"github.com/cilium/statedb/reconciler"
	"github.com/cilium/statedb/reflector"
	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
)

var Hive = hive.New(
	job.Cell,
	statedb.Cell,
	cell.SimpleHealthCell,

	cell.Provide(
		NewPodTable,
		newClientset,
		podReflectorConfig,
		podReconcilerConfig,
	),

	cell.Invoke(
		statedb.RegisterTable[*Pod],

		reconciler.Register[*Pod],
	),

	reflector.KubernetesCell[*Pod](),
)

func main() {
	Hive.Run()
}

func podReflectorConfig(client *kubernetes.Clientset, pods statedb.RWTable[*Pod]) reflector.KubernetesConfig[*Pod] {
	lw := ListerWatcherFromTyped(client.CoreV1().Pods(""))
	return reflector.KubernetesConfig[*Pod]{
		BufferSize:     100,
		BufferWaitTime: 100 * time.Millisecond,
		ListerWatcher:  lw,
		Table:          pods,
		Transform: func(obj any) (*Pod, bool) {
			pod, ok := obj.(*v1.Pod)
			if ok {
				return &Pod{Pod: pod, reconciliationStatus: reconciler.StatusPending()}, true
			}
			return nil, false
		},
	}
}

func newClientset() (*kubernetes.Clientset, error) {
	cfg, err := clientcmd.BuildConfigFromFlags("", path.Join(homedir.HomeDir(), ".kube", "config"))
	if err != nil {
		panic(err.Error())
	}

	return kubernetes.NewForConfig(cfg)
}
