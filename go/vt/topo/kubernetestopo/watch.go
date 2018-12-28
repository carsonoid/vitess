/*
Copyright 2017 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreedto in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kubernetestopo

import (
	"encoding/base64"

	"golang.org/x/net/context"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/client-go/tools/cache"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
)

// Watch is part of the topo.Conn interface.
func (s *Server) Watch(ctx context.Context, filePath string) (*topo.WatchData, <-chan *topo.WatchData, topo.CancelFunc) {
	log.Info("Starting Kubernetes topo Watch")

	current := &topo.WatchData{}

	// get current
	contents, ver, err := s.Get(ctx, filePath)
	if err != nil {
		// Per the topo.Conn interface:
		// "If the initial read fails, or the file doesn't
		// exist, current.Err is set, and 'changes'/'cancel' are nil."
		current.Err = err
		return current, nil, nil
	}
	current.Contents = contents
	current.Version = ver

	// Create a context, will be used to cancel the watch.
	watchCtx, watchCancel := context.WithCancel(context.Background())

	// Create the changes channel
	changes := make(chan *topo.WatchData, 10)

	// Create a signal channel for non-interrupt shutdowns
	gracefulShutdown := make(chan struct{})

	// Create the informer / indexer to watch the single resource
	restClient := s.kubeClient.CoreV1().RESTClient()
	listwatch := cache.NewListWatchFromClient(restClient, "configmaps", s.namespace, fields.OneTermEqualSelector("metadata.name", s.buildFileResource(filePath, []byte{}).Name))

	s.memberIndexer, s.memberInformer = cache.NewIndexerInformer(listwatch, &corev1.ConfigMap{}, 0,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				cm := obj.(*corev1.ConfigMap)
				out, err := base64.StdEncoding.DecodeString(cm.Data["value"])
				if err != nil {
					changes <- &topo.WatchData{Err: err}
					close(gracefulShutdown)
				} else {
					changes <- &topo.WatchData{
						Contents: out,
						Version:  KubernetesVersion(cm.GetResourceVersion()),
					}
				}
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				cm := newObj.(*corev1.ConfigMap)
				out, err := base64.StdEncoding.DecodeString(cm.Data["value"])
				if err != nil {
					changes <- &topo.WatchData{Err: err}
					close(gracefulShutdown)
				} else {
					changes <- &topo.WatchData{
						Contents: out,
						Version:  KubernetesVersion(cm.GetResourceVersion()),
					}
				}
			},
			DeleteFunc: func(obj interface{}) {
				cm := obj.(*corev1.ConfigMap)
				changes <- &topo.WatchData{Err: topo.NewError(topo.NoNode, cm.Name)}
				close(gracefulShutdown)
			},
		}, cache.Indexers{})

	// create control chan for informer and start it
	informerChan := make(chan struct{})
	go s.memberInformer.Run(informerChan)

	// Handle interrupts
	go closeOnDone(watchCtx, filePath, informerChan, gracefulShutdown, changes)

	return current, changes, topo.CancelFunc(watchCancel)
}

func closeOnDone(ctx context.Context, filePath string, informerChan chan struct{}, gracefulShutdown chan struct{}, changes chan *topo.WatchData) {
	select {
	case <-ctx.Done():
		if err := ctx.Err(); err != nil && err == context.Canceled {
			changes <- &topo.WatchData{Err: topo.NewError(topo.Interrupted, filePath)}
		}
	case <-gracefulShutdown:
	}
	close(informerChan)
	close(changes)
}
