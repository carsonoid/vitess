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
	"fmt"
	"hash/fnv"
	"path/filepath"
	"strconv"
	"time"

	"golang.org/x/net/context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"vitess.io/vitess/go/vt/log"
	"vitess.io/vitess/go/vt/topo"
)

// NodeReference contains the data relating to a node
type NodeReference struct {
	id    string
	key   string
	value string
}

// ToData converts a nodeReference to the data type used in the ConfigMap
func (n *NodeReference) ToData() map[string]string {
	return map[string]string{
		"key":   n.key,
		"value": base64.StdEncoding.EncodeToString([]byte(n.value)),
	}
}

func getHash(parent string) string {
	hasher := fnv.New64a()
	hasher.Write([]byte(parent))
	return strconv.FormatUint(hasher.Sum64(), 10)
}

func (s *Server) newNodeReference(key string) *NodeReference {
	key = filepath.Join(s.root, key)

	node := &NodeReference{
		id:  fmt.Sprintf("vt-%s", getHash(key)),
		key: key,
	}

	return node
}

func (s *Server) buildFileResource(filePath string, contents []byte) *corev1.ConfigMap {
	node := s.newNodeReference(filePath)

	// create data
	node.value = string(contents)

	// Create "file" object
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      node.id,
			Namespace: s.namespace,
		},
		Data: node.ToData(),
	}
}

// Create is part of the topo.Conn interface.
func (s *Server) Create(ctx context.Context, filePath string, contents []byte) (topo.Version, error) {
	log.V(7).Infof("Create at '%s' Contents: '%s'", filePath, string(contents))

	resource := s.buildFileResource(filePath, contents)

	final, err := s.resourceClient.Create(resource)
	if err != nil {
		return nil, convertError(err, filePath)
	}

	// Update the internal cache
	err = s.memberIndexer.Update(final)
	if err != nil {
		return nil, convertError(err, filePath)
	}

	return KubernetesVersion(final.GetResourceVersion()), nil
}

// Update is part of the topo.Conn interface.
func (s *Server) Update(ctx context.Context, filePath string, contents []byte, version topo.Version) (topo.Version, error) {
	log.V(7).Infof("Update at '%s' Contents: '%s'", filePath, string(contents))

	resource := s.buildFileResource(filePath, contents)

	result, err := s.resourceClient.Get(resource.Name, metav1.GetOptions{})
	if err != nil && errors.IsNotFound(err) && version == nil {
		// Update should create objects when the version is nil and the object is not found
		result, err := s.resourceClient.Create(resource)
		if err != nil {
			return nil, convertError(err, filePath)
		}
		return KubernetesVersion(result.GetResourceVersion()), nil
	}

	// If a non-nil version is given to update, fail on mismatched version
	if version != nil && KubernetesVersion(result.GetResourceVersion()) != version {
		return nil, topo.NewError(topo.BadVersion, filePath)
	}

	// set new contents
	result.Data["value"] = resource.Data["value"]

	// get result or err
	final, err := s.resourceClient.Update(result)
	if err != nil {
		return nil, convertError(err, filePath)
	}

	// Update the internal cache
	err = s.memberIndexer.Update(final)
	if err != nil {
		return nil, convertError(err, filePath)
	}

	return KubernetesVersion(final.GetResourceVersion()), nil
}

// Get is part of the topo.Conn interface.
func (s *Server) Get(ctx context.Context, filePath string) ([]byte, topo.Version, error) {
	log.V(7).Infof("Get at '%s'", filePath)

	node := s.newNodeReference(filePath)

	result, err := s.resourceClient.Get(node.id, metav1.GetOptions{})
	if err != nil {
		return []byte{}, nil, convertError(err, filePath)
	}

	c, hasContents := result.Data["value"]
	if !hasContents {
		return []byte{}, nil, convertError(fmt.Errorf("object found but has no contents"), filePath)
	}

	out, err := base64.StdEncoding.DecodeString(c)
	if err != nil {
		return []byte{}, nil, convertError(fmt.Errorf("unable to decode object contents"), filePath)
	}

	return out, KubernetesVersion(result.GetResourceVersion()), nil
}

// Delete is part of the topo.Conn interface.
func (s *Server) Delete(ctx context.Context, filePath string, version topo.Version) error {
	log.V(7).Infof("Delete at '%s'", filePath)

	node := s.newNodeReference(filePath)

	// Check version before delete
	current, err := s.resourceClient.Get(node.id, metav1.GetOptions{})
	if err != nil {
		return convertError(err, filePath)
	}
	if version != nil {
		if KubernetesVersion(current.GetResourceVersion()) != version {
			return topo.NewError(topo.BadVersion, filePath)
		}
	}

	err = s.resourceClient.Delete(node.id, &metav1.DeleteOptions{})
	if err != nil {
		return convertError(err, filePath)
	}

	// Wait for one of the following conditions
	// 1. Context is cancelled
	// 2. The object is no longer in the cache
	// 3. The object in the cache has a new uid (was deleted but recreated since we last checked)
	for {
		select {
		case <-ctx.Done():
			return convertError(ctx.Err(), filePath)
		case <-time.After(50 * time.Millisecond):
		}

		obj, ok, err := s.memberIndexer.Get(current)
		if err != nil { // error getting from cache
			return convertError(err, filePath)
		}
		if !ok { // deleted from cache
			break
		}
		cached := obj.(*corev1.ConfigMap)
		if cached.GetUID() != current.GetUID() {
			break // deleted and recreated
		}
	}

	return nil
}
