#!/bin/bash -ex

GEN_BASE_DIR=./go/vt/topo/kubernetestopo

# Generate the deepcopy types
deepcopy-gen -i ${GEN_BASE_DIR}/apis/topo/v1beta1 -O zz_generated.deepcopy --bounding-dirs ${GEN_BASE_DIR}/apis --go-header-file ${GEN_BASE_DIR}/boilerplate.go.txt

# Delete, generate, and move the clientset
# There is no way to get client-gen to automatically put files in the right place and still have the right import path
rm -rf go/vt/topo/kubernetestopo/client
client-gen --clientset-name versioned --input-base '' -i ${GEN_BASE_DIR}/apis/topo/v1beta1 -o ./ --output-package go/vt/topo/kubernetestopo/client/clientset --go-header-file go/vt/topo/kubernetestopo/apis/topo/boilerplate.go.txt
mv vitess.io/vitess/go/vt/topo/kubernetestopo/client go/vt/topo/kubernetestopo/

lister-gen --input-dirs ${GEN_BASE_DIR}/apis/topo/v1beta1 --output-package vitess.io/vitess/go/vt/topo/kubernetestopo/client/listers --go-header-file go/vt/topo/kubernetestopo/apis/topo/boilerplate.go.txt
informer-gen --input-dirs ${GEN_BASE_DIR}/apis/topo/v1beta1 --versioned-clientset-package vitess.io/vitess/go/vt/topo/kubernetestopo/client/clientset/versioned --listers-package vitess.io/vitess/go/vt/topo/kubernetestopo/client/listers --output-package vitess.io/vitess/go/vt/topo/kubernetestopo/client/informers --go-header-file go/vt/topo/kubernetestopo/apis/topo/boilerplate.go.txt
