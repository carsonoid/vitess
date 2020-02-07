package v1beta1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

// +genclient
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// TestType is a top-level type. A client is created for it.
type TestType struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`
	// +optional
	Status TestTypeStatus `json:"status,omitempty"`
}

// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

// TestTypeList is a top-level list type. The client methods for lists are automatically created.
// You are not supposed to create a separated client for this one.
type TestTypeList struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []TestType `json:"items"`
}

type TestTypeStatus struct {
	Blah string
}

// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object

type ClusterTestTypeList struct {
	metav1.TypeMeta
	metav1.ListMeta
	Items []ClusterTestType
}

// +genclient
// +genclient:nonNamespaced
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
// +genclient:method=GetScale,verb=get,subresource=scale,result=k8s.io/kubernetes/pkg/apis/autoscaling.Scale
// +genclient:method=UpdateScale,verb=update,subresource=scale,input=k8s.io/kubernetes/pkg/apis/autoscaling.Scale,result=k8s.io/kubernetes/pkg/apis/autoscaling.Scale

type ClusterTestType struct {
	metav1.TypeMeta `json:",inline"`
	// +optional
	metav1.ObjectMeta `json:"metadata,omitempty"`
	// +optional
	Status ClusterTestTypeStatus `json:"status,omitempty"`
}

type ClusterTestTypeStatus struct {
	Blah string
}
