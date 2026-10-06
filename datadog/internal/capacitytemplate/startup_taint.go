package capacitytemplate

import (
	"context"
	"fmt"
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

const (
	// DefaultStartupTaintKey is the Node taint the remover clears once real
	// TopoLVM capacity is published. The key carries Cluster Autoscaler's
	// built-in startup-taint prefix, so CA strips it from template Nodes and,
	// while it is present on a real Node, treats that Node as not yet started.
	// A not-started Node is replaced in CA's simulation by a template Node,
	// which matches the template-only CSIStorageCapacity objects. Without the
	// taint, a freshly Ready Node with no per-node capacity yet makes pending
	// Pods look unschedulable and triggers an extra scale-up.
	DefaultStartupTaintKey = "startup-taint.cluster-autoscaler.kubernetes.io/topolvm-capacity-not-ready"

	// CapacityNodeIndex indexes real (not controller-managed) per-node
	// CSIStorageCapacity objects by the Node named in TopoLVM's topology key.
	CapacityNodeIndex = "topolvmNode"
)

// IndexCapacityNode extracts the CapacityNodeIndex key.
func IndexCapacityNode(object client.Object) []string {
	capacity, ok := object.(*storagev1.CSIStorageCapacity)
	if !ok {
		return nil
	}
	if node := realCapacityNode(capacity); node != "" {
		return []string{node}
	}
	return nil
}

// realCapacityNode returns the Node a real per-node TopoLVM capacity belongs
// to, or "" for template capacities and objects without TopoLVM topology.
func realCapacityNode(capacity *storagev1.CSIStorageCapacity) string {
	if capacity.Labels[ManagedByLabel] == ManagedByValue || capacity.NodeTopology == nil {
		return ""
	}
	return capacity.NodeTopology.MatchLabels[TopologyNodeKey]
}

func positiveCapacity(capacity *storagev1.CSIStorageCapacity) bool {
	return capacity.Capacity != nil && capacity.Capacity.Sign() > 0
}

// StartupTaintReconciler removes a startup taint from a Node once every
// configured template StorageClass has a real, positive, per-node
// CSIStorageCapacity for that Node.
//
// It is driven only by CSIStorageCapacity events and does not watch Nodes:
// a capacity becoming positive enqueues the Node named in its TopoLVM
// topology, which is then read individually. This keeps Node objects out of
// memory and limits Node reads to capacity transitions.
//
// The configured StorageClasses are required explicitly because TopoLVM's
// embedded lvmd does not require every volume group to exist at startup: a
// Node can report positive local capacity while its remote volume group is
// still being prepared.
type StartupTaintReconciler struct {
	client         client.Client
	nodeReader     client.Reader
	recorder       events.EventRecorder
	taintKey       string
	storageClasses []string
	watched        map[string]struct{}
}

// NewStartupTaintReconciler validates its inputs and returns a reconciler.
// nodeReader must not be backed by an informer cache, or a Node watch would be
// started implicitly; use the manager's API reader.
func NewStartupTaintReconciler(
	c client.Client,
	nodeReader client.Reader,
	recorder events.EventRecorder,
	taintKey string,
	storageClasses []string,
) (*StartupTaintReconciler, error) {
	if err := ValidateStartupTaintKey(taintKey); err != nil {
		return nil, err
	}
	if len(storageClasses) == 0 {
		return nil, fmt.Errorf("startup-taint controller needs at least one StorageClass")
	}
	r := &StartupTaintReconciler{
		client:     c,
		nodeReader: nodeReader,
		recorder:   recorder,
		taintKey:   taintKey,
		watched:    make(map[string]struct{}, len(storageClasses)),
	}
	for _, storageClass := range storageClasses {
		if _, duplicate := r.watched[storageClass]; !duplicate {
			r.watched[storageClass] = struct{}{}
			r.storageClasses = append(r.storageClasses, storageClass)
		}
	}
	sort.Strings(r.storageClasses)
	return r, nil
}

// ValidateStartupTaintKey rejects keys that are not valid taint keys.
func ValidateStartupTaintKey(key string) error {
	if errs := validation.IsQualifiedName(key); len(errs) != 0 {
		return fmt.Errorf("invalid startup taint key %q: %v", key, errs)
	}
	return nil
}

// Reconcile removes the startup taint from one Node once it is safe to do so.
// Waiting needs no requeue: the next capacity transition for the Node
// enqueues it again.
func (r *StartupTaintReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := ctrl.LoggerFrom(ctx)

	pending, err := r.pendingStorageClasses(ctx, req.Name)
	if err != nil {
		return ctrl.Result{}, err
	}
	if len(pending) != 0 {
		logger.V(1).Info("waiting for per-node TopoLVM capacity", "pendingStorageClasses", pending)
		return ctrl.Result{}, nil
	}

	// resourceVersion=0 is served from the API server's watch cache rather
	// than etcd. A stale read is safe: the patch below carries the
	// resourceVersion, so it fails with a conflict and is retried.
	node := &corev1.Node{}
	if err := r.nodeReader.Get(ctx, req.NamespacedName, node,
		&client.GetOptions{Raw: &metav1.GetOptions{ResourceVersion: "0"}}); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	if !r.hasTaint(node) {
		return ctrl.Result{}, nil
	}
	if err := r.removeTaint(ctx, node); err != nil {
		return ctrl.Result{}, err
	}

	logger.Info("removed startup taint",
		"taint", r.taintKey,
		"storageClasses", r.storageClasses,
		"nodeAge", time.Since(node.CreationTimestamp.Time).Round(time.Second).String())
	if r.recorder != nil {
		r.recorder.Eventf(node, nil, corev1.EventTypeNormal, "StartupTaintRemoved", "RemoveStartupTaint",
			"removed %s; per-node TopoLVM capacity published for storage classes %v", r.taintKey, r.storageClasses)
	}
	return ctrl.Result{}, nil
}

// pendingStorageClasses returns the configured StorageClasses that do not yet
// have positive real capacity for the Node, using the cached capacities.
func (r *StartupTaintReconciler) pendingStorageClasses(ctx context.Context, nodeName string) ([]string, error) {
	capacities := &storagev1.CSIStorageCapacityList{}
	if err := r.client.List(ctx, capacities, client.MatchingFields{CapacityNodeIndex: nodeName}); err != nil {
		return nil, err
	}
	ready := make(map[string]bool, len(r.storageClasses))
	for i := range capacities.Items {
		if positiveCapacity(&capacities.Items[i]) {
			ready[capacities.Items[i].StorageClassName] = true
		}
	}
	var pending []string
	for _, storageClass := range r.storageClasses {
		if !ready[storageClass] {
			pending = append(pending, storageClass)
		}
	}
	return pending, nil
}

func (r *StartupTaintReconciler) hasTaint(node *corev1.Node) bool {
	for _, taint := range node.Spec.Taints {
		if taint.Key == r.taintKey {
			return true
		}
	}
	return false
}

// removeTaint drops every taint with the configured key. Spec.Taints is an
// atomic list for merge patches, so the patch carries the resourceVersion to
// avoid overwriting concurrent taint changes; conflicts are retried.
func (r *StartupTaintReconciler) removeTaint(ctx context.Context, node *corev1.Node) error {
	updated := node.DeepCopy()
	updated.Spec.Taints = updated.Spec.Taints[:0]
	for _, taint := range node.Spec.Taints {
		if taint.Key != r.taintKey {
			updated.Spec.Taints = append(updated.Spec.Taints, taint)
		}
	}
	return r.client.Patch(ctx, updated, client.MergeFromWithOptions(node, client.MergeFromWithOptimisticLock{}))
}

// relevant reports whether a capacity is a real per-node capacity for one of
// the configured StorageClasses.
func (r *StartupTaintReconciler) relevant(capacity *storagev1.CSIStorageCapacity) bool {
	_, watched := r.watched[capacity.StorageClassName]
	return watched && realCapacityNode(capacity) != ""
}

// capacityBecamePositive admits only events that can unblock a Node: a
// relevant capacity created positive (including the initial list after a
// restart) or updated from non-positive to positive. Ordinary capacity
// changes caused by volume provisioning never trigger a Node read.
func (r *StartupTaintReconciler) capacityBecamePositive() predicate.Funcs {
	return predicate.Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			capacity, ok := e.Object.(*storagev1.CSIStorageCapacity)
			return ok && r.relevant(capacity) && positiveCapacity(capacity)
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			oldCapacity, okOld := e.ObjectOld.(*storagev1.CSIStorageCapacity)
			newCapacity, okNew := e.ObjectNew.(*storagev1.CSIStorageCapacity)
			return okOld && okNew && r.relevant(newCapacity) &&
				!positiveCapacity(oldCapacity) && positiveCapacity(newCapacity)
		},
		DeleteFunc:  func(event.DeleteEvent) bool { return false },
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}

// SetupWithManager registers the capacity index and the capacity watch.
func (r *StartupTaintReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(
		context.Background(),
		&storagev1.CSIStorageCapacity{},
		CapacityNodeIndex,
		IndexCapacityNode,
	); err != nil {
		return err
	}
	return ctrl.NewControllerManagedBy(mgr).
		Named("datadog-topolvm-startup-taint-controller").
		Watches(
			&storagev1.CSIStorageCapacity{},
			handler.EnqueueRequestsFromMapFunc(requestsForRealCapacity),
			builder.WithPredicates(r.capacityBecamePositive()),
		).
		Complete(r)
}

func requestsForRealCapacity(_ context.Context, object client.Object) []reconcile.Request {
	capacity, ok := object.(*storagev1.CSIStorageCapacity)
	if !ok {
		return nil
	}
	nodeName := realCapacityNode(capacity)
	if nodeName == "" {
		return nil
	}
	return []reconcile.Request{{NamespacedName: types.NamespacedName{Name: nodeName}}}
}
