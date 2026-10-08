package capacitytemplate

import (
	"context"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/event"
)

const (
	testNodeName = "ip-10-0-0-1"
	otherTaint   = "node"
	localSC      = "ephemeral-local-data"
)

func taintedNode() *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: testNodeName},
		Spec: corev1.NodeSpec{Taints: []corev1.Taint{
			{Key: otherTaint, Value: "storage-workers", Effect: corev1.TaintEffectNoSchedule},
			{Key: DefaultStartupTaintKey, Value: "true", Effect: corev1.TaintEffectNoSchedule},
		}},
	}
}

// realCapacity mimics an external-provisioner per-node object; an empty value
// leaves capacity unset.
func realCapacity(name, storageClass, nodeName, value string) *storagev1.CSIStorageCapacity {
	c := &storagev1.CSIStorageCapacity{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "kube-system",
			Name:      name,
			Labels: map[string]string{
				"csi.storage.k8s.io/drivername": "topolvm.io",
				"csi.storage.k8s.io/managed-by": "external-provisioner",
			},
		},
		StorageClassName: storageClass,
		NodeTopology: &metav1.LabelSelector{MatchLabels: map[string]string{
			TopologyNodeKey: nodeName,
		}},
	}
	if value != "" {
		q := resource.MustParse(value)
		c.Capacity = &q
	}
	return c
}

// templateCapacity builds the object the NodeGroup reconciler would publish.
func templateCapacity() *storagev1.CSIStorageCapacity {
	ng := testNodeGroup("storage-workers", "uid-1")
	return (&Reconciler{}).desiredCapacity(ng, remoteTemplate, quantity("49Gi"))
}

type taintFixture struct {
	client   client.WithWatch
	patches  []string
	nodeGets int
}

func newTaintFixture(t *testing.T, objects ...client.Object) *taintFixture {
	t.Helper()
	f := &taintFixture{}
	f.client = fake.NewClientBuilder().
		WithScheme(testScheme(t)).
		WithIndex(&storagev1.CSIStorageCapacity{}, CapacityNodeIndex, IndexCapacityNode).
		WithObjects(objects...).
		WithInterceptorFuncs(interceptor.Funcs{
			Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*corev1.Node); ok {
					f.nodeGets++
				}
				return c.Get(ctx, key, obj, opts...)
			},
			Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
				data, err := patch.Data(obj)
				if err != nil {
					return err
				}
				f.patches = append(f.patches, string(data))
				return c.Patch(ctx, obj, patch, opts...)
			},
		}).
		Build()
	return f
}

func (f *taintFixture) reconciler(t *testing.T, storageClasses ...string) *StartupTaintReconciler {
	t.Helper()
	if len(storageClasses) == 0 {
		storageClasses = []string{remoteTemplate.StorageClass}
	}
	r, err := NewStartupTaintReconciler(f.client, f.client, nil, DefaultStartupTaintKey, storageClasses)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func reconcileNode(t *testing.T, r *StartupTaintReconciler) {
	t.Helper()
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: types.NamespacedName{Name: testNodeName}})
	if err != nil {
		t.Fatalf("Reconcile: %v", err)
	}
	if result.RequeueAfter != 0 {
		t.Fatalf("unexpected requeue %v; waiting is event-driven", result.RequeueAfter)
	}
}

func (f *taintFixture) taintKeys(t *testing.T) []string {
	t.Helper()
	node := &corev1.Node{}
	if err := f.client.Get(context.Background(), types.NamespacedName{Name: testNodeName}, node); err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(node.Spec.Taints))
	for _, taint := range node.Spec.Taints {
		keys = append(keys, taint.Key)
	}
	return keys
}

func (f *taintFixture) assertTainted(t *testing.T) {
	t.Helper()
	if keys := f.taintKeys(t); !reflect.DeepEqual(keys, []string{otherTaint, DefaultStartupTaintKey}) {
		t.Fatalf("taints = %v, want startup taint kept", keys)
	}
}

func (f *taintFixture) assertUntainted(t *testing.T) {
	t.Helper()
	if keys := f.taintKeys(t); !reflect.DeepEqual(keys, []string{otherTaint}) {
		t.Fatalf("taints = %v, want only the unrelated taint", keys)
	}
}

func TestStartupTaintWaitsForPositiveRealCapacity(t *testing.T) {
	sc := remoteTemplate.StorageClass
	tests := []struct {
		name     string
		objects  []client.Object
		wantWait bool
	}{
		{name: "no real capacity yet", wantWait: true},
		{
			name:     "capacity published as zero before topolvm-node annotates the node",
			objects:  []client.Object{realCapacity("csisc-zero", sc, testNodeName, "0")},
			wantWait: true,
		},
		{
			name:     "capacity without a value",
			objects:  []client.Object{realCapacity("csisc-nil", sc, testNodeName, "")},
			wantWait: true,
		},
		{
			name:     "positive capacity for a different node",
			objects:  []client.Object{realCapacity("csisc-other", sc, "ip-10-0-0-2", "49Gi")},
			wantWait: true,
		},
		{
			// Embedded lvmd starts without every volume group, so local
			// capacity can be positive while the remote VG is not ready.
			name:     "only local capacity is positive",
			objects:  []client.Object{realCapacity("csisc-local", localSC, testNodeName, "100Gi")},
			wantWait: true,
		},
		{
			name: "only a template capacity names the node",
			objects: func() []client.Object {
				c := templateCapacity()
				c.NodeTopology.MatchLabels[TopologyNodeKey] = testNodeName
				return []client.Object{c}
			}(),
			wantWait: true,
		},
		{
			name:     "positive capacity for the configured storage class",
			objects:  []client.Object{realCapacity("csisc-ready", sc, testNodeName, "49Gi")},
			wantWait: false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			f := newTaintFixture(t, append([]client.Object{taintedNode()}, tc.objects...)...)
			reconcileNode(t, f.reconciler(t))
			if tc.wantWait {
				if f.nodeGets != 0 {
					t.Fatalf("read the Node %d times while capacity is pending", f.nodeGets)
				}
				f.assertTainted(t)
				return
			}
			f.assertUntainted(t)
		})
	}
}

func TestStartupTaintWaitsForEveryConfiguredStorageClass(t *testing.T) {
	f := newTaintFixture(t, taintedNode(),
		realCapacity("csisc-a", remoteTemplate.StorageClass, testNodeName, "49Gi"))
	r := f.reconciler(t, remoteTemplate.StorageClass, remoteXFSTemplate.StorageClass)

	reconcileNode(t, r)
	f.assertTainted(t)

	if err := f.client.Create(context.Background(),
		realCapacity("csisc-b", remoteXFSTemplate.StorageClass, testNodeName, "49Gi")); err != nil {
		t.Fatal(err)
	}
	reconcileNode(t, r)
	f.assertUntainted(t)
}

func TestStartupTaintIgnoresUntaintedAndMissingNodes(t *testing.T) {
	node := taintedNode()
	node.Spec.Taints = node.Spec.Taints[:1]
	f := newTaintFixture(t, node, realCapacity("csisc-ready", remoteTemplate.StorageClass, testNodeName, "49Gi"),
		realCapacity("csisc-gone", remoteTemplate.StorageClass, "missing-node", "49Gi"))
	r := f.reconciler(t)

	reconcileNode(t, r)
	if _, err := r.Reconcile(context.Background(),
		ctrl.Request{NamespacedName: types.NamespacedName{Name: "missing-node"}}); err != nil {
		t.Fatalf("missing node: %v", err)
	}
	if len(f.patches) != 0 {
		t.Fatalf("patched a node without the startup taint: %v", f.patches)
	}
}

func TestStartupTaintRemovalUsesOptimisticLock(t *testing.T) {
	f := newTaintFixture(t, taintedNode(),
		realCapacity("csisc-ready", remoteTemplate.StorageClass, testNodeName, "49Gi"))
	reconcileNode(t, f.reconciler(t))
	f.assertUntainted(t)
	if len(f.patches) != 1 || !strings.Contains(f.patches[0], `"resourceVersion"`) ||
		!strings.Contains(f.patches[0], `"taints"`) {
		t.Fatalf("patches = %v, want one patch carrying resourceVersion and taints", f.patches)
	}
}

func TestCapacityBecamePositivePredicate(t *testing.T) {
	r, err := NewStartupTaintReconciler(nil, nil, nil, DefaultStartupTaintKey, []string{remoteTemplate.StorageClass})
	if err != nil {
		t.Fatal(err)
	}
	p := r.capacityBecamePositive()
	sc := remoteTemplate.StorageClass
	zero := realCapacity("c", sc, testNodeName, "0")
	unset := realCapacity("c", sc, testNodeName, "")
	small := realCapacity("c", sc, testNodeName, "1Gi")
	large := realCapacity("c", sc, testNodeName, "49Gi")
	local := realCapacity("c", localSC, testNodeName, "49Gi")
	template := templateCapacity()

	creates := []struct {
		name   string
		object *storagev1.CSIStorageCapacity
		want   bool
	}{
		{name: "positive configured", object: large, want: true},
		{name: "zero configured", object: zero},
		{name: "positive unconfigured storage class", object: local},
		{name: "template capacity", object: template},
	}
	for _, tc := range creates {
		if got := p.Create(event.CreateEvent{Object: tc.object}); got != tc.want {
			t.Errorf("Create(%s) = %v, want %v", tc.name, got, tc.want)
		}
	}

	updates := []struct {
		name     string
		old, new *storagev1.CSIStorageCapacity
		want     bool
	}{
		{name: "zero to positive", old: zero, new: large, want: true},
		{name: "unset to positive", old: unset, new: large, want: true},
		// Volume provisioning changes capacity constantly; these must not
		// cause Node reads.
		{name: "positive to positive", old: large, new: small},
		{name: "resync of positive", old: large, new: large},
		{name: "positive to zero", old: large, new: zero},
	}
	for _, tc := range updates {
		if got := p.Update(event.UpdateEvent{ObjectOld: tc.old, ObjectNew: tc.new}); got != tc.want {
			t.Errorf("Update(%s) = %v, want %v", tc.name, got, tc.want)
		}
	}
	if p.Delete(event.DeleteEvent{Object: large}) || p.Generic(event.GenericEvent{Object: large}) {
		t.Error("delete and generic events must be ignored")
	}
}

func TestRequestsForRealCapacity(t *testing.T) {
	noTopology := realCapacity("no-topology", "sc", testNodeName, "1Gi")
	noTopology.NodeTopology = nil

	tests := []struct {
		name   string
		object client.Object
		want   []string
	}{
		{name: "real per-node capacity", object: realCapacity("real", "sc", testNodeName, "1Gi"), want: []string{testNodeName}},
		{name: "template capacity", object: templateCapacity()},
		{name: "no topology", object: noTopology},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var got []string
			for _, req := range requestsForRealCapacity(context.Background(), tc.object) {
				got = append(got, req.Name)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("requests = %v, want %v", got, tc.want)
			}
			if index := IndexCapacityNode(tc.object); !reflect.DeepEqual(index, tc.want) {
				t.Fatalf("index = %v, want %v", index, tc.want)
			}
		})
	}
}

func TestNewStartupTaintReconcilerValidation(t *testing.T) {
	if err := ValidateStartupTaintKey(DefaultStartupTaintKey); err != nil {
		t.Fatalf("default key rejected: %v", err)
	}
	for _, key := range []string{"", "Bad Key", "a/b/c"} {
		if err := ValidateStartupTaintKey(key); err == nil {
			t.Fatalf("ValidateStartupTaintKey(%q) accepted an invalid key", key)
		}
	}
	if _, err := NewStartupTaintReconciler(nil, nil, nil, "Bad Key", []string{"sc"}); err == nil {
		t.Fatal("accepted an invalid key")
	}
	if _, err := NewStartupTaintReconciler(nil, nil, nil, DefaultStartupTaintKey, nil); err == nil {
		t.Fatal("accepted no storage classes")
	}
	r, err := NewStartupTaintReconciler(nil, nil, nil, DefaultStartupTaintKey, []string{"b", "a", "b"})
	if err != nil || !reflect.DeepEqual(r.storageClasses, []string{"a", "b"}) {
		t.Fatalf("storage classes = %v, %v; want deduplicated and sorted", r.storageClasses, err)
	}
}
