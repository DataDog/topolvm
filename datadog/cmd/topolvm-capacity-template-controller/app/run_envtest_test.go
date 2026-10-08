package app

import (
	"context"
	"errors"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/DataDog/topolvm/datadog/internal/capacitytemplate"
	corev1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
)

const (
	envNamespace    = "ea-test"
	envNodeGroup    = "remote-volume-test"
	envStorageClass = "ephemeral-remote-data"
	envLocalSC      = "ephemeral-local-data"
	envDeviceClass  = "remote-ssd"
	envFreshNode    = "ip-10-128-70-122"
)

// nodeGroupCRD is a schemaless stand-in for the Datadog NodeGroup CRD; the
// controller only reads it as unstructured.
func nodeGroupCRD() *apiextensionsv1.CustomResourceDefinition {
	return &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "nodegroups.datadoghq.com"},
		Spec: apiextensionsv1.CustomResourceDefinitionSpec{
			Group: "datadoghq.com",
			Scope: apiextensionsv1.NamespaceScoped,
			Names: apiextensionsv1.CustomResourceDefinitionNames{
				Plural: "nodegroups", Singular: "nodegroup", Kind: "NodeGroup", ListKind: "NodeGroupList",
			},
			Versions: []apiextensionsv1.CustomResourceDefinitionVersion{{
				Name: "v1", Served: true, Storage: true,
				Schema: &apiextensionsv1.CustomResourceValidation{
					OpenAPIV3Schema: &apiextensionsv1.JSONSchemaProps{
						Type:                   "object",
						XPreserveUnknownFields: ptr.To(true),
					},
				},
			}},
		},
	}
}

// startEnv starts an API server and returns a direct client, so assertions
// observe the API server rather than the manager's cache.
func startEnv(t *testing.T) (client.Client, *rest.Config) {
	t.Helper()
	if os.Getenv("KUBEBUILDER_ASSETS") == "" && os.Getenv("ENVTEST_ASSETS_DIR") == "" {
		t.Skip("envtest assets not configured; set KUBEBUILDER_ASSETS or ENVTEST_ASSETS_DIR")
	}
	logf.SetLogger(zap.New(zap.WriteTo(os.Stderr), zap.UseDevMode(true)))

	version := os.Getenv("ENVTEST_KUBERNETES_VERSION")
	env := &envtest.Environment{
		CRDs:                  []*apiextensionsv1.CustomResourceDefinition{nodeGroupCRD()},
		DownloadBinaryAssets:  version != "",
		BinaryAssetsDirectory: os.Getenv("ENVTEST_ASSETS_DIR"),
	}
	if version != "" {
		env.DownloadBinaryAssetsVersion = "v" + version
	}
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	t.Cleanup(func() { _ = env.Stop() })

	c, err := client.New(cfg, client.Options{Scheme: clientgoscheme.Scheme})
	if err != nil {
		t.Fatal(err)
	}
	return c, cfg
}

// requestLog records every API request the manager sends, as
// "<METHOD> <path>?<query>".
type requestLog struct {
	mu       sync.Mutex
	requests []string
}

func (l *requestLog) RoundTrip(next http.RoundTripper) http.RoundTripper {
	return roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		l.mu.Lock()
		l.requests = append(l.requests, req.Method+" "+req.URL.Path+"?"+req.URL.RawQuery)
		l.mu.Unlock()
		return next.RoundTrip(req)
	})
}

// nodeRequests returns the recorded requests for core/v1 Nodes.
func (l *requestLog) nodeRequests() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []string
	for _, r := range l.requests {
		if strings.Contains(r, "/api/v1/nodes") {
			out = append(out, r)
		}
	}
	return out
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

// startManager runs the production manager wiring against the API server and
// records the requests it makes.
func startManager(t *testing.T, cfg *rest.Config) (ctrl.Manager, *requestLog) {
	t.Helper()
	log := &requestLog{}
	cfg = rest.CopyConfig(cfg)
	cfg.Wrap(log.RoundTrip)
	opts := validOptions()
	opts.LeaderElect = false
	opts.MetricsBindAddress = "0"
	opts.HealthProbeBindAddress = "0"
	mgr, err := newManager(cfg, opts, capacitytemplate.Config{Templates: []capacitytemplate.TemplateConfig{{
		StorageClass: envStorageClass,
		DeviceClass:  envDeviceClass,
		Handler:      capacitytemplate.HandlerRemoteLVM,
	}}}, func(o *ctrl.Options) {
		// Several managers run in one test binary.
		o.Controller.SkipNameValidation = ptr.To(true)
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- mgr.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("manager: %v", err)
		}
	})
	return mgr, log
}

// assertSingleNodeRead checks the manager never listed or watched Nodes and
// only read the fresh Node individually from the watch cache, then patched it.
func assertSingleNodeRead(t *testing.T, log *requestLog) {
	t.Helper()
	requests := log.nodeRequests()
	if len(requests) == 0 {
		t.Fatal("no Node requests recorded; the request log is not wired")
	}
	get := "GET /api/v1/nodes/" + envFreshNode + "?resourceVersion=0"
	patch := "PATCH /api/v1/nodes/" + envFreshNode + "?"
	for _, r := range requests {
		if r != get && r != patch {
			t.Fatalf("unexpected Node request %q; all Node requests: %v", r, requests)
		}
	}
	t.Logf("all Node requests made by the manager: %v", requests)
}

func mustCreate(t *testing.T, c client.Client, objects ...client.Object) {
	t.Helper()
	for _, object := range objects {
		if err := c.Create(context.Background(), object); err != nil {
			t.Fatalf("create %T %s: %v", object, object.GetName(), err)
		}
	}
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", timeout, what)
}

func consistently(t *testing.T, duration time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		if !cond() {
			t.Fatalf("%s did not hold", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func hasStartupTaint(t *testing.T, c client.Client) bool {
	t.Helper()
	node := &corev1.Node{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: envFreshNode}, node); err != nil {
		t.Fatalf("get node: %v", err)
	}
	for _, taint := range node.Spec.Taints {
		if taint.Key == capacitytemplate.DefaultStartupTaintKey {
			return true
		}
	}
	return false
}

// realCapacity mimics an external-provisioner per-node object.
func realCapacity(storageClass string, bytes int64) *storagev1.CSIStorageCapacity {
	return &storagev1.CSIStorageCapacity{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:    "kube-system",
			GenerateName: "csisc-",
			Labels: map[string]string{
				"csi.storage.k8s.io/drivername": "topolvm.io",
				"csi.storage.k8s.io/managed-by": "external-provisioner",
			},
		},
		StorageClassName: storageClass,
		NodeTopology: &metav1.LabelSelector{MatchLabels: map[string]string{
			capacitytemplate.TopologyNodeKey: envFreshNode,
		}},
		Capacity: resource.NewQuantity(bytes, resource.BinarySI),
	}
}

func freshNode() *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: envFreshNode, Labels: map[string]string{
			"kubernetes.io/hostname":                 envFreshNode,
			capacitytemplate.TopologyNodeKey:         envFreshNode,
			capacitytemplate.NodeGroupNamespaceLabel: envNamespace,
			capacitytemplate.NodeGroupNameLabel:      envNodeGroup,
		}},
		Spec: corev1.NodeSpec{Taints: []corev1.Taint{
			{Key: "node", Value: envNodeGroup, Effect: corev1.TaintEffectNoSchedule},
			{Key: capacitytemplate.DefaultStartupTaintKey, Value: "true", Effect: corev1.TaintEffectNoSchedule},
		}},
	}
}

// assertOnlyStartupTaintRemoved checks every other taint survived the patch,
// including node.kubernetes.io/not-ready added by the API server's
// TaintNodesByCondition admission on create.
func assertOnlyStartupTaintRemoved(t *testing.T, c client.Client) {
	t.Helper()
	node := &corev1.Node{}
	if err := c.Get(context.Background(), types.NamespacedName{Name: envFreshNode}, node); err != nil {
		t.Fatal(err)
	}
	keys := map[string]bool{}
	for _, taint := range node.Spec.Taints {
		keys[taint.Key] = true
	}
	if !keys["node"] || !keys[corev1.TaintNodeNotReady] || len(keys) != 2 {
		t.Fatalf("unrelated taints not preserved: %#v", node.Spec.Taints)
	}
}

// TestStartupTaintLifecycle replays the staging timeline: the NodeGroup gets a
// template capacity; a fresh Node registers with the startup taint; local
// capacity comes up first; external-provisioner publishes zero remote
// capacity, then real remote capacity. The taint must stay until positive
// remote capacity exists and then be removed by the capacity event, without a
// Node informer.
func TestStartupTaintLifecycle(t *testing.T) {
	c, cfg := startEnv(t)
	mgr, log := startManager(t, cfg)
	ctx := context.Background()

	ng := &unstructured.Unstructured{}
	ng.SetGroupVersionKind(capacitytemplate.NodeGroupGVK)
	ng.SetNamespace(envNamespace)
	ng.SetName(envNodeGroup)
	if err := unstructured.SetNestedSlice(ng.Object, []interface{}{
		map[string]interface{}{"name": "remote-data", "provisioningMode": "lvm", "size": "50Gi"},
	}, "spec", "storage", "remoteVolumes"); err != nil {
		t.Fatal(err)
	}
	ngClient, err := client.New(cfg, client.Options{Scheme: mgr.GetScheme()})
	if err != nil {
		t.Fatal(err)
	}
	mustCreate(t, ngClient,
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: envNamespace}},
		&storagev1.StorageClass{
			ObjectMeta:        metav1.ObjectMeta{Name: envStorageClass},
			Provisioner:       "topolvm.io",
			VolumeBindingMode: ptr.To(storagev1.VolumeBindingWaitForFirstConsumer),
			Parameters:        map[string]string{"topolvm.io/device-class": envDeviceClass},
		},
		ng,
	)
	// The capacity-template controller still works with
	// ReaderFailOnMissingInformer enabled.
	eventually(t, 10*time.Second, "template CSIStorageCapacity", func() bool {
		list := &storagev1.CSIStorageCapacityList{}
		if err := c.List(ctx, list, client.InNamespace(envNamespace),
			client.MatchingLabels{capacitytemplate.ManagedByLabel: capacitytemplate.ManagedByValue}); err != nil {
			t.Fatal(err)
		}
		return len(list.Items) == 1
	})

	mustCreate(t, c, freshNode(), realCapacity(envLocalSC, 100*(1<<30)))
	consistently(t, 2*time.Second, "taint kept with only local capacity", func() bool {
		return hasStartupTaint(t, c)
	})

	remote := realCapacity(envStorageClass, 0)
	mustCreate(t, c, remote)
	consistently(t, 2*time.Second, "taint kept with zero remote capacity", func() bool {
		return hasStartupTaint(t, c)
	})

	patched := remote.DeepCopy()
	patched.Capacity = resource.NewQuantity(49*(1<<30), resource.BinarySI)
	if err := c.Patch(ctx, patched, client.MergeFrom(remote)); err != nil {
		t.Fatal(err)
	}
	eventually(t, 5*time.Second, "startup taint removed", func() bool {
		return !hasStartupTaint(t, c)
	})
	assertOnlyStartupTaintRemoved(t, c)
	assertSingleNodeRead(t, log)

	// No Node informer exists: with ReaderFailOnMissingInformer the cache
	// refuses the read instead of starting a cluster-wide Node watch.
	err = mgr.GetCache().Get(ctx, types.NamespacedName{Name: envFreshNode}, &corev1.Node{})
	var notCached *cache.ErrResourceNotCached
	if !errors.As(err, &notCached) {
		t.Fatalf("cached Node read error = %v, want ErrResourceNotCached", err)
	}

	cached := &storagev1.CSIStorageCapacityList{}
	if err := mgr.GetCache().List(ctx, cached); err != nil || len(cached.Items) == 0 {
		t.Fatalf("list cached capacities: %d, %v", len(cached.Items), err)
	}
	for _, capacity := range cached.Items {
		if capacity.ManagedFields != nil {
			t.Fatalf("cached CSIStorageCapacity %s keeps managedFields", capacity.Name)
		}
	}
}

// TestStartupTaintRemovedForCapacityPublishedBeforeStart covers a controller
// restart or leader change: capacity became positive while no controller was
// running, so only the initial list's create event can release the Node.
func TestStartupTaintRemovedForCapacityPublishedBeforeStart(t *testing.T) {
	c, cfg := startEnv(t)
	mustCreate(t, c, freshNode(), realCapacity(envStorageClass, 49*(1<<30)))
	if !hasStartupTaint(t, c) {
		t.Fatal("precondition: node should start tainted")
	}

	_, log := startManager(t, cfg)
	eventually(t, 10*time.Second, "startup taint removed after start", func() bool {
		return !hasStartupTaint(t, c)
	})
	assertOnlyStartupTaintRemoved(t, c)
	assertSingleNodeRead(t, log)
}
