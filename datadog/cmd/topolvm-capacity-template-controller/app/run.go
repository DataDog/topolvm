package app

import (
	"fmt"

	"github.com/DataDog/topolvm/datadog/internal/capacitytemplate"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
)

func run(opts Options) error {
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts.Zap)))
	setupLog := ctrl.Log.WithName("setup").WithName("datadog-capacity-template")

	controllerConfig, err := loadControllerConfig(opts.ConfigFile)
	if err != nil {
		return err
	}

	restConfig, err := ctrl.GetConfig()
	if err != nil {
		return fmt.Errorf("get Kubernetes configuration: %w", err)
	}

	mgr, err := newManager(restConfig, opts, controllerConfig)
	if err != nil {
		return err
	}

	for i, template := range controllerConfig.Templates {
		setupLog.Info("configured capacity template",
			"index", i,
			"storageClass", template.StorageClass,
			"deviceClass", template.DeviceClass,
			"spareGB", template.SpareGB,
			"handler", template.Handler)
	}
	setupLog.Info("starting manager",
		"config", opts.ConfigFile,
		"templates", len(controllerConfig.Templates),
		"syncPeriod", opts.SyncPeriod,
		"startupTaint", opts.StartupTaintKey)
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		return fmt.Errorf("run manager: %w", err)
	}
	return nil
}

// newManager builds the manager and registers every controller, without
// starting it. Tests use overrides to adjust manager options.
func newManager(
	restConfig *rest.Config,
	opts Options,
	controllerConfig capacitytemplate.Config,
	overrides ...func(*ctrl.Options),
) (ctrl.Manager, error) {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	capacitytemplate.AddNodeGroupToScheme(scheme)

	mgrOptions := ctrl.Options{
		Scheme: scheme,
		// NodeGroups are watched as unstructured objects. Without this, every
		// NodeGroup Get/List bypasses the informer and hits the API server.
		Client: client.Options{Cache: &client.CacheOptions{Unstructured: true}},
		Cache: cache.Options{
			SyncPeriod:       &opts.SyncPeriod,
			DefaultTransform: cache.TransformStripManagedFields(),
			// Reading a type without an informer fails instead of silently
			// starting a cluster-wide watch. Nodes in particular are read only
			// through the API reader.
			ReaderFailOnMissingInformer: true,
		},
		Metrics: metricsserver.Options{
			BindAddress: opts.MetricsBindAddress,
		},
		HealthProbeBindAddress:  opts.HealthProbeBindAddress,
		LeaderElection:          opts.LeaderElect,
		LeaderElectionID:        opts.LeaderElectionID,
		LeaderElectionNamespace: opts.LeaderElectionNamespace,
	}
	for _, override := range overrides {
		override(&mgrOptions)
	}
	mgr, err := ctrl.NewManager(restConfig, mgrOptions)
	if err != nil {
		return nil, fmt.Errorf("create manager: %w", err)
	}

	reconciler, err := capacitytemplate.NewReconciler(
		mgr.GetClient(),
		mgr.GetEventRecorder("datadog-topolvm-capacity-template-controller"),
		controllerConfig,
	)
	if err != nil {
		return nil, fmt.Errorf("create capacity controller: %w", err)
	}
	if err := reconciler.SetupWithManager(mgr); err != nil {
		return nil, fmt.Errorf("set up capacity controller: %w", err)
	}
	if opts.StartupTaintKey != "" {
		storageClasses := make([]string, 0, len(controllerConfig.Templates))
		for _, template := range controllerConfig.Templates {
			storageClasses = append(storageClasses, template.StorageClass)
		}
		taintReconciler, err := capacitytemplate.NewStartupTaintReconciler(
			mgr.GetClient(),
			mgr.GetAPIReader(),
			mgr.GetEventRecorder("datadog-topolvm-startup-taint-controller"),
			opts.StartupTaintKey,
			storageClasses,
		)
		if err != nil {
			return nil, fmt.Errorf("create startup-taint controller: %w", err)
		}
		if err := taintReconciler.SetupWithManager(mgr); err != nil {
			return nil, fmt.Errorf("set up startup-taint controller: %w", err)
		}
	}
	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return nil, err
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return nil, err
	}
	return mgr, nil
}
