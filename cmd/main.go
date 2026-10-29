/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"net/http"
	"os"
	"strings"
	"time"

	// Import all Kubernetes client auth plugins (e.g. Azure, GCP, OIDC, etc.)
	// to ensure that exec-entrypoint and run can make use of them.
	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics/filters"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"

	apiengine "github.com/karavy/api-engine/pkg/apiengine"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
	"github.com/karavy/latch/internal/controller"
	"github.com/karavy/latch/internal/controller/apicall"
	"github.com/karavy/latch/internal/controller/terraform"
	// +kubebuilder:scaffold:imports
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	utilruntime.Must(dinovav1alpha1.AddToScheme(scheme))
	// +kubebuilder:scaffold:scheme
}

// nolint:gocyclo
func main() {
	var metricsAddr string
	var metricsCertPath, metricsCertName, metricsCertKey string
	var webhookCertPath, webhookCertName, webhookCertKey string
	var enableLeaderElection bool
	var probeAddr string
	var secureMetrics bool
	var enableHTTP2 bool
	var apiAllowedOrigins, apiOriginsConfigMap string
	var tfStateEndpoint, tfStateBucket, tfStateRegion string
	var apiCAFile string
	var apiWorkers, apiQueueSize int
	var apiMaxResponseBytes int64
	var allowMutableTerraformSources bool
	var tlsOpts []func(*tls.Config)
	flag.StringVar(&tfStateEndpoint, "tfstate-s3-endpoint", "",
		"URL of the S3-compatible storage that holds every Terraform state, for example http://minio.storage:9000. "+
			"Credentials come from TFSTATE_S3_ACCESS_KEY_ID and TFSTATE_S3_SECRET_ACCESS_KEY.")
	flag.StringVar(&tfStateBucket, "tfstate-s3-bucket", "", "Bucket of the Terraform state storage.")
	flag.StringVar(&tfStateRegion, "tfstate-s3-region", "us-east-1",
		"Region of the Terraform state storage (often arbitrary on-premises).")
	flag.BoolVar(&allowMutableTerraformSources, "allow-mutable-terraform-sources", false,
		"Let TerraformTemplates with spec.allowMutableSources read ConfigMaps that are not immutable, "+
			"so that Terraform files can be edited in place while debugging. Leave it off in production.")
	flag.StringVar(&apiAllowedOrigins, "apicall-allowed-origins", "",
		"Comma-separated HTTP(S) origins allowed for ApiCall; empty denies all destinations.")
	flag.StringVar(&apiOriginsConfigMap, "apicall-allowed-origins-configmap", "",
		"Optional namespace/name of a ConfigMap whose \"origins\" key lists more allowed origins, one per line "+
			"or comma-separated, added to --apicall-allowed-origins. "+
			"It is read on every request, so edits apply without a restart.")
	flag.StringVar(&apiCAFile, "apicall-ca-file", "",
		"Optional PEM CA bundle to trust for ApiCall HTTPS endpoints, in addition to system roots.")
	flag.IntVar(&apiWorkers, "apicall-workers", 4, "Maximum concurrent ApiCall HTTP requests (1-64).")
	flag.IntVar(&apiQueueSize, "apicall-queue-size", 64, "Maximum queued ApiCall operations (1-4096).")
	flag.Int64Var(&apiMaxResponseBytes, "apicall-max-response-bytes", 4<<20,
		"Maximum bytes read from an ApiCall response. Only the values a Workflow step reads, at most 64 KiB, are stored.")
	flag.StringVar(&metricsAddr, "metrics-bind-address", "0", "The address the metrics endpoint binds to. "+
		"Use :8443 for HTTPS or :8080 for HTTP, or leave as 0 to disable the metrics service.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "The address the probe endpoint binds to.")
	flag.BoolVar(&enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager. "+
			"Enabling this will ensure there is only one active controller manager.")
	flag.BoolVar(&secureMetrics, "metrics-secure", true,
		"If set, the metrics endpoint is served securely via HTTPS. Use --metrics-secure=false to use HTTP instead.")
	flag.StringVar(&webhookCertPath, "webhook-cert-path", "", "The directory that contains the webhook certificate.")
	flag.StringVar(&webhookCertName, "webhook-cert-name", "tls.crt", "The name of the webhook certificate file.")
	flag.StringVar(&webhookCertKey, "webhook-cert-key", "tls.key", "The name of the webhook key file.")
	flag.StringVar(&metricsCertPath, "metrics-cert-path", "",
		"The directory that contains the metrics server certificate.")
	flag.StringVar(&metricsCertName, "metrics-cert-name", "tls.crt", "The name of the metrics server certificate file.")
	flag.StringVar(&metricsCertKey, "metrics-cert-key", "tls.key", "The name of the metrics server key file.")
	flag.BoolVar(&enableHTTP2, "enable-http2", false,
		"If set, HTTP/2 will be enabled for the metrics and webhook servers")
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	// if the enable-http2 flag is false (the default), http/2 should be disabled
	// due to its vulnerabilities. More specifically, disabling http/2 will
	// prevent from being vulnerable to the HTTP/2 Stream Cancellation and
	// Rapid Reset CVEs. For more information see:
	// - https://github.com/advisories/GHSA-qppj-fm5r-hxr3
	// - https://github.com/advisories/GHSA-4374-p667-p6c8
	disableHTTP2 := func(c *tls.Config) {
		setupLog.Info("Disabling HTTP/2")
		c.NextProtos = []string{"http/1.1"}
	}

	if !enableHTTP2 {
		tlsOpts = append(tlsOpts, disableHTTP2)
	}

	// Initial webhook TLS options
	webhookTLSOpts := tlsOpts
	webhookServerOptions := webhook.Options{
		TLSOpts: webhookTLSOpts,
	}

	if len(webhookCertPath) > 0 {
		setupLog.Info("Initializing webhook certificate watcher using provided certificates",
			"webhook-cert-path", webhookCertPath, "webhook-cert-name", webhookCertName, "webhook-cert-key", webhookCertKey)

		webhookServerOptions.CertDir = webhookCertPath
		webhookServerOptions.CertName = webhookCertName
		webhookServerOptions.KeyName = webhookCertKey
	}

	webhookServer := webhook.NewServer(webhookServerOptions)

	// Metrics endpoint is enabled in 'config/default/kustomization.yaml'. The Metrics options configure the server.
	// More info:
	// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.3/pkg/metrics/server
	// - https://book.kubebuilder.io/reference/metrics.html
	metricsServerOptions := metricsserver.Options{
		BindAddress:   metricsAddr,
		SecureServing: secureMetrics,
		TLSOpts:       tlsOpts,
	}

	if secureMetrics {
		// FilterProvider is used to protect the metrics endpoint with authn/authz.
		// These configurations ensure that only authorized users and service accounts
		// can access the metrics endpoint. The RBAC are configured in 'config/rbac/kustomization.yaml'. More info:
		// https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.3/pkg/metrics/filters#WithAuthenticationAndAuthorization
		metricsServerOptions.FilterProvider = filters.WithAuthenticationAndAuthorization
	}

	// If the certificate is not specified, controller-runtime will automatically
	// generate self-signed certificates for the metrics server. While convenient for development and testing,
	// this setup is not recommended for production.
	//
	// TODO(user): If you enable certManager, uncomment the following lines:
	// - [METRICS-WITH-CERTS] at config/default/kustomization.yaml to generate and use certificates
	// managed by cert-manager for the metrics server.
	// - [PROMETHEUS-WITH-CERTS] at config/prometheus/kustomization.yaml for TLS certification.
	if len(metricsCertPath) > 0 {
		setupLog.Info("Initializing metrics certificate watcher using provided certificates",
			"metrics-cert-path", metricsCertPath, "metrics-cert-name", metricsCertName, "metrics-cert-key", metricsCertKey)

		metricsServerOptions.CertDir = metricsCertPath
		metricsServerOptions.CertName = metricsCertName
		metricsServerOptions.KeyName = metricsCertKey
	}

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsServerOptions,
		WebhookServer:          webhookServer,
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElection,
		LeaderElectionID:       "ff17b1d7.dinova.one",
		// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
		// when the Manager ends. This requires the binary to immediately end when the
		// Manager is stopped, otherwise, this setting is unsafe. Setting this significantly
		// speeds up voluntary leader transitions as the new leader don't have to wait
		// LeaseDuration time first.
		//
		// In the default scaffold provided, the program ends immediately after
		// the manager stops, so would be fine to enable this option. However,
		// if you are doing or is intended to do any operation such as perform cleanups
		// after the manager stops then its usage might be unsafe.
		// LeaderElectionReleaseOnCancel: true,
	})
	if err != nil {
		setupLog.Error(err, "Failed to start manager")
		os.Exit(1)
	}

	apiClient, err := apiengine.NewClient(apiengine.Config{})
	if err != nil {
		setupLog.Error(err, "Failed to create API engine client")
		os.Exit(1)
	}

	if err := (&controller.BaseClusterReconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		APIClient: apiClient,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "basecluster")
		os.Exit(1)
	}
	// ApiCall uses its own bounded client; each worker also applies the CR's
	// timeout through context. No application retries or redirects are enabled.
	callHTTPClient, err := apiHTTPClient(apiCAFile)
	if err != nil {
		setupLog.Error(err, "Failed to configure ApiCall TLS trust")
		os.Exit(1)
	}
	if apiMaxResponseBytes < 64<<10 {
		setupLog.Error(errors.New("must be at least 65536"),
			"Invalid --apicall-max-response-bytes", "value", apiMaxResponseBytes)
		os.Exit(1)
	}
	callClient, err := apiengine.NewClient(apiengine.Config{
		HTTPClient: callHTTPClient, Timeout: time.Hour, MaxResponseBytes: apiMaxResponseBytes,
	})
	if err != nil {
		setupLog.Error(err, "Failed to create ApiCall client")
		os.Exit(1)
	}
	executor, err := apicall.NewAPIExecutor(callClient, apiWorkers, apiQueueSize)
	if err != nil {
		setupLog.Error(err, "Failed to create ApiCall workers")
		os.Exit(1)
	}
	var originsConfigMap types.NamespacedName
	if apiOriginsConfigMap != "" {
		namespace, name, found := strings.Cut(apiOriginsConfigMap, "/")
		if !found || namespace == "" || name == "" {
			setupLog.Error(errors.New("expected namespace/name"),
				"Invalid --apicall-allowed-origins-configmap", "value", apiOriginsConfigMap)
			os.Exit(1)
		}
		originsConfigMap = types.NamespacedName{Namespace: namespace, Name: name}
	}
	if err := (&controller.ApiCallReconciler{
		Client: mgr.GetClient(), Scheme: mgr.GetScheme(),
		Executor: executor, AllowedOrigins: apicall.ParseOrigins(apiAllowedOrigins),
		AllowedOriginsConfigMap: originsConfigMap,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "apicall")
		os.Exit(1)
	}
	stateConfig, err := terraform.NewS3StateConfig(tfStateEndpoint, tfStateBucket, tfStateRegion,
		os.Getenv("TFSTATE_S3_ACCESS_KEY_ID"), os.Getenv("TFSTATE_S3_SECRET_ACCESS_KEY"))
	if err != nil {
		setupLog.Error(err, "Invalid Terraform state storage configuration")
		os.Exit(1)
	}
	if !stateConfig.Configured() {
		setupLog.Info("No Terraform state storage configured; Terraform steps fail until --tfstate-s3-* are set")
	}
	if err := (&controller.WorkflowReconciler{
		Client: mgr.GetClient(), Scheme: mgr.GetScheme(),
		TerraformRunnerImage: controller.TerraformRunnerImage(), TerraformState: stateConfig,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "workflow")
		os.Exit(1)
	}
	if allowMutableTerraformSources {
		setupLog.Info("Mutable Terraform sources are allowed: templates with allowMutableSources read editable ConfigMaps")
	}
	if err := (&controller.TerraformTemplateReconciler{
		Client:              mgr.GetClient(),
		Scheme:              mgr.GetScheme(),
		AllowMutableSources: allowMutableTerraformSources,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "terraformtemplate")
		os.Exit(1)
	}
	if err := (&controller.WorkflowInstanceReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "workflowinstance")
		os.Exit(1)
	}
	if err := (&controller.WorkflowPipelineReconciler{
		Client: mgr.GetClient(),
		Scheme: mgr.GetScheme(),
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "Failed to create controller", "controller", "workflowpipeline")
		os.Exit(1)
	}
	// +kubebuilder:scaffold:builder

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "Failed to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("Starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "Failed to run manager")
		os.Exit(1)
	}
}

func apiHTTPClient(caFile string) (*http.Client, error) {
	if caFile == "" {
		return nil, nil
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}
	roots, err := x509.SystemCertPool()
	if err != nil || roots == nil {
		roots = x509.NewCertPool()
	}
	if ok := roots.AppendCertsFromPEM(pem); !ok {
		return nil, errors.New("CA file contains no valid PEM certificates")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if transport.TLSClientConfig == nil {
		transport.TLSClientConfig = &tls.Config{}
	} else {
		transport.TLSClientConfig = transport.TLSClientConfig.Clone()
	}
	transport.TLSClientConfig.RootCAs = roots
	return &http.Client{Transport: transport}, nil
}
