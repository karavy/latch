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

package controller

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"time"

	apiengine "github.com/karavy/api-engine/pkg/apiengine"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
	"github.com/karavy/latch/internal/controller/apicall"
)

var _ = Describe("Workflow controller", func() {
	It("sequences API calls, polls asynchronously, and maps the previous response into the next body", func() {
		var mu sync.Mutex
		var observed []string
		polls := 0
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			mu.Lock()
			defer mu.Unlock()
			switch req.URL.Path {
			case "/start":
				observed = append(observed, "start")
				_, _ = w.Write([]byte(`{"operationId":"op-42"}`))
			case "/poll":
				var body map[string]string
				Expect(json.NewDecoder(req.Body).Decode(&body)).To(Succeed())
				Expect(body["operation"]).To(Equal("op-42"))
				polls++
				observed = append(observed, "poll")
				_ = json.NewEncoder(w).Encode(map[string]any{"ready": polls >= 2, "resultId": "resource-99"})
			case "/finish":
				var body map[string]map[string]any
				Expect(json.NewDecoder(req.Body).Decode(&body)).To(Succeed())
				Expect(body["resource"]["id"]).To(Equal("resource-99"))
				Expect(body["resource"]["ready"]).To(BeTrue())
				Expect(body["resource"]["origin"]).To(Equal("op-42"))
				observed = append(observed, "finish")
				w.WriteHeader(http.StatusCreated)
			default:
				http.NotFound(w, req)
			}
		}))
		DeferCleanup(server.Close)

		apiClient, err := apiengine.NewClient(apiengine.Config{})
		Expect(err).NotTo(HaveOccurred())
		executor, err := apicall.NewAPIExecutor(apiClient, 2, 8)
		Expect(err).NotTo(HaveOccurred())
		workerCtx, stopWorkers := context.WithCancel(ctx)
		workersDone := make(chan struct{})
		go func() {
			defer close(workersDone)
			_ = executor.Start(workerCtx)
		}()
		DeferCleanup(func() {
			stopWorkers()
			Eventually(workersDone, 5*time.Second).Should(BeClosed())
		})
		Eventually(executor.Ready).Should(BeTrue())

		templates := map[string]dinovav1alpha1.ApiCallTemplateSpec{
			"seq-start":  {Method: http.MethodPost, Endpoint: server.URL + "/start"},
			"seq-poll":   {Method: http.MethodPost, Endpoint: server.URL + "/poll", Body: `{"operation":""}`},
			"seq-finish": {Method: http.MethodPost, Endpoint: server.URL + "/finish", Body: `{"resource":{}}`},
		}
		for templateName, spec := range templates {
			template := &dinovav1alpha1.ApiCallTemplate{ObjectMeta: metav1.ObjectMeta{Name: templateName, Namespace: "default"}, Spec: spec}
			Expect(k8sClient.Create(ctx, template)).To(Succeed())
			DeferCleanup(func() { Expect(k8sClient.Delete(ctx, template)).To(Succeed()) })
		}
		ref := func(name string) dinovav1alpha1.WorkflowResourceReference {
			return dinovav1alpha1.WorkflowResourceReference{Name: name}
		}

		name := types.NamespacedName{Name: "api-sequence", Namespace: "default"}
		createWorkflowFor(&dinovav1alpha1.WorkflowTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: name.Name, Namespace: name.Namespace},
			Spec: dinovav1alpha1.WorkflowTemplateSpec{Steps: []dinovav1alpha1.WorkflowStep{
				{Name: "start-job", API: &dinovav1alpha1.WorkflowAPIStep{
					TemplateRef: ref("seq-start"),
					Outputs:     []dinovav1alpha1.WorkflowOutput{{Name: "origin", From: "/operationId"}},
				}},
				{
					Name: "wait-job",
					API: &dinovav1alpha1.WorkflowAPIStep{
						TemplateRef: ref("seq-start"),
						Outputs:     []dinovav1alpha1.WorkflowOutput{{Name: "operation", From: "/operationId"}},
						Poll: &dinovav1alpha1.WorkflowPoll{
							TemplateRef:     ref("seq-poll"),
							Inputs:          []dinovav1alpha1.WorkflowInput{{Step: "wait-job", Output: "operation", To: "/operation"}},
							Outputs:         []dinovav1alpha1.WorkflowOutput{{Name: "resultId", From: "/resultId"}, {Name: "ready", From: "/ready"}},
							IntervalSeconds: 1, MaxAttempts: 3, ConditionJSONPointer: "/ready", EqualsJSON: "true",
						},
					},
				},
				{
					Name: "finish-job",
					API: &dinovav1alpha1.WorkflowAPIStep{
						TemplateRef: ref("seq-finish"),
						// "origin" comes from step 1, the others from step 2: not only the previous step is usable.
						Inputs: []dinovav1alpha1.WorkflowInput{
							{Step: "wait-job", Output: "resultId", To: "/resource/id"},
							{Step: "wait-job", Output: "ready", To: "/resource/ready"},
							{Step: "start-job", Output: "origin", To: "/resource/origin"},
						},
					},
				},
			}},
		}, "")

		workflowReconciler := &WorkflowReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), TerraformRunnerImage: "runner:test", TerraformState: testStateConfig(nil)}
		apiReconciler := &ApiCallReconciler{Client: k8sClient, Executor: executor, AllowedOrigins: []string{server.URL}}
		request := reconcile.Request{NamespacedName: name}
		Eventually(func() string {
			_, reconcileErr := workflowReconciler.Reconcile(ctx, request)
			Expect(reconcileErr).NotTo(HaveOccurred())
			children := &dinovav1alpha1.ApiCallList{}
			Expect(k8sClient.List(ctx, children, client.InNamespace(name.Namespace), client.MatchingLabels{"dinova.one/workflow": name.Name})).To(Succeed())
			for i := range children.Items {
				child := &children.Items[i]
				_, childErr := apiReconciler.Reconcile(ctx, reconcile.Request{NamespacedName: types.NamespacedName{Namespace: child.Namespace, Name: child.Name}})
				Expect(childErr).NotTo(HaveOccurred())
			}
			current := &dinovav1alpha1.Workflow{}
			Expect(k8sClient.Get(ctx, name, current)).To(Succeed())
			return current.Status.Phase
		}, 30*time.Second, 20*time.Millisecond).Should(Equal("Succeeded"))

		mu.Lock()
		Expect(observed).To(Equal([]string{"start", "start", "poll", "poll", "finish"}))
		mu.Unlock()
		result := &dinovav1alpha1.Workflow{}
		Expect(k8sClient.Get(ctx, name, result)).To(Succeed())
		Expect(stage(result).CurrentStep).To(Equal(int32(3)))
		Expect(stage(result).Steps).To(HaveLen(3))
		calls := &dinovav1alpha1.ApiCallList{}
		Expect(k8sClient.List(ctx, calls, client.InNamespace(name.Namespace))).To(Succeed())
		for i := range calls.Items {
			Expect(calls.Items[i].OwnerReferences).NotTo(BeEmpty(), "every ApiCall in the namespace must be a Workflow-owned copy")
		}
	})

	It("fails the workflow when a referenced ApiCall does not exist", func() {
		name := types.NamespacedName{Name: "missing-template", Namespace: "default"}
		createWorkflowFor(&dinovav1alpha1.WorkflowTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: name.Name, Namespace: name.Namespace},
			Spec: dinovav1alpha1.WorkflowTemplateSpec{Steps: []dinovav1alpha1.WorkflowStep{{
				Name: "call", API: &dinovav1alpha1.WorkflowAPIStep{TemplateRef: dinovav1alpha1.WorkflowResourceReference{Name: "does-not-exist"}},
			}}},
		}, "")
		reconciler := &WorkflowReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), TerraformRunnerImage: "runner:test", TerraformState: testStateConfig(nil)}
		Eventually(func() string {
			_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: name})
			Expect(err).NotTo(HaveOccurred())
			current := &dinovav1alpha1.Workflow{}
			Expect(k8sClient.Get(ctx, name, current)).To(Succeed())
			return current.Status.Phase
		}).Should(Equal("Failed"))
		children := &dinovav1alpha1.ApiCallList{}
		Expect(k8sClient.List(ctx, children, client.InNamespace(name.Namespace), client.MatchingLabels{"dinova.one/workflow": name.Name})).To(Succeed())
		Expect(children.Items).To(BeEmpty())
	})

	It("copies a TerraformTemplate into a ConfigMap and runs it in a PVC-backed job with its variables in a Secret", func() {
		name := types.NamespacedName{Name: "terraform-workflow", Namespace: "default"}
		template := &dinovav1alpha1.TerraformTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: "terraform-vdc", Namespace: name.Namespace},
			Spec: dinovav1alpha1.TerraformTemplateSpec{Files: []dinovav1alpha1.TerraformFile{
				{Name: "main.tf.tmpl", Content: `output "cluster_id" { value = "<FGT_CLUSTER_ID>" }`},
				{Name: "providers.tf", Content: "terraform {}"},
			}},
		}
		Expect(k8sClient.Create(ctx, template)).To(Succeed())
		DeferCleanup(func() { Expect(k8sClient.Delete(ctx, template)).To(Succeed()) })
		createWorkflowFor(&dinovav1alpha1.WorkflowTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: name.Name, Namespace: name.Namespace},
			Spec: dinovav1alpha1.WorkflowTemplateSpec{Steps: []dinovav1alpha1.WorkflowStep{{
				Name: "apply", Terraform: &dinovav1alpha1.WorkflowTerraformStep{
					TemplateRef: dinovav1alpha1.WorkflowResourceReference{Name: template.Name}, TimeoutSeconds: 60,
					EnvFromSecrets: []string{"cloud-credentials"},
					State:          dinovav1alpha1.WorkflowTerraformState{Key: "vdc/acme"},
				},
			}}},
		}, "1Gi")
		reconciler := &WorkflowReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), TerraformRunnerImage: "runner:test", TerraformState: testStateConfig(nil)}
		request := reconcile.Request{NamespacedName: name}
		for range 3 { // initializes the status, starts the template, runs its first step
			_, err := reconciler.Reconcile(ctx, request)
			Expect(err).NotTo(HaveOccurred())
		}
		current := &dinovav1alpha1.Workflow{}
		Expect(k8sClient.Get(ctx, name, current)).To(Succeed())
		workspace := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: name.Namespace, Name: stage(current).WorkspacePVCName}, workspace)).To(Succeed())
		workspace.Status.Phase = corev1.ClaimBound
		Expect(k8sClient.Status().Update(ctx, workspace)).To(Succeed())
		_, err := reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())

		By("storing the variables in a Secret")
		secrets := &corev1.SecretList{}
		Expect(k8sClient.List(ctx, secrets, client.InNamespace(name.Namespace), client.MatchingLabels{"dinova.one/workflow": name.Name})).To(Succeed())
		Expect(secrets.Items).To(HaveLen(1))
		Expect(json.Valid(secrets.Items[0].Data["terraform.auto.tfvars.json"])).To(BeTrue())

		By("copying the template files into a ConfigMap owned by the Workflow")
		configMaps := &corev1.ConfigMapList{}
		Expect(k8sClient.List(ctx, configMaps, client.InNamespace(name.Namespace), client.MatchingLabels{"dinova.one/workflow": name.Name})).To(Succeed())
		Expect(configMaps.Items).To(HaveLen(1))
		files := configMaps.Items[0]
		Expect(files.Data).To(Equal(map[string]string{"main.tf.tmpl": template.Spec.Files[0].Content, "providers.tf": "terraform {}"}))
		Expect(files.Annotations).To(HaveKeyWithValue("dinova.one/terraform-template", template.Name))
		Expect(files.OwnerReferences).To(ContainElement(HaveField("Name", name.Name)))

		By("running a job that mounts the copy, the workspace and the variables")
		jobs := &batchv1.JobList{}
		Expect(k8sClient.List(ctx, jobs, client.InNamespace(name.Namespace), client.MatchingLabels{"dinova.one/workflow": name.Name})).To(Succeed())
		Expect(jobs.Items).To(HaveLen(1))
		pod := jobs.Items[0].Spec.Template.Spec
		Expect(pod.Containers[0].Image).To(Equal("runner:test"))
		Expect(pod.Volumes).To(ContainElement(HaveField("Name", "workspace")))
		Expect(pod.Volumes).To(ContainElement(And(HaveField("Name", "templates"), HaveField("ConfigMap.Name", files.Name))))
		Expect(pod.Containers[0].EnvFrom).To(ContainElement(HaveField("SecretRef.Name", "cloud-credentials")))
		env := map[string]string{}
		for _, variable := range pod.Containers[0].Env {
			env[variable.Name] = variable.Value
		}
		Expect(env).To(HaveKeyWithValue("TF_STATE_KEY", "default/vdc/acme.tfstate"), "the state key is scoped to the namespace")
		Expect(env).To(HaveKeyWithValue("TF_STATE_BUCKET", "tfstate"))
		Expect(env).To(HaveKeyWithValue("TF_STATE_ENDPOINT", "http://minio.test:9000"))
		Expect(k8sClient.Get(ctx, name, current)).To(Succeed())
		Expect(stepStatus(current, "apply").StateKey).To(Equal("default/vdc/acme.tfstate"))

		By("not letting later edits of the template change the running step")
		fresh := &dinovav1alpha1.TerraformTemplate{}
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(template), fresh)).To(Succeed())
		fresh.Spec.Files[1].Content = "terraform { required_version = \">= 1.0\" }"
		Expect(k8sClient.Update(ctx, fresh)).To(Succeed())
		_, err = reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, client.ObjectKeyFromObject(&files), &files)).To(Succeed())
		Expect(files.Data["providers.tf"]).To(Equal("terraform {}"))
	})

	It("runs several Terraform steps in order, each in its own workspace folder with its own template", func() {
		name := types.NamespacedName{Name: "terraform-two-steps", Namespace: "default"}
		for _, tmpl := range []struct{ name, content string }{{"tf-net", `resource "terraform_data" "net" {}`}, {"tf-cluster", `resource "terraform_data" "cluster" {}`}} {
			template := &dinovav1alpha1.TerraformTemplate{
				ObjectMeta: metav1.ObjectMeta{Name: tmpl.name, Namespace: name.Namespace},
				Spec:       dinovav1alpha1.TerraformTemplateSpec{Files: []dinovav1alpha1.TerraformFile{{Name: "main.tf", Content: tmpl.content}}},
			}
			Expect(k8sClient.Create(ctx, template)).To(Succeed())
			DeferCleanup(func() { Expect(k8sClient.Delete(ctx, template)).To(Succeed()) })
		}
		createWorkflowFor(&dinovav1alpha1.WorkflowTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: name.Name, Namespace: name.Namespace},
			Spec: dinovav1alpha1.WorkflowTemplateSpec{Steps: []dinovav1alpha1.WorkflowStep{
				{Name: "network", Terraform: &dinovav1alpha1.WorkflowTerraformStep{TemplateRef: dinovav1alpha1.WorkflowResourceReference{Name: "tf-net"}, State: dinovav1alpha1.WorkflowTerraformState{Key: "two/network"}}},
				{Name: "cluster", Terraform: &dinovav1alpha1.WorkflowTerraformStep{TemplateRef: dinovav1alpha1.WorkflowResourceReference{Name: "tf-cluster"}, State: dinovav1alpha1.WorkflowTerraformState{Key: "two/cluster"}}},
			}},
		}, "1Gi")
		reconciler := &WorkflowReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), TerraformRunnerImage: "runner:test", TerraformState: testStateConfig(nil)}
		request := reconcile.Request{NamespacedName: name}
		reconcileOnce := func() *dinovav1alpha1.Workflow {
			_, err := reconciler.Reconcile(ctx, request)
			Expect(err).NotTo(HaveOccurred())
			current := &dinovav1alpha1.Workflow{}
			Expect(k8sClient.Get(ctx, name, current)).To(Succeed())
			return current
		}
		jobsOf := func() map[string]batchv1.Job {
			list := &batchv1.JobList{}
			Expect(k8sClient.List(ctx, list, client.InNamespace(name.Namespace), client.MatchingLabels{"dinova.one/workflow": name.Name})).To(Succeed())
			byStep := map[string]batchv1.Job{}
			for _, job := range list.Items {
				for _, env := range job.Spec.Template.Spec.Containers[0].Env {
					if env.Name == "WORKSPACE_DIR" {
						byStep[env.Value] = job
					}
				}
			}
			return byStep
		}
		templatesMountedBy := func(job batchv1.Job) string {
			for _, volume := range job.Spec.Template.Spec.Volumes {
				if volume.Name == "templates" {
					return volume.ConfigMap.Name
				}
			}
			return ""
		}

		reconcileOnce() // initializes the status
		reconcileOnce() // starts the template
		current := reconcileOnce()
		workspace := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: name.Namespace, Name: stage(current).WorkspacePVCName}, workspace)).To(Succeed())
		workspace.Status.Phase = corev1.ClaimBound
		Expect(k8sClient.Status().Update(ctx, workspace)).To(Succeed())

		By("starting only the first step")
		reconcileOnce()
		Expect(jobsOf()).To(HaveLen(1))
		first := jobsOf()["/workspace/network"]
		Expect(first.Name).NotTo(BeEmpty())

		By("not starting the second step while the first job is running")
		reconcileOnce()
		Expect(jobsOf()).To(HaveLen(1))

		By("starting the second step in its own folder once the first job succeeded")
		first.Status = batchv1.JobStatus{
			Succeeded: 1, StartTime: &metav1.Time{Time: time.Now().Add(-time.Minute)}, CompletionTime: &metav1.Time{Time: time.Now()},
			Conditions: []batchv1.JobCondition{
				{Type: batchv1.JobSuccessCriteriaMet, Status: corev1.ConditionTrue},
				{Type: batchv1.JobComplete, Status: corev1.ConditionTrue},
			},
		}
		Expect(k8sClient.Status().Update(ctx, &first)).To(Succeed())
		reconcileOnce()
		reconcileOnce()
		jobs := jobsOf()
		Expect(jobs).To(HaveLen(2))
		second := jobs["/workspace/cluster"]
		Expect(second.Name).NotTo(BeEmpty())

		By("giving each step its own template copy, inputs and job, on the same workspace volume")
		Expect(templatesMountedBy(first)).NotTo(Equal(templatesMountedBy(second)))
		for step, want := range map[string]string{"network": `resource "terraform_data" "net" {}`, "cluster": `resource "terraform_data" "cluster" {}`} {
			job := jobs["/workspace/"+step]
			configMap := &corev1.ConfigMap{}
			Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: name.Namespace, Name: templatesMountedBy(job)}, configMap)).To(Succeed())
			Expect(configMap.Data).To(Equal(map[string]string{"main.tf": want}))
			Expect(job.Spec.Template.Spec.Volumes).To(ContainElement(And(HaveField("Name", "workspace"), HaveField("PersistentVolumeClaim.ClaimName", stage(current).WorkspacePVCName))))
		}
		secrets := &corev1.SecretList{}
		Expect(k8sClient.List(ctx, secrets, client.InNamespace(name.Namespace), client.MatchingLabels{"dinova.one/workflow": name.Name})).To(Succeed())
		Expect(secrets.Items).To(HaveLen(2))
		claims := &corev1.PersistentVolumeClaimList{}
		Expect(k8sClient.List(ctx, claims, client.InNamespace(name.Namespace))).To(Succeed())
		owned := 0
		for _, claim := range claims.Items {
			for _, owner := range claim.OwnerReferences {
				if owner.Name == name.Name {
					owned++
				}
			}
		}
		Expect(owned).To(Equal(1), "all Terraform steps share a single workspace volume")

		By("finishing the workflow when the last job succeeds")
		second.Status = first.Status
		Expect(k8sClient.Status().Update(ctx, &second)).To(Succeed())
		reconcileOnce()
		final := reconcileOnce()
		Expect(final.Status.Phase).To(Equal("Succeeded"))
		Expect(stage(final).CurrentStep).To(Equal(int32(2)))
	})

	It("fails the workflow when the referenced TerraformTemplate does not exist", func() {
		name := types.NamespacedName{Name: "terraform-missing-template", Namespace: "default"}
		createWorkflowFor(&dinovav1alpha1.WorkflowTemplate{
			ObjectMeta: metav1.ObjectMeta{Name: name.Name, Namespace: name.Namespace},
			Spec: dinovav1alpha1.WorkflowTemplateSpec{Steps: []dinovav1alpha1.WorkflowStep{{
				Name: "apply", Terraform: &dinovav1alpha1.WorkflowTerraformStep{TemplateRef: dinovav1alpha1.WorkflowResourceReference{Name: "does-not-exist"}, State: dinovav1alpha1.WorkflowTerraformState{Key: "missing"}},
			}}},
		}, "1Gi")
		reconciler := &WorkflowReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), TerraformRunnerImage: "runner:test", TerraformState: testStateConfig(nil)}
		request := reconcile.Request{NamespacedName: name}
		for range 3 {
			_, err := reconciler.Reconcile(ctx, request)
			Expect(err).NotTo(HaveOccurred())
		}
		current := &dinovav1alpha1.Workflow{}
		Expect(k8sClient.Get(ctx, name, current)).To(Succeed())
		workspace := &corev1.PersistentVolumeClaim{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: name.Namespace, Name: stage(current).WorkspacePVCName}, workspace)).To(Succeed())
		workspace.Status.Phase = corev1.ClaimBound
		Expect(k8sClient.Status().Update(ctx, workspace)).To(Succeed())
		_, err := reconciler.Reconcile(ctx, request)
		Expect(err).NotTo(HaveOccurred())
		Expect(k8sClient.Get(ctx, name, current)).To(Succeed())
		Expect(current.Status.Phase).To(Equal("Failed"))
		Expect(readyReason(current)).To(Equal("TerraformTemplateUnavailable"))
	})

	It("validates TerraformTemplate files in the generated CRD", func() {
		newTemplate := func(files ...dinovav1alpha1.TerraformFile) *dinovav1alpha1.TerraformTemplate {
			return &dinovav1alpha1.TerraformTemplate{
				ObjectMeta: metav1.ObjectMeta{GenerateName: "tf-invalid-", Namespace: "default"},
				Spec:       dinovav1alpha1.TerraformTemplateSpec{Files: files},
			}
		}
		valid := dinovav1alpha1.TerraformFile{Name: "main.tf", Content: "x"}
		for name, template := range map[string]*dinovav1alpha1.TerraformTemplate{
			"no files":        newTemplate(),
			"path traversal":  newTemplate(dinovav1alpha1.TerraformFile{Name: "../main.tf", Content: "x"}),
			"subdirectory":    newTemplate(dinovav1alpha1.TerraformFile{Name: "a/main.tf", Content: "x"}),
			"not terraform":   newTemplate(dinovav1alpha1.TerraformFile{Name: "run.sh", Content: "x"}),
			"empty content":   newTemplate(dinovav1alpha1.TerraformFile{Name: "main.tf"}),
			"duplicate names": newTemplate(valid, valid),
		} {
			Expect(k8sClient.Create(ctx, template)).NotTo(Succeed(), name)
		}
		ok := newTemplate(valid, dinovav1alpha1.TerraformFile{Name: "vars.tf.tmpl", Content: "x"})
		Expect(k8sClient.Create(ctx, ok)).To(Succeed())
		Expect(k8sClient.Delete(ctx, ok)).To(Succeed())
	})
})
