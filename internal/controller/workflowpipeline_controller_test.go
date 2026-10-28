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
	"net/http"
	"strconv"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
	"github.com/karavy/latch/internal/controller/instance"
	"github.com/karavy/latch/internal/controller/pipeline"
)

// pipelineNamespace is the namespace of the pipelines the tests create.
const pipelineNamespace = "default"

// pipelineUnderTest drives a WorkflowPipeline; the tests play the instance and Workflow
// controllers by setting the status of the stage objects.
type pipelineUnderTest struct {
	key        types.NamespacedName
	reconciler *WorkflowPipelineReconciler
}

func newPipelineUnderTest(object *dinovav1alpha1.WorkflowPipeline) *pipelineUnderTest {
	GinkgoHelper()
	object.Namespace = pipelineNamespace
	Expect(k8sClient.Create(ctx, object)).To(Succeed())
	DeferCleanup(func() {
		current := &dinovav1alpha1.WorkflowPipeline{}
		if k8sClient.Get(ctx, client.ObjectKeyFromObject(object), current) == nil {
			controllerutil.RemoveFinalizer(current, "dinova.one/pipeline-deletion")
			Expect(client.IgnoreNotFound(k8sClient.Update(ctx, current))).To(Succeed())
			Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, current))).To(Succeed())
		}
		Expect(k8sClient.DeleteAllOf(ctx, &dinovav1alpha1.WorkflowInstance{}, client.InNamespace(pipelineNamespace),
			client.MatchingLabels{pipeline.PipelineLabel: object.Name})).To(Succeed())
		Expect(k8sClient.DeleteAllOf(ctx, &dinovav1alpha1.Workflow{}, client.InNamespace(pipelineNamespace),
			client.MatchingLabels{pipeline.PipelineLabel: object.Name})).To(Succeed())
	})
	test := &pipelineUnderTest{
		key:        client.ObjectKeyFromObject(object),
		reconciler: &WorkflowPipelineReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()},
	}
	By("adding the finalizer first")
	Expect(test.reconcile().Finalizers).To(ContainElement("dinova.one/pipeline-deletion"))
	return test
}

// reconcile runs the controller once and returns the pipeline, nil once it is gone.
func (t *pipelineUnderTest) reconcile() *dinovav1alpha1.WorkflowPipeline {
	GinkgoHelper()
	_, err := t.reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: t.key})
	Expect(err).NotTo(HaveOccurred())
	current := &dinovav1alpha1.WorkflowPipeline{}
	if err := k8sClient.Get(ctx, t.key, current); apierrors.IsNotFound(err) {
		return nil
	} else {
		Expect(err).NotTo(HaveOccurred())
	}
	return current
}

func (t *pipelineUnderTest) update(change func(*dinovav1alpha1.WorkflowPipeline)) {
	GinkgoHelper()
	current := &dinovav1alpha1.WorkflowPipeline{}
	Expect(k8sClient.Get(ctx, t.key, current)).To(Succeed())
	change(current)
	Expect(k8sClient.Update(ctx, current)).To(Succeed())
}

func stageStatus(current *dinovav1alpha1.WorkflowPipeline, name string) dinovav1alpha1.WorkflowPipelineStageStatus {
	GinkgoHelper()
	for _, stage := range current.Status.Stages {
		if stage.Name == name {
			return stage
		}
	}
	Fail("no status for stage " + name)
	return dinovav1alpha1.WorkflowPipelineStageStatus{}
}

func pipelineReady(current *dinovav1alpha1.WorkflowPipeline) metav1.Condition {
	GinkgoHelper()
	condition := meta.FindStatusCondition(current.Status.Conditions, "Ready")
	Expect(condition).NotTo(BeNil())
	return *condition
}

func getInstance(name string) *dinovav1alpha1.WorkflowInstance {
	GinkgoHelper()
	item := &dinovav1alpha1.WorkflowInstance{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: pipelineNamespace, Name: name}, item)).To(Succeed())
	return item
}

func getWorkflow(name string) *dinovav1alpha1.Workflow {
	GinkgoHelper()
	run := &dinovav1alpha1.Workflow{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: pipelineNamespace, Name: name}, run)).To(Succeed())
	return run
}

func workflowExists(name string) bool {
	return k8sClient.Get(ctx, types.NamespacedName{Namespace: pipelineNamespace, Name: name}, &dinovav1alpha1.Workflow{}) == nil
}

// setInstanceReady sets the Ready condition of an instance to True, as its controller would.
func setInstanceReady(name string) {
	GinkgoHelper()
	item := getInstance(name)
	meta.SetStatusCondition(&item.Status.Conditions, metav1.Condition{
		Type: "Ready", Status: metav1.ConditionTrue, Reason: "Applied", Message: "Applied", ObservedGeneration: item.Generation,
	})
	Expect(k8sClient.Status().Update(ctx, item)).To(Succeed())
}

// finishInstanceRun creates run number of an instance, succeeded, with the outputs of its
// step "apply" (JSON values).
func finishInstanceRun(name string, number int, outputs map[string]string) {
	GinkgoHelper()
	item := getInstance(name)
	run := &dinovav1alpha1.Workflow{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: pipelineNamespace, Name: name + "-" + strconv.Itoa(number),
			Labels:      map[string]string{instance.InstanceLabel: name},
			Annotations: map[string]string{instance.RunAnnotation: strconv.Itoa(number)},
		},
		Spec: dinovav1alpha1.WorkflowSpec{Templates: []dinovav1alpha1.WorkflowTemplateRun{{
			Name: "run", TemplateRef: item.Spec.TemplateRef, Parameters: item.Spec.Parameters,
		}}},
	}
	Expect(controllerutil.SetControllerReference(item, run, k8sClient.Scheme())).To(Succeed())
	Expect(k8sClient.Create(ctx, run)).To(Succeed())
	DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, run))).To(Succeed()) })
	finishWorkflow(run.Name, "Succeeded", "apply", outputs, nil)
}

// finishWorkflow ends a Workflow as its controller would, with the outputs of one step.
func finishWorkflow(name, phase, step string, outputs map[string]string, sensitive []string) {
	GinkgoHelper()
	run := getWorkflow(name)
	run.Status.Phase = phase
	run.Status.Templates = []dinovav1alpha1.WorkflowTemplateStatus{{Name: "run", Phase: phase, Steps: []dinovav1alpha1.WorkflowStepStatus{{
		Name: step, Phase: phase, Outputs: outputs, SensitiveOutputs: sensitive,
	}}}}
	meta.SetStatusCondition(&run.Status.Conditions, metav1.Condition{Type: "Ready", Status: metav1.ConditionFalse, Reason: phase, Message: "Step " + step + " " + phase})
	Expect(k8sClient.Status().Update(ctx, run)).To(Succeed())
}

// createAPIWorkflowTemplate creates a WorkflowTemplate with one API step "configure" that sends
// the ApiCallTemplate of the same name, with the given parameters.
func createAPIWorkflowTemplate(name string, idempotent bool, parameters ...string) {
	GinkgoHelper()
	template := &dinovav1alpha1.WorkflowTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: pipelineNamespace},
		Spec: dinovav1alpha1.WorkflowTemplateSpec{Steps: []dinovav1alpha1.WorkflowStep{{
			Name: "configure", API: &dinovav1alpha1.WorkflowAPIStep{
				TemplateRef: dinovav1alpha1.WorkflowResourceReference{Name: name}, Idempotent: idempotent,
			},
		}}},
	}
	for _, parameter := range parameters {
		template.Spec.Parameters = append(template.Spec.Parameters, dinovav1alpha1.WorkflowTemplateParameter{Name: parameter})
	}
	Expect(k8sClient.Create(ctx, template)).To(Succeed())
	DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, template))).To(Succeed()) })
}

// fromOutput reads an output of the step "apply" of a stage.
func fromOutput(name, stage, output string) dinovav1alpha1.WorkflowPipelineParameter {
	return dinovav1alpha1.WorkflowPipelineParameter{Name: name, ValueFrom: &dinovav1alpha1.WorkflowPipelineOutputReference{Stage: stage, Step: "apply", Output: output}}
}

func instanceStage(name, template string) dinovav1alpha1.WorkflowPipelineStage {
	return dinovav1alpha1.WorkflowPipelineStage{Name: name, Instance: &dinovav1alpha1.WorkflowPipelineInstanceStage{
		TemplateRef: dinovav1alpha1.WorkflowResourceReference{Name: template}, Interval: metav1.Duration{Duration: time.Hour},
		Approval: "Auto", WorkspaceStorage: "2Gi", DeletionPolicy: "Retain",
	}}
}

func workflowStage(name, template string, parameters []dinovav1alpha1.WorkflowPipelineParameter, dependsOn ...string) dinovav1alpha1.WorkflowPipelineStage {
	return dinovav1alpha1.WorkflowPipelineStage{Name: name, DependsOn: dependsOn, Parameters: parameters, Workflow: &dinovav1alpha1.WorkflowPipelineWorkflowStage{
		TemplateRef: dinovav1alpha1.WorkflowResourceReference{Name: template}, WorkspaceStorage: "2Gi",
	}}
}

var _ = Describe("WorkflowPipeline Controller", func() {
	It("refuses invalid stages when the pipeline is created", func() {
		invalid := func(name string, stages ...dinovav1alpha1.WorkflowPipelineStage) error {
			return k8sClient.Create(ctx, &dinovav1alpha1.WorkflowPipeline{
				ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: pipelineNamespace},
				Spec:       dinovav1alpha1.WorkflowPipelineSpec{Stages: stages},
			})
		}
		both := instanceStage("both", "t")
		both.Workflow = &dinovav1alpha1.WorkflowPipelineWorkflowStage{TemplateRef: dinovav1alpha1.WorkflowResourceReference{Name: "t"}}
		Expect(invalid("both", both)).To(MatchError(ContainSubstring("exactly one of instance or workflow must be set")))
		Expect(invalid("unknown", workflowStage("a", "t", nil, "missing"))).To(MatchError(ContainSubstring("dependsOn must name other stages")))
		Expect(invalid("itself", workflowStage("a", "t", nil, "a"))).To(MatchError(ContainSubstring("dependsOn must name other stages")))
		value := "x"
		twice := workflowStage("a", "t", []dinovav1alpha1.WorkflowPipelineParameter{{
			Name: "p", Value: &value, ValueFrom: &dinovav1alpha1.WorkflowPipelineOutputReference{Stage: "b", Step: "s", Output: "o"},
		}})
		Expect(invalid("twice", twice)).To(MatchError(ContainSubstring("exactly one of value or valueFrom must be set")))
		Expect(invalid("a-pipeline-name-long-enough-to-matter", workflowStage("a-stage-name-that-is-long-too", "t", nil))).
			To(MatchError(ContainSubstring("at most 55 characters together")))
	})

	It("runs the stages in the order of their dependencies and passes outputs between them", func() {
		createTerraformWorkflowTemplate("pipe-net")
		createTerraformWorkflowTemplate("pipe-dns")
		createTemplates(map[string]dinovav1alpha1.ApiCallTemplateSpec{"pipe-fw": {Method: http.MethodPost, Endpoint: "https://portal.example/fw"}})
		createAPIWorkflowTemplate("pipe-fw", false, "clientName", "networkId")
		test := newPipelineUnderTest(&dinovav1alpha1.WorkflowPipeline{
			ObjectMeta: metav1.ObjectMeta{Name: "chain", Labels: map[string]string{"dinova.one/client": "ACME"}},
			Spec: dinovav1alpha1.WorkflowPipelineSpec{
				Parameters: []dinovav1alpha1.WorkflowParameter{{Name: "clientName", Value: "ACME"}, {Name: "unused", Value: "x"}},
				Stages: []dinovav1alpha1.WorkflowPipelineStage{
					instanceStage("net", "pipe-net"),
					workflowStage("fw", "pipe-fw", []dinovav1alpha1.WorkflowPipelineParameter{fromOutput("networkId", "net", "networkId")}, "net"),
					instanceStage("dns", "pipe-dns"),
				},
			},
		})

		By("creating at once the stages that depend on nothing, with the parameters their templates declare")
		current := test.reconcile()
		Expect(current.Finalizers).To(ContainElement("dinova.one/pipeline-deletion"))
		net := getInstance("chain-net")
		Expect(metav1.IsControlledBy(net, current)).To(BeTrue())
		Expect(net.Labels).To(HaveKeyWithValue("dinova.one/client", "ACME"))
		Expect(net.Labels).To(HaveKeyWithValue(pipeline.StageLabel, "net"))
		Expect(net.Spec.Parameters).To(Equal([]dinovav1alpha1.WorkflowParameter{{Name: "clientName", Value: "ACME"}}))
		getInstance("chain-dns")
		Expect(stageStatus(current, "net").Phase).To(Equal("Running"))
		Expect(stageStatus(current, "fw").Phase).To(Equal("Pending"))
		Expect(stageStatus(current, "fw").Message).To(ContainSubstring("Waiting for stages net"))
		Expect(workflowExists("chain-fw-1")).To(BeFalse())

		By("starting a Workflow stage once its dependency is ready, with the output it reads")
		finishInstanceRun("chain-net", 1, map[string]string{"networkId": `"net-1"`})
		setInstanceReady("chain-net")
		current = test.reconcile()
		Expect(stageStatus(current, "net").Phase).To(Equal("Ready"))
		run := getWorkflow("chain-fw-1")
		Expect(metav1.IsControlledBy(run, current)).To(BeTrue())
		Expect(run.Spec.Templates[0].Parameters).To(Equal([]dinovav1alpha1.WorkflowParameter{
			{Name: "clientName", Value: "ACME"}, {Name: "networkId", Value: "net-1"},
		}))
		Expect(stageStatus(current, "fw")).To(And(HaveField("Phase", "Running"), HaveField("Workflow", "chain-fw-1"), HaveField("Runs", BeEquivalentTo(1))))

		By("being ready once every stage is")
		finishWorkflow("chain-fw-1", "Succeeded", "configure", nil, nil)
		current = test.reconcile()
		Expect(stageStatus(current, "fw").Phase).To(Equal("Ready"))
		Expect(pipelineReady(current).Reason).To(Equal("InProgress"))
		setInstanceReady("chain-dns")
		current = test.reconcile()
		Expect(pipelineReady(current).Status).To(Equal(metav1.ConditionTrue))

		By("marking the Workflow stage Outdated, without running it, when an output it used changes")
		finishInstanceRun("chain-net", 2, map[string]string{"networkId": `"net-2"`})
		current = test.reconcile()
		fw := stageStatus(current, "fw")
		Expect(fw.Phase).To(Equal("Outdated"))
		Expect(fw.Changed).To(Equal([]dinovav1alpha1.WorkflowPipelineChangedParameter{{Name: "networkId", Old: "net-1", New: "net-2"}}))
		Expect(fw.Message).To(ContainSubstring("list chain-fw-1 in spec.rerun"))
		Expect(pipelineReady(current).Reason).To(Equal("StageOutdated"))
		Expect(workflowExists("chain-fw-2")).To(BeFalse())

		By("running it again, once, when its run is listed in spec.rerun")
		test.update(func(p *dinovav1alpha1.WorkflowPipeline) { p.Spec.Rerun = []string{"chain-fw-1"} })
		current = test.reconcile()
		Expect(getWorkflow("chain-fw-2").Spec.Templates[0].Parameters).To(ContainElement(dinovav1alpha1.WorkflowParameter{Name: "networkId", Value: "net-2"}))
		Expect(stageStatus(current, "fw").Workflow).To(Equal("chain-fw-2"))
		Expect(workflowExists("chain-fw-1")).To(BeTrue(), "the old run is kept: deleting it would run its deleteSteps")
		finishWorkflow("chain-fw-2", "Succeeded", "configure", nil, nil)
		current = test.reconcile()
		Expect(stageStatus(current, "fw").Phase).To(Equal("Ready"))
		Expect(workflowExists("chain-fw-3")).To(BeFalse(), "the entry names a run that is not the current one")

		By("passing a new output to an instance stage, whose revision then changes")
		test.update(func(p *dinovav1alpha1.WorkflowPipeline) {
			p.Spec.Stages[2].DependsOn = []string{"net"}
			p.Spec.Stages[2].Parameters = []dinovav1alpha1.WorkflowPipelineParameter{fromOutput("clientName", "net", "networkId")}
		})
		test.reconcile()
		Expect(getInstance("chain-dns").Spec.Parameters).To(Equal([]dinovav1alpha1.WorkflowParameter{{Name: "clientName", Value: "net-2"}}))
	})

	It("runs a failed Workflow stage again only when listed in spec.rerun", func() {
		createTemplates(map[string]dinovav1alpha1.ApiCallTemplateSpec{"pipe-fail": {Method: http.MethodPost, Endpoint: "https://portal.example/fail"}})
		createAPIWorkflowTemplate("pipe-fail", false)
		test := newPipelineUnderTest(&dinovav1alpha1.WorkflowPipeline{
			ObjectMeta: metav1.ObjectMeta{Name: "failing"},
			Spec:       dinovav1alpha1.WorkflowPipelineSpec{Stages: []dinovav1alpha1.WorkflowPipelineStage{workflowStage("call", "pipe-fail", nil)}},
		})
		test.reconcile()
		finishWorkflow("failing-call-1", "Failed", "configure", nil, nil)
		current := test.reconcile()
		Expect(stageStatus(current, "call").Phase).To(Equal("Failed"))
		Expect(stageStatus(current, "call").Message).To(And(ContainSubstring("Step configure Failed"), ContainSubstring("List failing-call-1 in spec.rerun")))
		Expect(pipelineReady(current).Reason).To(Equal("StageFailed"))
		Expect(workflowExists("failing-call-2")).To(BeFalse())

		test.update(func(p *dinovav1alpha1.WorkflowPipeline) { p.Spec.Rerun = []string{"failing-call-1"} })
		current = test.reconcile()
		Expect(stageStatus(current, "call").Workflow).To(Equal("failing-call-2"))
	})

	It("runs a Workflow stage again by itself only when rerunOnChange is set and its API steps are safe to repeat", func() {
		createTerraformWorkflowTemplate("pipe-src")
		createTemplates(map[string]dinovav1alpha1.ApiCallTemplateSpec{
			"pipe-post": {Method: http.MethodPost, Endpoint: "https://portal.example/post"},
			"pipe-put":  {Method: http.MethodPut, Endpoint: "https://portal.example/put"},
		})
		createAPIWorkflowTemplate("pipe-post", false, "value")
		createAPIWorkflowTemplate("pipe-put", true, "value")
		rerunning := func(name, template string) dinovav1alpha1.WorkflowPipelineStage {
			stage := workflowStage(name, template, []dinovav1alpha1.WorkflowPipelineParameter{fromOutput("value", "src", "value")}, "src")
			stage.Workflow.RerunOnChange = true
			return stage
		}
		test := newPipelineUnderTest(&dinovav1alpha1.WorkflowPipeline{
			ObjectMeta: metav1.ObjectMeta{Name: "rerun"},
			Spec: dinovav1alpha1.WorkflowPipelineSpec{Stages: []dinovav1alpha1.WorkflowPipelineStage{
				instanceStage("src", "pipe-src"), rerunning("post", "pipe-post"), rerunning("put", "pipe-put"),
			}},
		})
		test.reconcile()
		finishInstanceRun("rerun-src", 1, map[string]string{"value": "1"})
		setInstanceReady("rerun-src")
		current := test.reconcile()

		By("refusing rerunOnChange on a POST that is not marked idempotent")
		post := stageStatus(current, "post")
		Expect(post.Phase).To(Equal("Failed"))
		Expect(post.Message).To(ContainSubstring(`step "configure" of WorkflowTemplate "pipe-post" sends a POST and is not marked idempotent`))
		Expect(workflowExists("rerun-post-1")).To(BeFalse())

		By("passing a number output as its JSON text, and running again when it changes")
		Expect(getWorkflow("rerun-put-1").Spec.Templates[0].Parameters).To(Equal([]dinovav1alpha1.WorkflowParameter{{Name: "value", Value: "1"}}))
		finishWorkflow("rerun-put-1", "Succeeded", "configure", nil, nil)
		finishInstanceRun("rerun-src", 2, map[string]string{"value": "2"})
		current = test.reconcile()
		Expect(stageStatus(current, "put").Workflow).To(Equal("rerun-put-2"))
		Expect(getWorkflow("rerun-put-2").Spec.Templates[0].Parameters).To(Equal([]dinovav1alpha1.WorkflowParameter{{Name: "value", Value: "2"}}))
	})

	It("refuses sensitive outputs and invalid dependencies", func() {
		createTerraformWorkflowTemplate("pipe-secret")
		createTemplates(map[string]dinovav1alpha1.ApiCallTemplateSpec{"pipe-use": {Method: http.MethodPost, Endpoint: "https://portal.example/use"}})
		createAPIWorkflowTemplate("pipe-use", false, "token")
		test := newPipelineUnderTest(&dinovav1alpha1.WorkflowPipeline{
			ObjectMeta: metav1.ObjectMeta{Name: "secret"},
			Spec: dinovav1alpha1.WorkflowPipelineSpec{Stages: []dinovav1alpha1.WorkflowPipelineStage{
				instanceStage("src", "pipe-secret"),
				workflowStage("use", "pipe-use", []dinovav1alpha1.WorkflowPipelineParameter{fromOutput("token", "src", "token")}, "src"),
			}},
		})
		test.reconcile()
		run := &dinovav1alpha1.Workflow{
			ObjectMeta: metav1.ObjectMeta{Namespace: pipelineNamespace, Name: "secret-src-1",
				Labels: map[string]string{instance.InstanceLabel: "secret-src"}, Annotations: map[string]string{instance.RunAnnotation: "1"}},
			Spec: dinovav1alpha1.WorkflowSpec{Templates: []dinovav1alpha1.WorkflowTemplateRun{{Name: "run", TemplateRef: dinovav1alpha1.WorkflowResourceReference{Name: "pipe-secret"}}}},
		}
		Expect(controllerutil.SetControllerReference(getInstance("secret-src"), run, k8sClient.Scheme())).To(Succeed())
		Expect(k8sClient.Create(ctx, run)).To(Succeed())
		finishWorkflow("secret-src-1", "Succeeded", "apply", nil, []string{"token"})
		setInstanceReady("secret-src")
		current := test.reconcile()
		Expect(stageStatus(current, "use").Phase).To(Equal("Failed"))
		Expect(stageStatus(current, "use").Message).To(ContainSubstring("is sensitive"))
		Expect(workflowExists("secret-use-1")).To(BeFalse())

		By("refusing a stage that reads an output of a stage it does not depend on")
		test.update(func(p *dinovav1alpha1.WorkflowPipeline) { p.Spec.Stages[1].DependsOn = nil })
		current = test.reconcile()
		Expect(pipelineReady(current).Reason).To(Equal("InvalidPipeline"))
		Expect(pipelineReady(current).Message).To(ContainSubstring(`reads an output of stage "src", which the stage does not depend on`))

		By("refusing stages that depend on each other")
		test.update(func(p *dinovav1alpha1.WorkflowPipeline) {
			p.Spec.Stages[1].DependsOn = []string{"src"}
			p.Spec.Stages[0].DependsOn = []string{"use"}
		})
		current = test.reconcile()
		Expect(pipelineReady(current).Message).To(ContainSubstring("stages src, use depend on each other"))
	})

	It("deletes the stages in the opposite order of their dependencies", func() {
		createTerraformWorkflowTemplate("pipe-base")
		createTemplates(map[string]dinovav1alpha1.ApiCallTemplateSpec{"pipe-top": {Method: http.MethodPost, Endpoint: "https://portal.example/top"}})
		createAPIWorkflowTemplate("pipe-top", false)
		test := newPipelineUnderTest(&dinovav1alpha1.WorkflowPipeline{
			ObjectMeta: metav1.ObjectMeta{Name: "gone"},
			Spec: dinovav1alpha1.WorkflowPipelineSpec{Stages: []dinovav1alpha1.WorkflowPipelineStage{
				instanceStage("base", "pipe-base"), workflowStage("top", "pipe-top", nil, "base"),
			}},
		})
		test.reconcile()
		setInstanceReady("gone-base")
		test.reconcile()
		Expect(workflowExists("gone-top-1")).To(BeTrue())

		current := &dinovav1alpha1.WorkflowPipeline{}
		Expect(k8sClient.Get(ctx, test.key, current)).To(Succeed())
		Expect(k8sClient.Delete(ctx, current)).To(Succeed())
		current = test.reconcile()
		Expect(workflowExists("gone-top-1")).To(BeFalse(), "the stage that depends on base goes first")
		getInstance("gone-base")
		Expect(stageStatus(current, "base").Phase).To(Equal("Deleting"))

		current = test.reconcile()
		Expect(apierrors.IsNotFound(k8sClient.Get(ctx, types.NamespacedName{Namespace: pipelineNamespace, Name: "gone-base"}, &dinovav1alpha1.WorkflowInstance{}))).To(BeTrue())
		Expect(current).NotTo(BeNil(), "the pipeline goes once the objects of its stages are gone")
		Expect(test.reconcile()).To(BeNil())
	})
})
