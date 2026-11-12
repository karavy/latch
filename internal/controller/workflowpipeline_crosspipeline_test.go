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

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
	"github.com/karavy/latch/internal/controller/pipeline"
)

// fromPipeline reads an output of a step of a stage of another pipeline.
func fromPipeline(name, other, stage, step, output string) dinovav1alpha1.WorkflowPipelineParameter {
	return dinovav1alpha1.WorkflowPipelineParameter{Name: name, ValueFrom: &dinovav1alpha1.WorkflowPipelineOutputReference{
		Pipeline: other, Stage: stage, Step: step, Output: output,
	}}
}

var _ = Describe("WorkflowPipeline reading another pipeline", func() {
	It("waits for the other pipeline, passes its output, and keeps it until its readers are gone", func() {
		createTemplates(map[string]dinovav1alpha1.ApiCallTemplateSpec{
			"xp-deliver": {Method: http.MethodPost, Endpoint: "https://portal.example/deliver"},
			"xp-connect": {Method: http.MethodPost, Endpoint: "https://portal.example/connect"},
		})
		createAPIWorkflowTemplate("xp-deliver", false)
		createAPIWorkflowTemplate("xp-connect", false, "deliveryJobId", "nicIndex")
		tenantSpec := dinovav1alpha1.WorkflowPipelineSpec{Stages: []dinovav1alpha1.WorkflowPipelineStage{workflowStage("fgt", "xp-deliver", nil)}}

		By("waiting, without dependsOn, while the other pipeline does not exist")
		value := "2"
		cluster := newPipelineUnderTest(&dinovav1alpha1.WorkflowPipeline{
			ObjectMeta: metav1.ObjectMeta{Name: "xp-cluster"},
			Spec: dinovav1alpha1.WorkflowPipelineSpec{Stages: []dinovav1alpha1.WorkflowPipelineStage{
				workflowStage("connect", "xp-connect", []dinovav1alpha1.WorkflowPipelineParameter{
					fromPipeline("deliveryJobId", "xp-tenant", "fgt", "deliver", "id"),
					{Name: "nicIndex", Value: &value},
				}),
			}},
		})
		current := cluster.reconcile()
		Expect(stageStatus(current, "connect")).To(And(HaveField("Phase", "Pending"),
			HaveField("Message", ContainSubstring("Waiting for pipeline xp-tenant, for output id of step deliver of its stage fgt"))))
		Expect(pipelineReady(current).Reason).To(Equal("InProgress"))
		Expect(workflowExists("xp-cluster-connect-1")).To(BeFalse())
		Expect(pipeline.ReadPipelines(current)).To(Equal([]string{"xp-tenant"}))

		By("waiting while the stage of the other pipeline runs")
		tenant := newPipelineUnderTest(&dinovav1alpha1.WorkflowPipeline{ObjectMeta: metav1.ObjectMeta{Name: "xp-tenant"}, Spec: tenantSpec})
		tenant.reconcile()
		Expect(workflowExists("xp-tenant-fgt-1")).To(BeTrue())
		current = cluster.reconcile()
		Expect(stageStatus(current, "connect")).To(And(HaveField("Phase", "Pending"),
			HaveField("Message", ContainSubstring("Waiting for stage fgt of pipeline xp-tenant to succeed"))))

		By("starting with the output once that stage succeeded")
		finishWorkflow("xp-tenant-fgt-1", "Succeeded", "deliver", map[string]string{"id": "7"}, nil)
		Expect(pipelineReady(tenant.reconcile()).Status).To(Equal(metav1.ConditionTrue))
		current = cluster.reconcile()
		Expect(getWorkflow("xp-cluster-connect-1").Spec.Templates[0].Parameters).To(Equal([]dinovav1alpha1.WorkflowParameter{
			{Name: "deliveryJobId", Value: "7"}, {Name: "nicIndex", Value: "2"},
		}))
		finishWorkflow("xp-cluster-connect-1", "Succeeded", "configure", nil, nil)
		Expect(stageStatus(cluster.reconcile(), "connect").Phase).To(Equal("Ready"))

		By("marking the stage Outdated when the output of the other pipeline changes")
		tenant.update(func(p *dinovav1alpha1.WorkflowPipeline) { p.Spec.Rerun = []string{"xp-tenant-fgt-1"} })
		tenant.reconcile()
		finishWorkflow("xp-tenant-fgt-2", "Succeeded", "deliver", map[string]string{"id": "8"}, nil)
		tenant.reconcile()
		connect := stageStatus(cluster.reconcile(), "connect")
		Expect(connect.Phase).To(Equal("Outdated"))
		Expect(connect.Changed).To(Equal([]dinovav1alpha1.WorkflowPipelineChangedParameter{{Name: "deliveryJobId", Old: "7", New: "8"}}))
		Expect(workflowExists("xp-cluster-connect-2")).To(BeFalse())

		By("keeping the other pipeline and its stages while a pipeline reads it")
		Expect(k8sClient.Delete(ctx, tenant.reconcile())).To(Succeed())
		current = tenant.reconcile()
		Expect(current).NotTo(BeNil())
		Expect(pipelineReady(current)).To(And(HaveField("Reason", "ReadByPipelines"),
			HaveField("Message", ContainSubstring("Read by pipelines xp-cluster"))))
		Expect(stageStatus(current, "fgt").Phase).To(Equal("Deleting"))
		Expect(workflowExists("xp-tenant-fgt-2")).To(BeTrue(), "the stages of a pipeline that is read are not deleted")
		Expect(stageStatus(cluster.reconcile(), "connect")).To(And(HaveField("Phase", "Failed"),
			HaveField("Message", "Pipeline xp-tenant, whose output id of step deliver of stage fgt this stage reads, is being deleted")),
			"a Workflow stage that already ran shows that it reads a pipeline being deleted")

		By("deleting it once its readers are gone")
		Expect(k8sClient.Delete(ctx, cluster.reconcile())).To(Succeed())
		cluster.reconcile()
		Expect(workflowExists("xp-cluster-connect-1")).To(BeFalse())
		Expect(cluster.reconcile()).To(BeNil())
		tenant.reconcile()
		Expect(workflowExists("xp-tenant-fgt-2")).To(BeFalse())
		Expect(tenant.reconcile()).To(BeNil())
	})

	It("fails a stage that reads a stage the other pipeline does not have, and needs dependsOn in its own pipeline", func() {
		createTemplates(map[string]dinovav1alpha1.ApiCallTemplateSpec{"xp-use": {Method: http.MethodPost, Endpoint: "https://portal.example/use"}})
		createAPIWorkflowTemplate("xp-use", false, "id")
		newPipelineUnderTest(&dinovav1alpha1.WorkflowPipeline{
			ObjectMeta: metav1.ObjectMeta{Name: "xp-other"},
			Spec:       dinovav1alpha1.WorkflowPipelineSpec{Stages: []dinovav1alpha1.WorkflowPipelineStage{workflowStage("real", "xp-use", nil)}},
		})
		reader := newPipelineUnderTest(&dinovav1alpha1.WorkflowPipeline{
			ObjectMeta: metav1.ObjectMeta{Name: "xp-reader"},
			Spec: dinovav1alpha1.WorkflowPipelineSpec{Stages: []dinovav1alpha1.WorkflowPipelineStage{
				workflowStage("use", "xp-use", []dinovav1alpha1.WorkflowPipelineParameter{fromPipeline("id", "xp-other", "missing", "s", "id")}),
			}},
		})
		current := reader.reconcile()
		Expect(stageStatus(current, "use")).To(And(HaveField("Phase", "Failed"), HaveField("Message", "Pipeline xp-other has no stage missing")))

		By("treating its own name as this pipeline, where dependsOn is required")
		reader.update(func(p *dinovav1alpha1.WorkflowPipeline) {
			p.Spec.Stages = append(p.Spec.Stages, workflowStage("src", "xp-use", nil))
			p.Spec.Stages[0].Parameters = []dinovav1alpha1.WorkflowPipelineParameter{fromPipeline("id", "xp-reader", "src", "s", "id")}
		})
		current = reader.reconcile()
		Expect(pipelineReady(current).Message).To(ContainSubstring(`reads an output of stage "src", which the stage does not depend on`))
		Expect(pipeline.ReadPipelines(current)).To(BeEmpty())
	})

	It("refuses pipelines that read each other, and deletes them in order once the cycle is broken", func() {
		createTemplates(map[string]dinovav1alpha1.ApiCallTemplateSpec{"xc-use": {Method: http.MethodPost, Endpoint: "https://portal.example/use"}})
		createAPIWorkflowTemplate("xc-use", false, "id")
		source := newPipelineUnderTest(&dinovav1alpha1.WorkflowPipeline{
			ObjectMeta: metav1.ObjectMeta{Name: "xc-b"},
			Spec:       dinovav1alpha1.WorkflowPipelineSpec{Stages: []dinovav1alpha1.WorkflowPipelineStage{workflowStage("src", "xc-use", nil)}},
		})
		reader := newPipelineUnderTest(&dinovav1alpha1.WorkflowPipeline{
			ObjectMeta: metav1.ObjectMeta{Name: "xc-a"},
			Spec: dinovav1alpha1.WorkflowPipelineSpec{Stages: []dinovav1alpha1.WorkflowPipelineStage{
				workflowStage("use", "xc-use", []dinovav1alpha1.WorkflowPipelineParameter{fromPipeline("id", "xc-b", "src", "configure", "id")}),
			}},
		})
		source.reconcile()
		finishWorkflow("xc-b-src-1", "Succeeded", "configure", map[string]string{"id": "1"}, nil)
		source.reconcile()
		reader.reconcile()
		Expect(workflowExists("xc-a-use-1")).To(BeTrue())

		By("refusing both once they read each other")
		source.update(func(p *dinovav1alpha1.WorkflowPipeline) {
			p.Spec.Stages[0].Parameters = []dinovav1alpha1.WorkflowPipelineParameter{fromPipeline("id", "xc-a", "use", "configure", "id")}
		})
		Expect(pipelineReady(source.reconcile())).To(And(HaveField("Reason", "InvalidPipeline"),
			HaveField("Message", ContainSubstring("Pipelines xc-b, xc-a, xc-b read each other's outputs"))))
		Expect(pipelineReady(reader.reconcile())).To(And(HaveField("Reason", "InvalidPipeline"),
			HaveField("Message", ContainSubstring("Pipelines xc-a, xc-b, xc-a read each other's outputs"))))

		By("deleting nothing while the cycle stands")
		Expect(k8sClient.Delete(ctx, reader.reconcile())).To(Succeed())
		Expect(k8sClient.Delete(ctx, source.reconcile())).To(Succeed())
		Expect(pipelineReady(reader.reconcile()).Reason).To(Equal("ReadCycle"))
		Expect(pipelineReady(source.reconcile())).To(And(HaveField("Reason", "ReadCycle"),
			HaveField("Message", ContainSubstring("remove valueFrom.pipeline from one of them"))))
		Expect(workflowExists("xc-a-use-1")).To(BeTrue())
		Expect(workflowExists("xc-b-src-1")).To(BeTrue())

		By("deleting the reader first, then the pipeline it read, once the cycle is broken")
		source.update(func(p *dinovav1alpha1.WorkflowPipeline) { p.Spec.Stages[0].Parameters = nil })
		Expect(pipelineReady(source.reconcile()).Reason).To(Equal("ReadByPipelines"))
		Expect(workflowExists("xc-b-src-1")).To(BeTrue())
		reader.reconcile()
		Expect(workflowExists("xc-a-use-1")).To(BeFalse())
		Expect(reader.reconcile()).To(BeNil())
		source.reconcile()
		Expect(workflowExists("xc-b-src-1")).To(BeFalse())
		Expect(source.reconcile()).To(BeNil())
	})
})
