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
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
	"github.com/karavy/latch/internal/controller/terraform"
)

// filesConfigMap is the immutable ConfigMap that holds files, one key for each file name.
func filesConfigMap(name string, files map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Data:       files,
		Immutable:  new(true),
	}
}

// reconcileTerraformTemplate makes the revision of the current files of a template, like the
// running operator does after any change of the template or of its ConfigMaps.
func reconcileTerraformTemplate(name string) *dinovav1alpha1.TerraformTemplate {
	GinkgoHelper()
	reconciler := &TerraformTemplateReconciler{Client: k8sClient, Scheme: k8sClient.Scheme()}
	key := types.NamespacedName{Namespace: "default", Name: name}
	_, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
	Expect(err).NotTo(HaveOccurred())
	template := &dinovav1alpha1.TerraformTemplate{}
	Expect(k8sClient.Get(ctx, key, template)).To(Succeed())
	return template
}

// createTerraformTemplate creates a TerraformTemplate that reads files from the immutable
// ConfigMap <name>-files, and makes its first revision.
func createTerraformTemplate(name string, files map[string]string) *dinovav1alpha1.TerraformTemplate {
	GinkgoHelper()
	source := filesConfigMap(name+"-files", files)
	Expect(k8sClient.Create(ctx, source)).To(Succeed())
	DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, source))).To(Succeed()) })
	template := &dinovav1alpha1.TerraformTemplate{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}}
	for fileName := range files {
		template.Spec.Files = append(template.Spec.Files, dinovav1alpha1.TerraformFile{
			Name: fileName, ConfigMapKeyRef: dinovav1alpha1.WorkflowConfigMapKeyReference{Name: source.Name, Key: fileName},
		})
	}
	Expect(k8sClient.Create(ctx, template)).To(Succeed())
	DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, template))).To(Succeed()) })
	return reconcileTerraformTemplate(name)
}

// replaceConfigMap deletes a ConfigMap and creates it again with other files: the only way to
// change an immutable ConfigMap.
func replaceConfigMap(name string, files map[string]string) {
	GinkgoHelper()
	Expect(k8sClient.Delete(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"}})).To(Succeed())
	Expect(k8sClient.Create(ctx, filesConfigMap(name, files))).To(Succeed())
}

func templateRevisions(name string) []appsv1.ControllerRevision {
	GinkgoHelper()
	list := &appsv1.ControllerRevisionList{}
	Expect(k8sClient.List(ctx, list, client.InNamespace("default"), client.MatchingLabels{terraform.TemplateLabel: name})).To(Succeed())
	return list.Items
}

func templateReady(template *dinovav1alpha1.TerraformTemplate) *metav1.Condition {
	return meta.FindStatusCondition(template.Status.Conditions, "Ready")
}

var _ = Describe("TerraformTemplate Controller", func() {
	It("makes a numbered revision every time the content of the files changes", func() {
		template := createTerraformTemplate("tf-revisions", map[string]string{"main.tf": "terraform {}"})
		Expect(template.Status.Revision).To(BeEquivalentTo(1))
		Expect(template.Status.Digest).To(Equal(terraform.Digest(map[string]string{"main.tf": "terraform {}"})))
		Expect(template.Status.Sources).To(ConsistOf(HaveField("Name", "tf-revisions-files")))
		Expect(templateReady(template).Status).To(Equal(metav1.ConditionTrue))
		first := template.Status.RevisionName
		revision := &appsv1.ControllerRevision{}
		Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: first}, revision)).To(Succeed())
		Expect(terraform.RevisionFiles(revision)).To(Equal(map[string]string{"main.tf": "terraform {}"}))
		Expect(metav1.IsControlledBy(revision, template)).To(BeTrue())

		By("keeping the revision while nothing changes")
		Expect(reconcileTerraformTemplate("tf-revisions").Status.Revision).To(BeEquivalentTo(1))
		Expect(templateRevisions("tf-revisions")).To(HaveLen(1))

		By("making revision 2 when the ConfigMap is created again with other files")
		replaceConfigMap("tf-revisions-files", map[string]string{"main.tf": `resource "terraform_data" "x" {}`})
		template = reconcileTerraformTemplate("tf-revisions")
		Expect(template.Status.Revision).To(BeEquivalentTo(2))
		Expect(template.Status.RevisionName).NotTo(Equal(first))
		Expect(templateRevisions("tf-revisions")).To(HaveLen(2))

		By("not making a revision when the ConfigMap is created again with the same files")
		uid := template.Status.Sources[0].UID
		replaceConfigMap("tf-revisions-files", map[string]string{"main.tf": `resource "terraform_data" "x" {}`})
		template = reconcileTerraformTemplate("tf-revisions")
		Expect(template.Status.Revision).To(BeEquivalentTo(2))
		Expect(template.Status.Sources[0].UID).NotTo(Equal(uid), "the status names the ConfigMap the revision was read from")

		By("bringing back revision 1 when the files are those of revision 1 again")
		replaceConfigMap("tf-revisions-files", map[string]string{"main.tf": "terraform {}"})
		template = reconcileTerraformTemplate("tf-revisions")
		Expect(template.Status.Revision).To(BeEquivalentTo(1))
		Expect(template.Status.RevisionName).To(Equal(first))
	})

	It("keeps only the newest revisions", func() {
		template := createTerraformTemplate("tf-history", map[string]string{"main.tf": "# 1"})
		template.Spec.RevisionHistoryLimit = 2
		Expect(k8sClient.Update(ctx, template)).To(Succeed())
		for _, content := range []string{"# 2", "# 3", "# 4"} {
			replaceConfigMap("tf-history-files", map[string]string{"main.tf": content})
			template = reconcileTerraformTemplate("tf-history")
		}
		Expect(template.Status.Revision).To(BeEquivalentTo(4))
		revisions := templateRevisions("tf-history")
		numbers := make([]int64, 0, len(revisions))
		for _, revision := range revisions {
			numbers = append(numbers, revision.Revision)
		}
		Expect(numbers).To(ConsistOf(int64(3), int64(4)))

		By("never going back to a number already given, even after its revision was deleted")
		replaceConfigMap("tf-history-files", map[string]string{"main.tf": "# 1"})
		Expect(reconcileTerraformTemplate("tf-history").Status.Revision).To(BeEquivalentTo(5))
	})

	It("is not ready, and keeps its revision, when the files cannot be read", func() {
		template := createTerraformTemplate("tf-not-ready", map[string]string{"main.tf": "terraform {}"})
		revision := template.Status.RevisionName
		for reason, files := range map[string]*corev1.ConfigMap{
			"ConfigMapNotImmutable": {ObjectMeta: metav1.ObjectMeta{Name: "tf-not-ready-files", Namespace: "default"}, Data: map[string]string{"main.tf": "x"}},
			"ConfigMapKeyMissing":   filesConfigMap("tf-not-ready-files", map[string]string{"other.tf": "x"}),
			"ConfigMapUnavailable":  nil,
		} {
			Expect(k8sClient.Delete(ctx, &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "tf-not-ready-files", Namespace: "default"}})).To(Succeed())
			if files != nil {
				Expect(k8sClient.Create(ctx, files)).To(Succeed())
			}
			template = reconcileTerraformTemplate("tf-not-ready")
			Expect(templateReady(template).Status).To(Equal(metav1.ConditionFalse), reason)
			Expect(templateReady(template).Reason).To(Equal(reason))
			Expect(template.Status.RevisionName).To(Equal(revision), "the last good revision stays in the status")
			_, err := terraform.CurrentRevision(template)
			Expect(err).To(HaveOccurred(), "a Workflow must not run a template that is not ready")
			if files == nil {
				Expect(k8sClient.Create(ctx, filesConfigMap("tf-not-ready-files", map[string]string{"main.tf": "terraform {}"}))).To(Succeed())
			}
		}
	})

	It("validates TerraformTemplate files in the generated CRD", func() {
		newTemplate := func(files ...dinovav1alpha1.TerraformFile) *dinovav1alpha1.TerraformTemplate {
			return &dinovav1alpha1.TerraformTemplate{
				ObjectMeta: metav1.ObjectMeta{GenerateName: "tf-invalid-", Namespace: "default"},
				Spec:       dinovav1alpha1.TerraformTemplateSpec{Files: files},
			}
		}
		file := func(name string) dinovav1alpha1.TerraformFile {
			return dinovav1alpha1.TerraformFile{Name: name, ConfigMapKeyRef: dinovav1alpha1.WorkflowConfigMapKeyReference{Name: "files", Key: "main"}}
		}
		for name, template := range map[string]*dinovav1alpha1.TerraformTemplate{
			"no files":          newTemplate(),
			"path traversal":    newTemplate(file("../main.tf")),
			"subdirectory":      newTemplate(file("a/main.tf")),
			"not terraform":     newTemplate(file("run.sh")),
			"no ConfigMap":      newTemplate(dinovav1alpha1.TerraformFile{Name: "main.tf"}),
			"duplicate names":   newTemplate(file("main.tf"), file("main.tf")),
			"history too short": {ObjectMeta: metav1.ObjectMeta{GenerateName: "tf-invalid-", Namespace: "default"}, Spec: dinovav1alpha1.TerraformTemplateSpec{Files: []dinovav1alpha1.TerraformFile{file("main.tf")}, RevisionHistoryLimit: -1}},
		} {
			Expect(k8sClient.Create(ctx, template)).NotTo(Succeed(), name)
		}
		ok := newTemplate(file("main.tf"), file("vars.tf.tmpl"))
		Expect(k8sClient.Create(ctx, ok)).To(Succeed())
		Expect(ok.Spec.RevisionHistoryLimit).To(BeEquivalentTo(10), "the default history limit")
		Expect(k8sClient.Delete(ctx, ok)).To(Succeed())
	})
})
