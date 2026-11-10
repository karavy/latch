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
	"maps"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	. "github.com/onsi/gomega/gstruct"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
	"github.com/karavy/latch/internal/controller/terraform"
)

// fakeGit is a GitFetcher that returns files and commit, or err, and counts the reads.
type fakeGit struct {
	files  map[string]string
	commit string
	err    error
	calls  int
	auth   *terraform.GitAuth
}

func (f *fakeGit) Fetch(_ context.Context, _ dinovav1alpha1.TerraformGitSource, auth *terraform.GitAuth) (map[string]string, string, error) {
	f.calls++
	f.auth = auth
	if f.err != nil {
		return nil, "", f.err
	}
	return maps.Clone(f.files), f.commit, nil
}

// reconcileGitTemplate is reconcileTerraformTemplate with git read by fetcher.
func reconcileGitTemplate(name string, fetcher *fakeGit) (*dinovav1alpha1.TerraformTemplate, ctrl.Result) {
	GinkgoHelper()
	reconciler := &TerraformTemplateReconciler{Client: k8sClient, Scheme: k8sClient.Scheme(), Git: fetcher}
	key := types.NamespacedName{Namespace: "default", Name: name}
	result, err := reconciler.Reconcile(ctx, reconcile.Request{NamespacedName: key})
	Expect(err).NotTo(HaveOccurred())
	template := &dinovav1alpha1.TerraformTemplate{}
	Expect(k8sClient.Get(ctx, key, template)).To(Succeed())
	return template, result
}

// createGitTemplate creates a template that reads branch main of a repository and, when
// files is not empty, also the files of the immutable ConfigMap <name>-files.
func createGitTemplate(name string, files map[string]string, secret string) {
	GinkgoHelper()
	template := &dinovav1alpha1.TerraformTemplate{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: dinovav1alpha1.TerraformTemplateSpec{Git: &dinovav1alpha1.TerraformGitSource{
			URL: "https://gitlab.example.com/modules/" + name, Ref: dinovav1alpha1.TerraformGitReference{Branch: "main"},
		}},
	}
	if secret != "" {
		template.Spec.Git.SecretRef = &dinovav1alpha1.TerraformGitSecretReference{Name: secret}
	}
	if len(files) > 0 {
		source := filesConfigMap(name+"-files", files)
		Expect(k8sClient.Create(ctx, source)).To(Succeed())
		DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, source))).To(Succeed()) })
		for fileName := range files {
			template.Spec.Files = append(template.Spec.Files, dinovav1alpha1.TerraformFile{
				Name: fileName, ConfigMapKeyRef: dinovav1alpha1.WorkflowConfigMapKeyReference{Name: source.Name, Key: fileName},
			})
		}
	}
	Expect(k8sClient.Create(ctx, template)).To(Succeed())
	DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, template))).To(Succeed()) })
}

func revisionFiles(template *dinovav1alpha1.TerraformTemplate) map[string]string {
	GinkgoHelper()
	revision := &appsv1.ControllerRevision{}
	Expect(k8sClient.Get(ctx, types.NamespacedName{Namespace: "default", Name: template.Status.RevisionName}, revision)).To(Succeed())
	files, err := terraform.RevisionFiles(revision)
	Expect(err).NotTo(HaveOccurred())
	return files
}

var _ = Describe("TerraformTemplate Controller with a git source", func() {
	const commitA, commitB = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	It("reads git together with spec.files, and the repository only when the source changes", func() {
		createGitTemplate("tf-git", map[string]string{"latch-providers.tf": "# providers 1"}, "")
		fetcher := &fakeGit{files: map[string]string{"main.tf": "# main A"}, commit: commitA}
		template, _ := reconcileGitTemplate("tf-git", fetcher)
		Expect(templateReady(template).Status).To(Equal(metav1.ConditionTrue), templateReady(template).Message)
		Expect(templateReady(template).Message).To(ContainSubstring("git commit aaaaaaaaaaaa"))
		Expect(template.Status.Revision).To(BeEquivalentTo(1))
		Expect(template.Status.Git).To(PointTo(MatchFields(IgnoreExtras, Fields{
			"URL": Equal("https://gitlab.example.com/modules/tf-git"), "Ref": Equal("branch/main"), "Path": Equal("."),
			"Commit": Equal(commitA), "Files": Equal([]string{"main.tf"}),
		})))
		Expect(template.Status.Sources).To(ConsistOf(HaveField("Name", "tf-git-files")))
		Expect(revisionFiles(template)).To(Equal(map[string]string{"main.tf": "# main A", "latch-providers.tf": "# providers 1"}))
		Expect(fetcher.calls).To(Equal(1))
		Expect(fetcher.auth).To(BeNil())

		By("not reading the repository again while the source does not change, even if the branch moved")
		fetcher.files, fetcher.commit = map[string]string{"main.tf": "# main B"}, commitB
		template, _ = reconcileGitTemplate("tf-git", fetcher)
		Expect(fetcher.calls).To(Equal(1))
		Expect(template.Status.Revision).To(BeEquivalentTo(1))

		By("making a revision with the files of the same commit when a ConfigMap changes")
		replaceConfigMap("tf-git-files", map[string]string{"latch-providers.tf": "# providers 2"})
		template, _ = reconcileGitTemplate("tf-git", fetcher)
		Expect(fetcher.calls).To(Equal(1))
		Expect(template.Status.Revision).To(BeEquivalentTo(2))
		Expect(template.Status.Git.Commit).To(Equal(commitA))
		Expect(revisionFiles(template)).To(Equal(map[string]string{"main.tf": "# main A", "latch-providers.tf": "# providers 2"}))

		By("reading the new commit when the annotation dinova.one/git-refresh changes")
		template.Annotations = map[string]string{terraform.GitRefreshAnnotation: "1"}
		Expect(k8sClient.Update(ctx, template)).To(Succeed())
		template, _ = reconcileGitTemplate("tf-git", fetcher)
		Expect(fetcher.calls).To(Equal(2))
		Expect(template.Status.Revision).To(BeEquivalentTo(3))
		Expect(template.Status.Git.Commit).To(Equal(commitB))
		Expect(template.Status.Git.Refresh).To(Equal("1"))
		Expect(revisionFiles(template)).To(HaveKeyWithValue("main.tf", "# main B"))

		By("reading the repository when the ref changes")
		template.Spec.Git.Ref = dinovav1alpha1.TerraformGitReference{Tag: "v2"}
		Expect(k8sClient.Update(ctx, template)).To(Succeed())
		template, _ = reconcileGitTemplate("tf-git", fetcher)
		Expect(fetcher.calls).To(Equal(3))
		Expect(template.Status.Git.Ref).To(Equal("tag/v2"))
	})

	It("is not ready when a file comes from both git and spec.files", func() {
		createGitTemplate("tf-git-duplicate", map[string]string{"main.tf": "# from a ConfigMap"}, "")
		template, result := reconcileGitTemplate("tf-git-duplicate", &fakeGit{files: map[string]string{"main.tf": "# from git"}, commit: commitA})
		Expect(templateReady(template).Status).To(Equal(metav1.ConditionFalse))
		Expect(templateReady(template).Reason).To(Equal("InvalidFiles"))
		Expect(result.RequeueAfter).To(BeZero())
	})

	It("reads the credentials of the Secret and tries again when git cannot be read", func() {
		createGitTemplate("tf-git-auth", nil, "tf-git-auth-credentials")
		fetcher := &fakeGit{files: map[string]string{"main.tf": "# main"}, commit: commitA}

		By("waiting for the Secret")
		template, result := reconcileGitTemplate("tf-git-auth", fetcher)
		Expect(templateReady(template).Reason).To(Equal("GitSecretUnavailable"))
		Expect(result.RequeueAfter).To(Equal(terraform.GitRetryInterval))
		Expect(fetcher.calls).To(BeZero())

		secret := &corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Name: "tf-git-auth-credentials", Namespace: "default"},
			StringData: map[string]string{"username": "deploy-token", "password": "s3cret"},
		}
		Expect(k8sClient.Create(ctx, secret)).To(Succeed())
		DeferCleanup(func() { Expect(client.IgnoreNotFound(k8sClient.Delete(ctx, secret))).To(Succeed()) })

		By("trying again after a failure that may go away")
		fetcher.err = &terraform.SourceError{Reason: "GitFetchFailed", Message: "connection refused", Retry: true}
		template, result = reconcileGitTemplate("tf-git-auth", fetcher)
		Expect(templateReady(template).Reason).To(Equal("GitFetchFailed"))
		Expect(result.RequeueAfter).To(Equal(terraform.GitRetryInterval))
		_, err := terraform.CurrentRevision(template)
		Expect(err).To(HaveOccurred())

		By("making the revision once git answers")
		fetcher.err = nil
		template, result = reconcileGitTemplate("tf-git-auth", fetcher)
		Expect(templateReady(template).Status).To(Equal(metav1.ConditionTrue))
		Expect(result.RequeueAfter).To(BeZero())
		Expect(fetcher.auth).To(Equal(&terraform.GitAuth{Username: "deploy-token", Password: "s3cret"}))
	})

	It("needs git or files, an https URL and a path inside the repository", func() {
		template := &dinovav1alpha1.TerraformTemplate{ObjectMeta: metav1.ObjectMeta{Name: "tf-empty", Namespace: "default"}}
		Expect(k8sClient.Create(ctx, template)).To(MatchError(ContainSubstring("set git, files or both")))
		template.Spec.Git = &dinovav1alpha1.TerraformGitSource{URL: "http://insecure.example.com/r", Ref: dinovav1alpha1.TerraformGitReference{Branch: "main"}}
		Expect(k8sClient.Create(ctx, template)).NotTo(Succeed(), "only https")
		template.Spec.Git = &dinovav1alpha1.TerraformGitSource{URL: "https://example.com/r", Ref: dinovav1alpha1.TerraformGitReference{Branch: "main"}, Path: "../etc"}
		Expect(k8sClient.Create(ctx, template)).NotTo(Succeed(), "no .. in path")
		template.Spec.Git = &dinovav1alpha1.TerraformGitSource{URL: "https://example.com/r", Ref: dinovav1alpha1.TerraformGitReference{Branch: "main", Tag: "v1"}}
		Expect(k8sClient.Create(ctx, template)).To(MatchError(ContainSubstring("branch and tag cannot be used together")))
	})
})
