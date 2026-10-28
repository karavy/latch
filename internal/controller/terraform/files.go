// Package terraform holds the Kubernetes side of a Workflow Terraform step: turning a
// TerraformTemplate into the files a Job mounts, and building that Job. Rendering the
// templates and running init/plan/apply is done by terraform-engine inside the Job.
package terraform

import (
	"fmt"
	"regexp"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
)

// fileName mirrors the CRD validation of TerraformFile.Name, which is re-checked here
// because the files end up as paths inside the runner container.
var fileName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*\.tf(\.tmpl)?$`)

// ConfigMapData returns the files of a TerraformTemplate keyed by file name, ready to be
// stored in the ConfigMap that the Job mounts as its template directory. Each file is read with
// lookup, and the error of the lookup is returned as it is.
func ConfigMapData(template *dinovav1alpha1.TerraformTemplate, lookup func(dinovav1alpha1.WorkflowConfigMapKeyReference) (string, error)) (map[string]string, error) {
	if len(template.Spec.Files) == 0 {
		return nil, fmt.Errorf("TerraformTemplate %q has no files", template.Name)
	}
	data := make(map[string]string, len(template.Spec.Files))
	for _, file := range template.Spec.Files {
		if !fileName.MatchString(file.Name) {
			return nil, fmt.Errorf("TerraformTemplate %q has an invalid file name %q", template.Name, file.Name)
		}
		if _, duplicate := data[file.Name]; duplicate {
			return nil, fmt.Errorf("TerraformTemplate %q repeats file %q", template.Name, file.Name)
		}
		content, err := lookup(file.ConfigMapKeyRef)
		if err != nil {
			return nil, err
		}
		if content == "" {
			return nil, fmt.Errorf("TerraformTemplate %q has an empty file %q", template.Name, file.Name)
		}
		data[file.Name] = content
	}
	return data, nil
}
