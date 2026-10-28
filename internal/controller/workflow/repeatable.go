package workflow

import (
	"context"
	"fmt"
	"net/http"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	dinovav1alpha1 "github.com/karavy/latch/api/v1alpha1"
)

// UnrepeatableStep says which API step of a template is not safe to send again, if any: one
// whose request is not a GET and that is not marked idempotent. Empty when every API step can
// be repeated, so that the template can run again and again (WorkflowInstance) or again when its
// values change (WorkflowPipeline rerunOnChange). Only the step's own request counts: a poll
// reads, and a recover request is sent only after a failure of the same run.
func UnrepeatableStep(ctx context.Context, reader client.Reader, namespace string, template *dinovav1alpha1.WorkflowTemplate) (string, error) {
	for _, step := range template.Spec.Steps {
		if step.API == nil || step.API.Idempotent {
			continue
		}
		request := &dinovav1alpha1.ApiCallTemplate{}
		err := reader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: step.API.TemplateRef.Name}, request)
		switch {
		case apierrors.IsNotFound(err):
			return fmt.Sprintf("the ApiCallTemplate %q of step %q was not found", step.API.TemplateRef.Name, step.Name), nil
		case err != nil:
			return "", err
		case request.Spec.Method != http.MethodGet:
			return fmt.Sprintf("step %q of WorkflowTemplate %q sends a %s and is not marked idempotent", step.Name, template.Name, request.Spec.Method), nil
		}
	}
	return "", nil
}

// APICallTemplateNames returns the ApiCallTemplates the steps of a template send: the request
// of each API step, of its poll and of its recover.
func APICallTemplateNames(template *dinovav1alpha1.WorkflowTemplate) []string {
	var names []string
	for _, step := range template.Spec.Steps {
		if step.API == nil {
			continue
		}
		names = append(names, step.API.TemplateRef.Name)
		if poll := step.API.Poll; poll != nil {
			if poll.TemplateRef.Name != "" {
				names = append(names, poll.TemplateRef.Name)
			}
			if poll.Recover != nil {
				names = append(names, poll.Recover.TemplateRef.Name)
			}
		}
	}
	return names
}
