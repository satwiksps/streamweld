package v1alpha1

import (
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8svalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
)

// Validate checks an InferenceRoute independently of admission webhooks.
func (route *InferenceRoute) Validate() error {
	return route.ValidationErrors().ToAggregate()
}

// ValidationErrors returns API-style field errors suitable for a webhook.
func (route *InferenceRoute) ValidationErrors() field.ErrorList {
	if route == nil {
		return field.ErrorList{field.Required(field.NewPath("object"), "InferenceRoute is required")}
	}
	specPath := field.NewPath("spec")
	var problems field.ErrorList
	if strings.TrimSpace(route.Spec.Model) == "" {
		problems = append(problems, field.Required(specPath.Child("model"), "model must not be blank"))
	} else if route.Spec.Model != strings.TrimSpace(route.Spec.Model) {
		problems = append(problems, field.Invalid(specPath.Child("model"), route.Spec.Model, "model must not have surrounding whitespace"))
	}
	selectorPath := specPath.Child("backends", "selector")
	selector := route.Spec.Backends.Selector
	if len(selector.MatchLabels) == 0 && len(selector.MatchExpressions) == 0 {
		problems = append(problems, field.Required(selectorPath, "selector must contain matchLabels or matchExpressions"))
	} else if _, err := metav1.LabelSelectorAsSelector(&selector); err != nil {
		problems = append(problems, field.Invalid(selectorPath, selector, err.Error()))
	}
	portPath := specPath.Child("backends", "port")
	if route.Spec.Backends.Port < 1 || route.Spec.Backends.Port > 65535 {
		problems = append(problems, field.Invalid(portPath, route.Spec.Backends.Port, "port must be between 1 and 65535"))
	}
	policyNamePath := specPath.Child("policyRef", "name")
	if route.Spec.PolicyRef.Name == "" {
		problems = append(problems, field.Required(policyNamePath, "policy name is required"))
	} else if messages := k8svalidation.IsDNS1123Subdomain(route.Spec.PolicyRef.Name); len(messages) != 0 {
		problems = append(problems, field.Invalid(policyNamePath, route.Spec.PolicyRef.Name, strings.Join(messages, "; ")))
	}
	return problems
}

// Validate checks a DurabilityPolicy using Kubernetes default semantics without
// mutating the receiver.
func (policy *DurabilityPolicy) Validate() error {
	return policy.ValidationErrors().ToAggregate()
}

// ValidationErrors returns API-style field errors suitable for a webhook.
func (policy *DurabilityPolicy) ValidationErrors() field.ErrorList {
	if policy == nil {
		return field.ErrorList{field.Required(field.NewPath("object"), "DurabilityPolicy is required")}
	}
	spec := policy.Spec.WithDefaults()
	path := field.NewPath("spec")
	var problems field.ErrorList
	if *spec.MaxMigrations < 0 {
		problems = append(problems, field.Invalid(path.Child("maxMigrations"), *spec.MaxMigrations, "must be non-negative"))
	}
	if *spec.MaxMigrationTokens < 0 {
		problems = append(problems, field.Invalid(path.Child("maxMigrationTokens"), *spec.MaxMigrationTokens, "must be non-negative"))
	}
	if spec.MaxStreamDuration.Duration <= 0 {
		problems = append(problems, field.Invalid(path.Child("maxStreamDuration"), spec.MaxStreamDuration.Duration.String(), "must be positive"))
	}
	if !spec.OrphanPolicy.Valid() {
		problems = append(problems, field.NotSupported(
			path.Child("orphanPolicy"), spec.OrphanPolicy,
			[]string{string(OrphanContinue), string(OrphanCancelAfter), string(OrphanCancel)},
		))
	}
	if spec.OrphanTimeout.Duration <= 0 {
		problems = append(problems, field.Invalid(path.Child("orphanTimeout"), spec.OrphanTimeout.Duration.String(), "must be positive"))
	}
	if *spec.SeamWindowBytes <= 0 {
		problems = append(problems, field.Invalid(path.Child("seamWindowBytes"), *spec.SeamWindowBytes, "must be positive"))
	}
	if spec.JournalTTL.Duration <= 0 {
		problems = append(problems, field.Invalid(path.Child("journalTTL"), spec.JournalTTL.Duration.String(), "must be positive"))
	}
	return problems
}
