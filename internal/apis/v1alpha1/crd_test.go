package v1alpha1

import (
	"os"
	"path/filepath"
	"testing"

	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	structuralschema "k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	"sigs.k8s.io/yaml"
)

func TestCRDManifestsAreValidAndMatchAPIContract(t *testing.T) {
	t.Parallel()
	tests := []struct {
		file string
		name string
		kind string
	}{
		{"streamweld.io_inferenceroutes.yaml", "inferenceroutes.streamweld.io", "InferenceRoute"},
		{"streamweld.io_durabilitypolicies.yaml", "durabilitypolicies.streamweld.io", "DurabilityPolicy"},
	}
	for _, test := range tests {
		test := test
		t.Run(test.kind, func(t *testing.T) {
			t.Parallel()
			crd := readCRD(t, test.file)
			if crd.Name != test.name || crd.Spec.Group != GroupName || crd.Spec.Scope != apiextensionsv1.NamespaceScoped {
				t.Fatalf("CRD identity = name %q, group %q, scope %q", crd.Name, crd.Spec.Group, crd.Spec.Scope)
			}
			if crd.Spec.Names.Kind != test.kind || crd.Spec.Names.ListKind != test.kind+"List" {
				t.Fatalf("CRD names = %+v", crd.Spec.Names)
			}
			if len(crd.Spec.Versions) != 1 {
				t.Fatalf("versions = %d, want 1", len(crd.Spec.Versions))
			}
			version := crd.Spec.Versions[0]
			if version.Name != Version || !version.Served || !version.Storage || version.Subresources == nil || version.Subresources.Status == nil {
				t.Fatalf("version contract = %+v", version)
			}
			if version.Schema == nil || version.Schema.OpenAPIV3Schema == nil {
				t.Fatal("CRD has no OpenAPI v3 schema")
			}

			internalSchema := new(apiextensions.JSONSchemaProps)
			if err := apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(
				version.Schema.OpenAPIV3Schema, internalSchema, nil,
			); err != nil {
				t.Fatalf("convert OpenAPI schema to internal API: %v", err)
			}
			structural, err := structuralschema.NewStructural(internalSchema)
			if err != nil {
				t.Fatalf("build Kubernetes structural schema: %v", err)
			}
			if problems := structuralschema.ValidateStructural(field.NewPath("openAPIV3Schema"), structural); len(problems) != 0 {
				t.Fatalf("Kubernetes rejected structural schema:\n%v", problems.ToAggregate())
			}
		})
	}
}

func TestInferenceRouteCRDSchemaContract(t *testing.T) {
	t.Parallel()
	crd := readCRD(t, "streamweld.io_inferenceroutes.yaml")
	root := crd.Spec.Versions[0].Schema.OpenAPIV3Schema
	spec := requiredProperty(t, root, "spec")
	assertRequiredFields(t, spec, "model", "backends", "policyRef")
	backends := requiredProperty(t, spec, "backends")
	assertRequiredFields(t, backends, "selector", "port")
	port := requiredProperty(t, backends, "port")
	if port.Minimum == nil || *port.Minimum != 1 || port.Maximum == nil || *port.Maximum != 65535 {
		t.Fatalf("port bounds = minimum %v, maximum %v", port.Minimum, port.Maximum)
	}
	selector := requiredProperty(t, backends, "selector")
	if len(selector.XValidations) == 0 {
		t.Fatal("selector has no non-empty CEL validation")
	}
	if selector.XMapType == nil || *selector.XMapType != "atomic" {
		t.Fatalf("selector map semantics = %v, want atomic", selector.XMapType)
	}
	policyRef := requiredProperty(t, spec, "policyRef")
	assertRequiredFields(t, policyRef, "name")
	if policyRef.XMapType == nil || *policyRef.XMapType != "atomic" {
		t.Fatalf("policyRef map semantics = %v, want atomic", policyRef.XMapType)
	}

	status := requiredProperty(t, root, "status")
	for _, name := range []string{
		"healthyBackends", "drainingBackends", "templateVerdict", "templateProbedAt",
		"activeStreams", "observedGeneration", "conditions", "backends",
	} {
		requiredProperty(t, status, name)
	}
	backendStatuses := requiredProperty(t, status, "backends")
	if backendStatuses.XListType == nil || *backendStatuses.XListType != "map" ||
		len(backendStatuses.XListMapKeys) != 1 || backendStatuses.XListMapKeys[0] != "id" {
		t.Fatalf("status.backends list semantics = type %v, keys %v", backendStatuses.XListType, backendStatuses.XListMapKeys)
	}
	if backendStatuses.Items == nil || backendStatuses.Items.Schema == nil {
		t.Fatal("status.backends has no item schema")
	}
	assertRequiredFields(t, backendStatuses.Items.Schema, "id", "address", "ready", "draining", "templateVerdict")
	for _, name := range []string{"message", "lastProbedAt", "imageDigest", "tokenizerHash"} {
		requiredProperty(t, backendStatuses.Items.Schema, name)
	}
}

func TestDurabilityPolicyCRDDefaultsAndConstraints(t *testing.T) {
	t.Parallel()
	crd := readCRD(t, "streamweld.io_durabilitypolicies.yaml")
	spec := requiredProperty(t, crd.Spec.Versions[0].Schema.OpenAPIV3Schema, "spec")
	wants := map[string]string{
		"maxMigrations":         "3",
		"maxMigrationTokens":    "8192",
		"maxStreamDuration":     `"15m"`,
		"orphanPolicy":          `"continue"`,
		"orphanTimeout":         `"60s"`,
		"allowCrossVersion":     "false",
		"allowStructuredResume": "false",
		"seamWindowBytes":       "64",
		"journalTTL":            `"10m"`,
	}
	if len(spec.Properties) != len(wants) {
		t.Fatalf("policy spec has %d fields, want exactly %d: %v", len(spec.Properties), len(wants), propertyNames(spec))
	}
	for fieldName, want := range wants {
		property := requiredProperty(t, spec, fieldName)
		if property.Default == nil {
			t.Errorf("%s has no Kubernetes default", fieldName)
			continue
		}
		if got := string(property.Default.Raw); got != want {
			t.Errorf("%s default = %s, want %s", fieldName, got, want)
		}
	}
	for _, name := range []string{"maxMigrations", "maxMigrationTokens"} {
		property := requiredProperty(t, spec, name)
		if property.Minimum == nil || *property.Minimum != 0 {
			t.Errorf("%s minimum = %v, want 0", name, property.Minimum)
		}
	}
	seam := requiredProperty(t, spec, "seamWindowBytes")
	if seam.Minimum == nil || *seam.Minimum != 1 {
		t.Errorf("seamWindowBytes minimum = %v, want 1", seam.Minimum)
	}
	if got := requiredProperty(t, spec, "orphanPolicy").Enum; len(got) != 3 {
		t.Errorf("orphanPolicy enum = %v, want three policies", got)
	}
	for _, name := range []string{"maxStreamDuration", "orphanTimeout", "journalTTL"} {
		if validations := requiredProperty(t, spec, name).XValidations; len(validations) == 0 {
			t.Errorf("%s has no positive-duration CEL validation", name)
		}
	}
}

func readCRD(t *testing.T, fileName string) *apiextensionsv1.CustomResourceDefinition {
	t.Helper()
	path := filepath.Join("..", "..", "..", "deploy", "helm", "streamweld", "crds", fileName)
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	crd := new(apiextensionsv1.CustomResourceDefinition)
	if err := yaml.UnmarshalStrict(contents, crd); err != nil {
		t.Fatalf("decode %s: %v", path, err)
	}
	return crd
}

func requiredProperty(t *testing.T, schema *apiextensionsv1.JSONSchemaProps, name string) *apiextensionsv1.JSONSchemaProps {
	t.Helper()
	property, ok := schema.Properties[name]
	if !ok {
		t.Fatalf("schema property %q is missing; have %v", name, propertyNames(schema))
	}
	return &property
}

func assertRequiredFields(t *testing.T, schema *apiextensionsv1.JSONSchemaProps, names ...string) {
	t.Helper()
	required := make(map[string]bool, len(schema.Required))
	for _, name := range schema.Required {
		required[name] = true
	}
	for _, name := range names {
		if !required[name] {
			t.Errorf("field %q is not required; required fields are %v", name, schema.Required)
		}
	}
}

func propertyNames(schema *apiextensionsv1.JSONSchemaProps) []string {
	names := make([]string, 0, len(schema.Properties))
	for name := range schema.Properties {
		names = append(names, name)
	}
	return names
}
