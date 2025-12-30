package flume

import (
	"encoding/json"
	"sort"
)

// FactorySpec provides a complete description of a factory's capabilities
// for introspection, documentation, and schema generation.
type FactorySpec struct {
	Processors []ProcessorSpec `json:"processors"`
	Predicates []PredicateSpec `json:"predicates"`
	Conditions []ConditionSpec `json:"conditions"`
	Channels   []ChannelSpec   `json:"channels"`
	Connectors []ConnectorSpec `json:"connectors"`
}

// ProcessorSpec describes a registered processor.
type ProcessorSpec struct {
	Name        string   `json:"name"`
	Description string   `json:"description,omitempty"`
	Tags        []string `json:"tags,omitempty"`
}

// PredicateSpec describes a registered predicate.
type PredicateSpec struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// ConditionSpec describes a registered condition.
type ConditionSpec struct {
	Name        string   `json:"name"`
	Description string   `json:"description,omitempty"`
	Values      []string `json:"values,omitempty"`
}

// ChannelSpec describes a registered channel.
type ChannelSpec struct {
	Name string `json:"name"`
}

// ConnectorSpec describes a connector type and its schema requirements.
type ConnectorSpec struct {
	Type           string      `json:"type"`
	Description    string      `json:"description"`
	RequiredFields []FieldSpec `json:"required_fields,omitempty"`
	OptionalFields []FieldSpec `json:"optional_fields,omitempty"`
}

// FieldSpec describes a field within a connector.
type FieldSpec struct {
	Name        string `json:"name"`
	Type        string `json:"type"` // "string", "int", "duration", "node", "node_array", "node_map"
	Description string `json:"description"`
}

// NodeRefSpec documents the processor reference syntax.
var nodeRefSpec = ConnectorSpec{
	Type:        "ref",
	Description: "References a registered processor by name",
	RequiredFields: []FieldSpec{
		{Name: "ref", Type: "string", Description: "Name of a registered processor"},
	},
}

// connectorSpecs defines all available connector types and their schemas.
// This is the authoritative definition of the schema grammar.
var connectorSpecs = []ConnectorSpec{
	nodeRefSpec,
	{
		Type:        "sequence",
		Description: "Executes children in order, passing data through each",
		RequiredFields: []FieldSpec{
			{Name: "children", Type: "node_array", Description: "Processors to execute in sequence"},
		},
		OptionalFields: []FieldSpec{
			{Name: "name", Type: "string", Description: "Optional name for the connector"},
		},
	},
	{
		Type:        "concurrent",
		Description: "Executes children in parallel, waits for all to complete",
		RequiredFields: []FieldSpec{
			{Name: "children", Type: "node_array", Description: "Processors to execute concurrently"},
		},
		OptionalFields: []FieldSpec{
			{Name: "name", Type: "string", Description: "Optional name for the connector"},
		},
	},
	{
		Type:        "race",
		Description: "Executes children in parallel, returns first successful result",
		RequiredFields: []FieldSpec{
			{Name: "children", Type: "node_array", Description: "Processors to race"},
		},
		OptionalFields: []FieldSpec{
			{Name: "name", Type: "string", Description: "Optional name for the connector"},
		},
	},
	{
		Type:        "fallback",
		Description: "Tries primary, uses fallback on failure",
		RequiredFields: []FieldSpec{
			{Name: "children", Type: "node_array", Description: "Exactly 2 children: [primary, fallback]"},
		},
		OptionalFields: []FieldSpec{
			{Name: "name", Type: "string", Description: "Optional name for the connector"},
		},
	},
	{
		Type:        "retry",
		Description: "Retries child on failure with optional backoff",
		RequiredFields: []FieldSpec{
			{Name: "child", Type: "node", Description: "Processor to retry"},
		},
		OptionalFields: []FieldSpec{
			{Name: "name", Type: "string", Description: "Optional name for the connector"},
			{Name: "attempts", Type: "int", Description: "Maximum retry attempts (default: 3)"},
			{Name: "backoff", Type: "duration", Description: "Backoff duration between retries (e.g., '100ms')"},
		},
	},
	{
		Type:        "timeout",
		Description: "Enforces a time limit on child execution",
		RequiredFields: []FieldSpec{
			{Name: "child", Type: "node", Description: "Processor to timeout"},
		},
		OptionalFields: []FieldSpec{
			{Name: "name", Type: "string", Description: "Optional name for the connector"},
			{Name: "duration", Type: "duration", Description: "Timeout duration (default: 30s)"},
		},
	},
	{
		Type:        "filter",
		Description: "Conditionally executes based on predicate result",
		RequiredFields: []FieldSpec{
			{Name: "predicate", Type: "string", Description: "Name of registered predicate"},
			{Name: "then", Type: "node", Description: "Processor when predicate is true"},
		},
		OptionalFields: []FieldSpec{
			{Name: "name", Type: "string", Description: "Optional name for the connector"},
			{Name: "else", Type: "node", Description: "Processor when predicate is false"},
		},
	},
	{
		Type:        "switch",
		Description: "Routes to different processors based on condition result",
		RequiredFields: []FieldSpec{
			{Name: "condition", Type: "string", Description: "Name of registered condition"},
			{Name: "routes", Type: "node_map", Description: "Map of condition values to processors"},
		},
		OptionalFields: []FieldSpec{
			{Name: "name", Type: "string", Description: "Optional name for the connector"},
			{Name: "default", Type: "node", Description: "Processor when no route matches"},
		},
	},
	{
		Type:        "circuit-breaker",
		Description: "Prevents cascading failures by stopping requests to failing services",
		RequiredFields: []FieldSpec{
			{Name: "child", Type: "node", Description: "Processor to protect"},
		},
		OptionalFields: []FieldSpec{
			{Name: "name", Type: "string", Description: "Optional name for the connector"},
			{Name: "failure_threshold", Type: "int", Description: "Failures before opening circuit (default: 5)"},
			{Name: "recovery_timeout", Type: "duration", Description: "Time before attempting recovery (default: 60s)"},
		},
	},
	{
		Type:        "rate-limit",
		Description: "Throttles request rate to protect downstream services",
		RequiredFields: []FieldSpec{
			{Name: "child", Type: "node", Description: "Processor to rate limit"},
		},
		OptionalFields: []FieldSpec{
			{Name: "name", Type: "string", Description: "Optional name for the connector"},
			{Name: "requests_per_second", Type: "float", Description: "Maximum requests per second (default: 10)"},
			{Name: "burst_size", Type: "int", Description: "Maximum burst size (default: 1)"},
		},
	},
	{
		Type:        "stream",
		Description: "Sends data to a registered channel, optionally continues processing",
		RequiredFields: []FieldSpec{
			{Name: "stream", Type: "string", Description: "Name of registered channel"},
		},
		OptionalFields: []FieldSpec{
			{Name: "name", Type: "string", Description: "Optional name for the connector"},
			{Name: "stream_timeout", Type: "duration", Description: "Timeout for channel write (e.g., '5s'); blocks indefinitely if not set"},
			{Name: "child", Type: "node", Description: "Optional processor to execute after sending"},
			{Name: "children", Type: "node_array", Description: "Optional processors to execute after sending"},
		},
	},
}

// Spec returns a complete specification of the factory's capabilities.
// This includes all registered components and the connector grammar.
// Output is sorted alphabetically for deterministic results.
func (f *Factory[T]) Spec() FactorySpec {
	f.mu.RLock()
	defer f.mu.RUnlock()

	spec := FactorySpec{
		Processors: make([]ProcessorSpec, 0, len(f.processors)),
		Predicates: make([]PredicateSpec, 0, len(f.predicates)),
		Conditions: make([]ConditionSpec, 0, len(f.conditions)),
		Channels:   make([]ChannelSpec, 0, len(f.channels)),
		Connectors: connectorSpecs,
	}

	for name, pm := range f.processors {
		// Get description from the processor's identity if available
		description := ""
		if pm.processor != nil {
			description = pm.processor.Identity().Description()
		}
		spec.Processors = append(spec.Processors, ProcessorSpec{
			Name:        name,
			Description: description,
			Tags:        pm.tags,
		})
	}
	sortProcessors(spec.Processors)

	for name, pm := range f.predicates {
		spec.Predicates = append(spec.Predicates, PredicateSpec{
			Name:        name,
			Description: pm.identity.Description(),
		})
	}
	sortPredicates(spec.Predicates)

	for name, cm := range f.conditions {
		spec.Conditions = append(spec.Conditions, ConditionSpec{
			Name:        name,
			Description: cm.identity.Description(),
			Values:      cm.values,
		})
	}
	sortConditions(spec.Conditions)

	for name := range f.channels {
		spec.Channels = append(spec.Channels, ChannelSpec{
			Name: name,
		})
	}
	sortChannels(spec.Channels)

	return spec
}

// SpecJSON returns the factory specification as a JSON string.
// This is suitable for LLM prompt injection or external tooling.
func (f *Factory[T]) SpecJSON() (string, error) {
	spec := f.Spec()
	data, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func sortProcessors(s []ProcessorSpec) {
	sort.Slice(s, func(i, j int) bool { return s[i].Name < s[j].Name })
}

func sortPredicates(s []PredicateSpec) {
	sort.Slice(s, func(i, j int) bool { return s[i].Name < s[j].Name })
}

func sortConditions(s []ConditionSpec) {
	sort.Slice(s, func(i, j int) bool { return s[i].Name < s[j].Name })
}

func sortChannels(s []ChannelSpec) {
	sort.Slice(s, func(i, j int) bool { return s[i].Name < s[j].Name })
}
