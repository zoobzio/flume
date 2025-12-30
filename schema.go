package flume

// Schema defines a pipeline structure that can be built dynamically.
type Schema struct {
	// Version tracks the schema version for change management
	Version string `json:"version,omitempty" yaml:"version,omitempty"`

	Node `yaml:",inline"`
}

// Node represents a single element in the pipeline schema.
// It can be either a processor reference or a connector definition.
type Node struct {
	Then              *Node           `json:"then,omitempty" yaml:"then,omitempty"`
	Default           *Node           `json:"default,omitempty" yaml:"default,omitempty"`
	Routes            map[string]Node `json:"routes,omitempty" yaml:"routes,omitempty"`
	Else              *Node           `json:"else,omitempty" yaml:"else,omitempty"`
	Child             *Node           `json:"child,omitempty" yaml:"child,omitempty"`
	Predicate         string          `json:"predicate,omitempty" yaml:"predicate,omitempty"`
	Stream            string          `json:"stream,omitempty" yaml:"stream,omitempty"`
	ErrorHandler      string          `json:"error_handler,omitempty" yaml:"error_handler,omitempty"`
	Condition         string          `json:"condition,omitempty" yaml:"condition,omitempty"`
	Name              string          `json:"name,omitempty" yaml:"name,omitempty"`
	Type              string          `json:"type,omitempty" yaml:"type,omitempty"`
	Reducer           string          `json:"reducer,omitempty" yaml:"reducer,omitempty"`
	Backoff           string          `json:"backoff,omitempty" yaml:"backoff,omitempty"`
	Duration          string          `json:"duration,omitempty" yaml:"duration,omitempty"`
	StreamTimeout     string          `json:"stream_timeout,omitempty" yaml:"stream_timeout,omitempty"`
	RecoveryTimeout   string          `json:"recovery_timeout,omitempty" yaml:"recovery_timeout,omitempty"`
	Ref               string          `json:"ref,omitempty" yaml:"ref,omitempty"`
	Children          []Node          `json:"children,omitempty" yaml:"children,omitempty"`
	BurstSize         int             `json:"burst_size,omitempty" yaml:"burst_size,omitempty"`
	RequestsPerSecond float64         `json:"requests_per_second,omitempty" yaml:"requests_per_second,omitempty"`
	FailureThreshold  int             `json:"failure_threshold,omitempty" yaml:"failure_threshold,omitempty"`
	Attempts          int             `json:"attempts,omitempty" yaml:"attempts,omitempty"`
	Workers           int             `json:"workers,omitempty" yaml:"workers,omitempty"`
}
