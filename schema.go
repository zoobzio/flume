package flume

// Schema defines a pipeline structure that can be built dynamically.
type Schema struct {
	// Version tracks the schema version for change management
	Version string `json:"version,omitempty" yaml:"version,omitempty"`

	Node `yaml:",inline"`
}

// Node represents a single element in the pipeline schema.
// It can be either a processor reference or a connector definition.
type Node struct { //nolint:govet
	// Ref is a reference to a registered processor (mutually exclusive with Type)
	Ref string `json:"ref,omitempty" yaml:"ref,omitempty"`

	// Type defines the connector type (sequence, concurrent, switch, etc.)
	Type string `json:"type,omitempty" yaml:"type,omitempty"`

	// Name for the created connector (optional, defaults to type)
	Name string `json:"name,omitempty" yaml:"name,omitempty"`

	// Children for composite connectors (sequence, concurrent, etc.)
	Children []Node `json:"children,omitempty" yaml:"children,omitempty"`

	// Child for single-child connectors (timeout, retry, etc.)
	Child *Node `json:"child,omitempty" yaml:"child,omitempty"`

	// Filter/Contest fields
	Predicate string `json:"predicate,omitempty" yaml:"predicate,omitempty"`
	Then      *Node  `json:"then,omitempty" yaml:"then,omitempty"`
	Else      *Node  `json:"else,omitempty" yaml:"else,omitempty"`

	// Switch fields
	Condition string          `json:"condition,omitempty" yaml:"condition,omitempty"`
	Routes    map[string]Node `json:"routes,omitempty" yaml:"routes,omitempty"`
	Default   *Node           `json:"default,omitempty" yaml:"default,omitempty"`

	// Retry configuration
	Attempts int    `json:"attempts,omitempty" yaml:"attempts,omitempty"`
	Backoff  string `json:"backoff,omitempty" yaml:"backoff,omitempty"`

	// Timeout configuration
	Duration string `json:"duration,omitempty" yaml:"duration,omitempty"`

	// Circuit breaker configuration
	FailureThreshold int    `json:"failure_threshold,omitempty" yaml:"failure_threshold,omitempty"`
	RecoveryTimeout  string `json:"recovery_timeout,omitempty" yaml:"recovery_timeout,omitempty"`

	// Rate limiter configuration
	RequestsPerSecond float64 `json:"requests_per_second,omitempty" yaml:"requests_per_second,omitempty"`
	BurstSize         int     `json:"burst_size,omitempty" yaml:"burst_size,omitempty"`

	// Stream configuration
	Stream        string `json:"stream,omitempty" yaml:"stream,omitempty"`
	StreamTimeout string `json:"stream_timeout,omitempty" yaml:"stream_timeout,omitempty"`

	// Concurrent configuration
	Reducer string `json:"reducer,omitempty" yaml:"reducer,omitempty"`

	// Handle configuration
	ErrorHandler string `json:"error_handler,omitempty" yaml:"error_handler,omitempty"`

	// Worker pool configuration
	Workers int `json:"workers,omitempty" yaml:"workers,omitempty"`
}
