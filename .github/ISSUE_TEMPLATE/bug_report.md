---
name: Bug report
about: Create a report to help us improve
title: '[BUG] '
labels: 'bug'
assignees: ''

---

**Describe the bug**
A clear and concise description of what the bug is.

**To Reproduce**
Steps to reproduce the behavior:
1. Create factory with '...'
2. Register processors '...'
3. Build schema '...'
4. See error

**Schema Example**
```yaml
# The schema that causes the issue
type: sequence
children:
  - ref: processor-name
```

**Code Example**
```go
// Minimal code example that reproduces the issue
package main

import (
    "context"
    "github.com/zoobzio/flume"
)

func main() {
    // Your code here
}
```

**Expected behavior**
A clear and concise description of what you expected to happen.

**Actual behavior**
What actually happened, including any error messages or stack traces.

**Environment:**
 - OS: [e.g. macOS, Linux, Windows]
 - Go version: [e.g. 1.21.0]
 - flume version: [e.g. v0.1.0]
 - pipz version: [e.g. v0.0.19]

**Additional context**
Add any other context about the problem here.
