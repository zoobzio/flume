# Security Policy

## Supported Versions

We release patches for security vulnerabilities. Which versions are eligible for receiving such patches depends on the CVSS v3.0 Rating:

| Version | Supported          | Status |
| ------- | ------------------ | ------ |
| latest  | ✅ | Active development |
| < latest | ❌ | Security fixes only for critical issues |

## Reporting a Vulnerability

We take the security of flume seriously. If you have discovered a security vulnerability in this project, please report it responsibly.

### How to Report

**Please DO NOT report security vulnerabilities through public GitHub issues.**

Instead, please report them via one of the following methods:

1. **GitHub Security Advisories** (Preferred)
   - Go to the [Security tab](https://github.com/zoobzio/flume/security) of this repository
   - Click "Report a vulnerability"
   - Fill out the form with details about the vulnerability

2. **Email**
   - Send details to the repository maintainer through GitHub profile contact information
   - Use PGP encryption if possible for sensitive details

### What to Include

Please include the following information (as much as you can provide) to help us better understand the nature and scope of the possible issue:

- **Type of issue** (e.g., race condition, schema injection, validation bypass, etc.)
- **Full paths of source file(s)** related to the manifestation of the issue
- **The location of the affected source code** (tag/branch/commit or direct URL)
- **Any special configuration required** to reproduce the issue
- **Step-by-step instructions** to reproduce the issue
- **Proof-of-concept or exploit code** (if possible)
- **Impact of the issue**, including how an attacker might exploit the issue
- **Your name and affiliation** (optional)

### What to Expect

- **Acknowledgment**: We will acknowledge receipt of your vulnerability report within 48 hours
- **Initial Assessment**: Within 7 days, we will provide an initial assessment of the report
- **Resolution Timeline**: We aim to resolve critical issues within 30 days
- **Disclosure**: We will coordinate with you on the disclosure timeline

### Preferred Languages

We prefer all communications to be in English.

## Security Best Practices

When using flume in your applications, we recommend:

1. **Keep Dependencies Updated**
   ```bash
   go get -u github.com/zoobzio/flume
   ```

2. **Schema Validation**
   - Always validate schemas before deployment using `ValidateSchemaStructure`
   - Use `ValidateSchema` at runtime with a configured factory
   - Don't load schemas from untrusted sources without validation

3. **Hot Reload Security**
   - Secure the source of schema updates (e.g., authenticated API endpoints)
   - Log all schema changes for audit purposes
   - Consider schema versioning for rollback capability

4. **Resource Management**
   - Close channels when no longer needed
   - Set appropriate timeouts for pipeline operations
   - Configure circuit breakers for external dependencies

5. **Concurrency Safety**
   - Ensure your data type's `Clone()` method creates deep copies
   - Don't share mutable state across processors without synchronization
   - Be aware that concurrent/race nodes run in parallel

## Security Features

flume includes several built-in security features:

- **Type Safety**: Go generics ensure compile-time type checking
- **Schema Validation**: Comprehensive validation catches errors before building
- **Reference Validation**: All processor/predicate/condition references verified
- **Thread Safety**: Atomic operations for hot-reload scenarios
- **Timeout Enforcement**: Built-in timeout connector prevents runaway operations
- **Circuit Breakers**: Built-in circuit breaker pattern for fault isolation

## Automated Security Scanning

This project uses:

- **CodeQL**: GitHub's semantic code analysis for security vulnerabilities
- **golangci-lint**: Static analysis including security linters (gosec)
- **Codecov**: Coverage tracking to ensure security-critical code is tested
- **Race Detection**: Tests run with `-race` flag to detect data races

## Vulnerability Disclosure Policy

- Security vulnerabilities will be disclosed via GitHub Security Advisories
- We follow a 90-day disclosure timeline for non-critical issues
- Critical vulnerabilities may be disclosed sooner after patches are available
- We will credit reporters who follow responsible disclosure practices

## Credits

We thank the following individuals for responsibly disclosing security issues:

_This list is currently empty. Be the first to help improve our security!_

---

**Last Updated**: 2025-12-12
