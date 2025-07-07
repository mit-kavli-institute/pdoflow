# GitHub Actions CI/CD Workflows

This directory contains GitHub Actions workflows for continuous integration and testing of PDOFlow.

## Workflows

### 1. Tests (`test.yml`)
**Trigger**: On push to main branches and pull requests

The main testing workflow that:
- Runs tests across Python 3.9, 3.10, 3.11, and 3.12
- Executes parallel tests using pytest-xdist
- Performs linting (flake8) and type checking (mypy)
- Checks code formatting with Black
- Generates and combines coverage reports
- Builds documentation
- Requires all checks to pass

### 2. Coverage Report (`coverage.yml`)
**Trigger**: On pull requests and after test workflow completion

Provides detailed coverage reporting:
- Comments on PRs with coverage changes
- Compares coverage against base branch
- Generates coverage badges
- Enforces 80% minimum coverage threshold
- Updates PR status checks

### 3. Scheduled Tests (`scheduled-tests.yml`)
**Trigger**: Daily at 2 AM UTC or manual dispatch

Comprehensive testing including:
- Full test suite with extended Hypothesis examples
- Performance regression tests
- Security vulnerability scanning
- Automatic issue creation on failures

### 4. Dependency Check (`dependency-check.yml`)
**Trigger**: On dependency file changes, weekly schedule, or manual

Monitors project dependencies:
- Reviews dependency changes in PRs
- Audits for security vulnerabilities
- Checks for outdated packages
- Validates license compatibility

## Environment Variables

### Required for Tests
- PostgreSQL connection details are provided by the service container
- `HYPOTHESIS_PROFILE`: Set to 'ci' for more thorough testing

### Optional
- `GITHUB_TOKEN`: Automatically provided for PR comments and issue creation

## PostgreSQL Service

All test workflows use PostgreSQL 14 Alpine with:
- User: `postgres`
- Password: `testing`
- Database: `postgres`
- Port: `5432`

## Caching Strategy

- **pip packages**: Cached by Python version and pyproject.toml hash
- **tox environments**: Implicitly cached through pip
- **Coverage data**: Uploaded as artifacts and combined across Python versions

## Artifacts

Workflows generate the following artifacts:
- **coverage-report**: HTML and JSON coverage reports (7 days retention)
- **test-results**: Per-version test results (30 days retention)
- **benchmark-results**: Performance benchmarks (90 days retention)
- **security-reports**: Vulnerability scan results (30 days retention)
- **documentation**: Built Sphinx docs (7 days retention)

## Branch Protection

Recommended branch protection rules:
- Require PR reviews
- Require status checks:
  - `Test Python 3.9/3.10/3.11/3.12`
  - `Lint and Type Check`
  - `Coverage Status Check`
  - `All Tests Pass`
- Require branches to be up to date
- Include administrators

## Manual Workflow Dispatch

Several workflows support manual triggering:
- **test.yml**: Standard test run
- **scheduled-tests.yml**: With custom Hypothesis profile selection
- **dependency-check.yml**: On-demand dependency audit

## Maintenance

### Adding Python Versions
1. Update `matrix.python-version` in test workflows
2. Update `tox.ini` to include new Python version
3. Ensure any version-specific code is compatible

### Updating Dependencies
1. Modify `pyproject.toml`
2. Dependency check workflow will automatically run
3. Review security and license reports

### Coverage Threshold
- Current minimum: 80%
- To modify: Update `--fail-under` value in workflows and `tox.ini`

## Troubleshooting

### Test Failures
- Check the test logs in the Actions tab
- For Hypothesis failures, note the failing example for reproduction
- Database connection issues usually indicate service container problems

### Coverage Issues
- Ensure all test files are discovered
- Check that coverage is properly combined across parallel runs
- Verify `.coveragerc` or `pyproject.toml` coverage settings

### Performance
- Use workflow concurrency settings to limit parallel runs
- Consider using larger runners for extensive test suites
- Monitor cache hit rates in the Actions UI
