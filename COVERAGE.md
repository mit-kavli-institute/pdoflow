# Coverage Configuration for PDOFlow

This document describes the modern coverage setup for the PDOFlow project.

## Overview

The coverage configuration uses the modern `coverage.py` package (v7.0+) with configuration stored in `pyproject.toml` rather than legacy `.coveragerc` files.

## Running Coverage

### Basic Usage

```bash
# Run all tests with coverage
tox

# Run specific Python version with coverage
tox -e py312

# Generate coverage reports
tox -e clean,py312,combine,report
```

### Coverage Workflow

1. **Clean**: Remove any existing coverage data
   ```bash
   tox -e clean
   ```

2. **Run Tests**: Execute tests with coverage collection
   ```bash
   tox -e py39,py310,py311,py312
   ```

3. **Combine**: Merge coverage data from parallel test runs
   ```bash
   tox -e combine
   ```

4. **Report**: Generate coverage reports
   ```bash
   tox -e report
   ```

### CI/CD Integration

For CI/CD pipelines, use the coverage-check environment:
```bash
tox -e coverage-check
```

This will fail if coverage drops below 80%.

For GitHub Actions:
```bash
tox -e gh-actions
```

## Configuration Details

### Coverage Settings (pyproject.toml)

- **Branch Coverage**: Enabled to track both line and branch coverage
- **Source**: Configured to track only the `pdoflow` package
- **Parallel Mode**: Enabled for pytest-xdist compatibility
- **Context**: Tracks which test environment generated coverage

### Path Mapping

The configuration handles different installation paths:
- Development: `src/pdoflow`
- Tox environments: `.tox/*/lib/python*/site-packages/pdoflow`
- Windows: `.tox\\*\\Lib\\site-packages\\pdoflow`

### Exclusions

The following lines are excluded from coverage:
- `pragma: no cover` comments
- `__repr__` methods
- Type checking blocks
- Abstract methods
- Unreachable code

## Reports

### Available Reports

1. **Terminal Report**: Shows missing lines in the console
2. **HTML Report**: Interactive HTML report in `htmlcov/`
3. **JSON Report**: Machine-readable report in `coverage.json`
4. **XML Report**: For CI/CD integration (GitHub Actions environment)

### Viewing HTML Report

After running coverage:
```bash
open htmlcov/index.html  # macOS
xdg-open htmlcov/index.html  # Linux
start htmlcov/index.html  # Windows
```

## Tips

1. **Parallel Testing**: The configuration supports parallel test execution with pytest-xdist
2. **Incremental Coverage**: Use `--cov-append` to add to existing coverage data
3. **Specific Tests**: Run coverage on specific test files:
   ```bash
   tox -e py312 -- tests/test_priority.py
   ```

## Troubleshooting

If coverage seems incorrect:

1. Clean all coverage data: `tox -e clean`
2. Check that `PYTHONPATH` includes the source directory
3. Ensure tests are importing from the installed package, not the source directory
4. Verify that `.coverage.*` files are being created during test runs
