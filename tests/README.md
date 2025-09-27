# ENTSOE-PY Test Suite

This directory contains comprehensive tests for the entsoe-py project, covering all major components and functionality.

## Test Structure

### Core Tests
- **test_client.py**: Tests for EntsoeRawClient and EntsoePandasClient classes
- **test_client_improved.py**: Enhanced client tests with advanced scenarios
- **test_mappings.py**: Tests for Area enum and lookup functions
- **test_exceptions.py**: Tests for custom exception classes
- **test_parsers.py**: Tests for XML parsing functions with realistic data
- **test_series_parsers.py**: Tests for time series parsing functions
- **test_utils.py**: Tests for utility functions
- **test_decorators.py**: Tests for decorator functions (retry, year_limited, etc.)

### Integration Tests
- **test_integration.py**: Tests for component interactions and workflows
- **test_integration_improved.py**: Enhanced integration tests with multi-country workflows
- **test_files.py**: Tests for file client functionality with error handling
- **test_misc.py**: Tests for miscellaneous utility functions
- **test_working_suite.py**: Tests for working suite functionality

## Test Coverage

The test suite covers:
- ✅ Client initialization and configuration
- ✅ API key handling (environment variables and direct)
- ✅ HTTP request handling and error scenarios
- ✅ Data parsing and transformation with realistic XML data
- ✅ Exception handling and error propagation
- ✅ Timezone handling and datetime conversion
- ✅ Decorator functionality (retry, pagination, year limiting)
- ✅ Area mappings and lookups
- ✅ File operations and authentication
- ✅ Multi-country data workflows
- ✅ Large dataset handling and performance
- ✅ Parameter validation and edge cases
- ✅ Error recovery and resilience testing

## Running Tests

```bash
# Run all tests
python -m pytest tests/

# Run with verbose output
python -m pytest tests/ -v

# Run specific test file
python -m pytest tests/test_client.py

# Run with coverage
python -m pytest tests/ --cov=entsoe

# Run tests without warnings
python -m pytest tests/ -q
```

## Test Results

**✅ All 103 tests pass successfully** with zero warnings after recent improvements.

## Recent Improvements

### Parser Tests Enhanced
- **Realistic XML Data**: Tests now use actual ENTSO-E API XML format
- **Data Validation**: Tests verify parsed values match expected results
- **Multiple Scenarios**: Coverage for different resolutions, PSR types, and data structures
- **Proper XML Structure**: Fixed XML parsing to use XML parser instead of HTML parser

### Client Tests Expanded
- **Missing Method Coverage**: Added tests for `query_load_forecast`, `query_installed_generation_capacity`
- **Parameter Validation**: Parametrized tests for country code validation
- **Edge Cases**: Large date range handling and timezone edge cases
- **Error Scenarios**: Comprehensive error handling validation

### Integration Tests Improved
- **Multi-Country Workflows**: Tests combining data from multiple countries
- **Data Consistency**: Validation of data alignment across different methods
- **Performance Testing**: Large dataset handling (1 year of hourly data)
- **Error Recovery**: Tests for error handling and recovery workflows

### File Client Tests Enhanced
- **Authentication Errors**: Proper error handling for authentication failures
- **File Operations**: Validation of file download and listing operations
- **Parametrized Testing**: Multiple folder scenarios with different file counts

## Key Features Tested

1. **Client Functionality**: Both raw and pandas clients with comprehensive error handling
2. **Data Parsing**: XML parsing with realistic ENTSO-E API responses
3. **Authentication**: Robust authentication testing for file operations
4. **Decorators**: Retry logic, pagination, and time-based limiting
5. **Mappings**: Country/area code lookups and validation
6. **Integration**: Cross-component functionality and complex workflows
7. **Performance**: Large dataset handling and concurrent request simulation
8. **Resilience**: Error recovery and edge case handling

## Warning-Free Execution

The test suite now runs with **zero warnings** after:
- Switching from HTML parser to XML parser for BeautifulSoup
- Fixing deprecated pandas frequency syntax (`'M'` → `'ME'`)
- Proper warning filters in pytest configuration

The test suite ensures robust functionality, proper error handling, and real-world usage scenarios across all components of the entsoe-py library.