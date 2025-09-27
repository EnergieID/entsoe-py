import pytest
from entsoe.exceptions import (
    PaginationError,
    NoMatchingDataError,
    InvalidPSRTypeError,
    InvalidBusinessParameterError,
    InvalidParameterError
)


class TestExceptions:
    
    def test_pagination_error(self):
        with pytest.raises(PaginationError):
            raise PaginationError("Test pagination error")
    
    def test_no_matching_data_error(self):
        with pytest.raises(NoMatchingDataError):
            raise NoMatchingDataError("No data found")
    
    def test_invalid_psr_type_error(self):
        with pytest.raises(InvalidPSRTypeError):
            raise InvalidPSRTypeError("Invalid PSR type")
    
    def test_invalid_business_parameter_error(self):
        with pytest.raises(InvalidBusinessParameterError):
            raise InvalidBusinessParameterError("Invalid business parameter")
    
    def test_invalid_parameter_error(self):
        with pytest.raises(InvalidParameterError):
            raise InvalidParameterError("Invalid parameter")
    
    def test_exceptions_are_exception_subclasses(self):
        assert issubclass(PaginationError, Exception)
        assert issubclass(NoMatchingDataError, Exception)
        assert issubclass(InvalidPSRTypeError, Exception)
        assert issubclass(InvalidBusinessParameterError, Exception)
        assert issubclass(InvalidParameterError, Exception)
    
    def test_exception_with_message(self):
        message = "Custom error message"
        error = PaginationError(message)
        assert str(error) == message