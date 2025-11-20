"""
Standardized exception hierarchy for consistent error handling.
"""


class ServiceError(Exception):
    """Base exception for service-related errors."""
    def __init__(self, message: str, service: str = None, error_code: str = None):
        super().__init__(message)
        self.service = service
        self.error_code = error_code
        self.message = message


class RetryableError(ServiceError):
    """Error that can be retried (transient failures)."""
    def __init__(self, message: str, service: str = None, retry_after: int = None):
        super().__init__(message, service=service, error_code="RETRYABLE")
        self.retry_after = retry_after


class ConfigurationError(ServiceError):
    """Configuration-related error."""
    def __init__(self, message: str, config_key: str = None):
        super().__init__(message, error_code="CONFIG_ERROR")
        self.config_key = config_key


class ServiceUnavailableError(ServiceError):
    """Service is unavailable (down, unreachable, etc.)."""
    def __init__(self, message: str, service: str):
        super().__init__(message, service=service, error_code="SERVICE_UNAVAILABLE")


class DatabaseError(ServiceError):
    """Database-related error."""
    def __init__(self, message: str, operation: str = None):
        super().__init__(message, service="database", error_code="DB_ERROR")
        self.operation = operation


class StorageError(ServiceError):
    """Storage (MinIO, S3, etc.) related error."""
    def __init__(self, message: str, operation: str = None):
        super().__init__(message, service="storage", error_code="STORAGE_ERROR")
        self.operation = operation


class TimeoutError(RetryableError):
    """Operation timeout."""
    def __init__(self, message: str, service: str, timeout: int):
        super().__init__(message, service=service, retry_after=timeout)
        self.timeout = timeout

