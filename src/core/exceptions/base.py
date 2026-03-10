from typing import Any


class AppException(Exception):
    status_code: int = 500
    error_code: str = "INTERNAL_ERROR"
    message: str = "An unexpected error occurred."

    def __init__(self, message: str | None = None, details: Any = None) -> None:
        self.message = message or self.__class__.message
        self.details = details
        super().__init__(self.message)


class ValidationException(AppException):
    status_code = 422
    error_code = "VALIDATION_ERROR"
    message = "Validation failed."


class AuthenticationException(AppException):
    status_code = 401
    error_code = "AUTHENTICATION_ERROR"
    message = "Authentication required."


class AuthorizationException(AppException):
    status_code = 403
    error_code = "AUTHORIZATION_ERROR"
    message = "Insufficient permissions."


class NotFoundException(AppException):
    status_code = 404
    error_code = "NOT_FOUND"
    message = "Resource not found."


class ConflictException(AppException):
    status_code = 409
    error_code = "CONFLICT"
    message = "State conflict."


class SLABreachException(AppException):
    status_code = 422
    error_code = "SLA_BREACH"
    message = "SLA has been breached."


class InvalidTransitionException(AppException):
    status_code = 422
    error_code = "INVALID_TRANSITION"
    message = "Invalid status transition."


class LLMException(AppException):
    """Base class for all LLM-related exceptions."""
    status_code = 502
    error_code = "LLM_ERROR"
    message = "LLM service encountered an error."


class LLMRateLimitException(LLMException):
    status_code = 429
    error_code = "LLM_RATE_LIMIT"
    message = "LLM rate limit exceeded. Please try again later."

    def __init__(self, message: str | None = None, details: Any = None, retry_after: int | None = None) -> None:
        super().__init__(message=message, details=details)
        self.retry_after = retry_after  # seconds until retry is safe


class LLMAuthenticationException(LLMException):
    status_code = 502
    error_code = "LLM_AUTHENTICATION_ERROR"
    message = "LLM authentication failed. Check your API key."


class LLMConnectionException(LLMException):
    status_code = 503
    error_code = "LLM_CONNECTION_ERROR"
    message = "Could not connect to LLM service. It may be unreachable."


class LLMAPIStatusException(LLMException):
    status_code = 502
    error_code = "LLM_API_STATUS_ERROR"
    message = "LLM API returned an unexpected error status."

    def __init__(self, message: str | None = None, details: Any = None, api_status_code: int | None = None) -> None:
        super().__init__(message=message, details=details)
        self.api_status_code = api_status_code

class HTTPClientException(AppException):
    """Base class for all HTTP client exceptions."""
    status_code = 502
    error_code = "HTTP_CLIENT_ERROR"
    message = "HTTP client encountered an error."

class HTTPServiceUnreachableException(HTTPClientException):
    status_code = 503
    error_code = "HTTP_SERVICE_UNREACHABLE"
    message = "Upstream service is unreachable."

class HTTPServiceNotFoundException(HTTPClientException):
    status_code = 404
    error_code = "HTTP_SERVICE_NOT_FOUND"
    message = "Requested resource not found on upstream service."

class HTTPServiceUnauthorizedException(HTTPClientException):
    status_code = 401
    error_code = "HTTP_SERVICE_UNAUTHORIZED"
    message = "Upstream service rejected the request as unauthorized."

class EmailException(AppException):
    """Base class for all email exceptions."""
    status_code = 502
    error_code = "EMAIL_ERROR"
    message = "Email service encountered an error."

class EmailNotConfiguredException(EmailException):
    status_code = 503
    error_code = "EMAIL_NOT_CONFIGURED"
    message = "SMTP is not configured."

class EmailSendFailedException(EmailException):
    status_code = 502
    error_code = "EMAIL_SEND_FAILED"
    message = "Failed to send email."

class ServiceUnavailableException(AppException):
    status_code = 503
    error_code = "SERVICE_UNAVAILABLE"
    message = "Service temporarily unavailable."

class ForbiddenException(AppException):
    status_code = 403
    error_code = "FORBIDDEN"
    message = "Access forbidden."