"""Custom exceptions for the pipeline. All pipeline-specific errors subclass PipelineError."""


class PipelineError(Exception):
    """Base class for all pipeline errors."""


class ConfigError(PipelineError):
    """Raised for missing/invalid environment variables or config files."""


class SourceRequestError(PipelineError):
    """Raised when an HTTP request fails after retries, or returns a non-2xx status."""


class EmptyResponseError(PipelineError):
    """Raised when a 2xx response has an empty body or zero rows when rows were expected."""


class ManifestMismatchError(PipelineError):
    """Raised when a file's checksum or row count does not match its manifest entry."""


class LandingFileMissingError(PipelineError):
    """Raised when a required landing file (e.g. a manually downloaded export) is absent."""