"""Errors."""


class InterpretationError(RuntimeError):
    """Errors on interpretation of patterns."""
    pass


class TypeInferenceError(RuntimeError):
    """Errors on type-inference."""
    pass
