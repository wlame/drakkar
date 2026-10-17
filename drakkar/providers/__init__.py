"""Shared machinery for the pluggable providers that a feature calls over a network or a file system.

``resilience`` runs one provider operation under a time limit, with bounded
retries and a circuit breaker. It knows nothing about what the provider does.
"""
