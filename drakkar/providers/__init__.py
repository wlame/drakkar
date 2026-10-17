"""Shared machinery for the pluggable providers that a feature calls over a network or a file system.

``resilience`` runs one provider operation under a time limit, with bounded
retries and a circuit breaker. It knows nothing about what the provider does.

``connections`` resolves the named connections to shared services and opens
each one once, so every feature that names a connection shares its client.

``lookup`` finds the factory of a configured provider name: a feature's
built-in table plus the plugins installed in its entry-point group.

``thread`` runs one provider's blocking file calls on a daemon thread of its
own, so a hung mount never holds a thread of the shared pool.
"""
