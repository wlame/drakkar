"""The fleet layer: how workers find each other.

A worker publishes a small card about itself to a registry provider and
reads the cards of its peers. Nothing in the message pipeline depends on
it: sources, executor, sinks and offset commits run the same with the
registry switched off.
"""
