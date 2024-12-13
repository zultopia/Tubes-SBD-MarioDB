.PHONY: opt-test

opt-test:
	@python3 -m QueryOptimizer.unittest

opt:
	@python3 -m QueryOptimizer.main