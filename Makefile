all:

.PHONY: clean
clean:
	find examples -name "*.pyc" | xargs rm
	find src -name "*.pyc" | xargs rm
	find . -depth 1 -name "*.pyc" | xargs rm

	find examples -name "*.pyo" | xargs rm
	find src -name "*.pyo" | xargs rm
	find . -depth 1 -name "*.pyo" | xargs rm

	find examples -name "__pycache__" | xargs rm -r
	find src -name "__pycache__" | xargs rm -r
	find . -depth 1 -name "__pycache__" | xargs rm -r

	find examples -name "*~" | xargs rm
	find src -name "*~" | xargs rm
	find . -depth 1 -name "*~" | xargs rm

	find src -name "*.so" | xargs rm
