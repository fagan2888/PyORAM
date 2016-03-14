all:

.PHONY: clean
clean:
	find examples -name "*.pyc" | xargs rm
	find pyoram -name "*.pyc" | xargs rm
	find . -depth 1 -name "*.pyc" | xargs rm

	find examples -name "*.pyo" | xargs rm
	find pyoram -name "*.pyo" | xargs rm
	find . -depth 1 -name "*.pyo" | xargs rm

	find examples -name "__pycache__" | xargs rm -r
	find pyoram -name "__pycache__" | xargs rm -r
	find . -depth 1 -name "__pycache__" | xargs rm -r

	find examples -name "*~" | xargs rm
	find pyoram -name "*~" | xargs rm
	find . -depth 1 -name "*~" | xargs rm

	find pyoram -name "*.so" | xargs rm
