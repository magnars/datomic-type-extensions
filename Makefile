test:
	./bin/kaocha

deploy:
	lein deploy clojars

.PHONY: test
