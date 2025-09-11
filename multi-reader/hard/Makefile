
.PHONY: t
t:
	@echo "🚀 run tests"
	@./compile.sh
	@./run.sh || true
	@rm __tests

.PHONY: build
build:
	@echo "🛠️ build"
	@./compile.sh || true
	@rm __tests
