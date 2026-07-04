.PHONY: build test tidy fmt terraform-fmt terraform-validate clean

LAMBDA_DIR := build/lambda
LAMBDA_ZIP := $(LAMBDA_DIR)/consumer.zip

build:
	mkdir -p $(LAMBDA_DIR)
	CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -tags lambda.norpc -trimpath -ldflags="-s -w" -o $(LAMBDA_DIR)/bootstrap ./cmd/consumer
	cd $(LAMBDA_DIR) && zip -q -FS consumer.zip bootstrap
	go build -trimpath -o build/experiment ./cmd/experiment

test:
	go test ./...

tidy:
	go mod tidy

fmt:
	gofmt -w cmd internal

terraform-fmt:
	terraform -chdir=terraform fmt -recursive

terraform-validate: build
	terraform -chdir=terraform init -backend=false
	terraform -chdir=terraform validate

clean:
	rm -rf build results
