BIN=givi
.PHONY: $(BIN) test

export GOPATH:=$(shell pwd)

$(BIN):
	go install $(BIN)

run: $(BIN)
	bin/$(BIN)

test:
	go test $(BIN)

depends:
	go get github.com/vmihailenco/pg
