.PHONY: build pack
build:
	mkdir -p bin
	go build -trimpath -o bin/filemanager-share-worker ./cmd/filemanager-share-worker

pack:
	bash scripts/pack.sh dist
