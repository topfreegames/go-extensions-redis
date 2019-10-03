.PHONY: mocks

setup:
	@go get github.com/golang/mock/gomock
	@go install github.com/golang/mock/mockgen

mocks:
	@GO111MODULE=on mockgen -package mocks -destination mocks/mocks.go github.com/topfreegames/go-extensions-redis Client,Lock,Locker,Mux
	@GO111MODULE=on mockgen -package mocks -destination mocks/pipeliner.go github.com/go-redis/redis Pipeliner

test:
	@docker-compose up -d redis
	@go test -tags=unit -v
	@docker-compose down
