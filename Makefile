setup:
	@go get github.com/golang/mock/gomock
	@go install github.com/golang/mock/mockgen

mocks:
	@GO111MODULE=on mockgen -package mocks -destination mocks/mocks.go github.com/topfreegames/go-extensions-redis Client,Lock,Locker,Mux
