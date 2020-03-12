.PHONY: all

all:
	go build \
		-ldflags "-X \"github.com/nwpc-oper/nwpc-message-client/commands/nwpc_message_consumer/app.Version=${VERSION}\" \
        -X \"github.com/nwpc-oper/nwpc-message-client/commands/nwpc_message_consumer/app.BuildTime=${BUILD_TIME}\" \
        -X \"github.com/nwpc-oper/nwpc-message-client/commands/nwpc_message_consumer/app.GitCommit=${GIT_COMMIT}\" " \
		-o ${BIN_PATH}/nwpc_message_consumer \
		main.go