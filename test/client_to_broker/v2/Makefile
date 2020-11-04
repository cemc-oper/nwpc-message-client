.PHONY: all

all:
	go build \
		-tags=forceposix \
		-ldflags "-X \"github.com/nwpc-oper/nwpc-message-client/commands/nwpc_message_client/app.Version=${VERSION}\" \
        -X \"github.com/nwpc-oper/nwpc-message-client/commands/nwpc_message_client/app.BuildTime=${BUILD_TIME}\" \
        -X \"github.com/nwpc-oper/nwpc-message-client/commands/nwpc_message_client/app.GitCommit=${GIT_COMMIT}\" " \
		-o ${BIN_PATH}/test_client_to_broker \
		main.go