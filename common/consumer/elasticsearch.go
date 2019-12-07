package consumer

import (
	"context"
	"github.com/nwpc-oper/nwpc-message-client/common"
	"github.com/olivere/elastic/v7"
	log "github.com/sirupsen/logrus"
)

type ElasticSearchTarget struct {
	Server string
}

type messageWithIndex struct {
	Index   string
	Message common.EventMessage
}

func pushMessages(client *elastic.Client, messages []messageWithIndex, ctx context.Context) {
	bulkRequest := client.Bulk()
	for _, indexMessage := range messages {
		request := elastic.NewBulkIndexRequest().
			Index(indexMessage.Index).
			Doc(indexMessage.Message)
		bulkRequest.Add(request)
	}
	_, err := bulkRequest.Do(ctx)
	if err != nil {
		log.WithFields(log.Fields{
			"component": "elastic",
			"event":     "index",
		}).Errorf("%v", err)
		return
	}
}
