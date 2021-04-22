# nwpc-message-client

A message client for NWPC operation systems.

## Installing

Download the latest release and build source code.

Use `Makefile` to build source code on Linux.

All tools will be installed in `bin` directory.

## Getting started

Run `nwpc_message_client production` to send message for production.

The following message sends GRIB2 production message for GRAPES GFS GMF system 
via a broker server using by `nwpc_message_client broker`.

```bash
nwpc_message_client production \
  --system grapes_gfs_gmf \
  --production-stream oper \
  --production-type grib2 \
  --production-name orig \
  --event storage \
  --status complete \
  --start-time "2021042200" \
  --forecast-time "000h" \
  --rabbitmq-server "rabbitmq server" \
  --broker-address "broker server"

```

The command above will send message below to NWPC Message Platform's RabbitMQ server.

```json
{
  "app": "nwpc-message-client",
  "type": "production",
  "time": "2021-04-22T04:42:28.271018513Z",
  "data": {
    "system": "grapes_gfs_gmf",
    "stream": "oper",
    "type": "grib2",
    "name": "orig",
    "start_time": "2021-04-22T00:00:00Z",
    "forecast_time": "000h",
    "event": "storage",
    "status": 1
  }
}
```

Please run `nwpc_message_clinet production --help` to get more usage.

## License

Copyright &copy; 2019-2021, Perilla Roc at nwpc-oper.

`nwpc-message-client` is licensed under [MIT License](LICENSE)