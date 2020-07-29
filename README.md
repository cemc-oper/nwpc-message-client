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
    --status completed \
    --start-time 2019120600 \
    --forecast-time 3h \
    --rabbitmq-server=amqp://guest:guest@localhost:5672 \
    --with-broker \
    --broker-address=localhost:33384
```

Please run `nwpc_message_clinet production --help` to get more usage.

## License

Copyright &copy; 2019-2020, Perilla Roc at nwpc-oper.

`nwpc-message-client` is licensed under [MIT License](LICENSE)