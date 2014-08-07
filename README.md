# Apache Kafka 0.8.x client written on Erlang

## Summary

This is a very early implementation and provides only
some of the basic features of the Kafka protocol:

- metadata requests;
- synchronous produce requests;
- asynchronous produce requests.

## License

BSD two-clause license.

## Build dependencies

* GNU Make;
* Erlang OTP;
* erlang-dev, erlang-tools (only when they are not provided with the Erlang OTP, e.g. in Debian);
* erlang-edoc (optional, needed to generate HTML docs);
* erlang-dialyzer (optional, needed to run the Dialyzer).

## Runtime dependencies

* Erlang OTP.

## Build

```make compile```

## Generate an HTML documentation for the code

```make html```

## Dialyze the code

```make dialyze```

## TODO:

Request types:

* fetch request;
* offset request;
* consumer metadata request;
* offset commit request;
* offset fetch request.

Other features:

* message buffering;
* make use of round-robin filling the topic partitions during producing.

Miscellaneous tasks:

* add some code examples.
