# tscached

![tscached logo]
(https://github.com/zachm/tscached/raw/master/logo/logo.png)

tscached is a smart caching proxy, built with Redis, for time series data in the [KairosDB](https://kairosdb.github.io/) format.

Inspired by [arussellsaw/postcache](https://github.com/arussellsaw/postcache) - tscached goes one
step further: *A previously issued query will be reissued across only the elapsed time since its
last execution.* This provides a substantial improvement in serving high-volume load, especially temporally long queries that return thousands of time series. Using only simple techniques - consistent hashing, read-through caching, and backend load chunking - we provide user-perceived read latency improvements of up to 100x.

There are several different frontends to use with a Kairos-compliant API like this one, but the most full-featured remains (as always) [Grafana](http://grafana.org/) with [this plugin](https://github.com/grafana/kairosdb-datasource) installed.
