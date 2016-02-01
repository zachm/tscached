# tscached

tscached is a smart caching proxy, built with Redis, for time series data in the KairosDB format.

Inspired by [arussellsaw/postcache](https://github.com/arussellsaw/postcache) - tscached goes one
step further: A previously issued query will be reissued across only the elapsed time since its
last execution. In brief, this is a read-through, append-optimized, nominally consistent time
series cache.

Everything that follows is something of a fluid design document.

## Stored Data

We're storing two types of data in Redis, now referred to as **KQuery** and **MTS**.

### KQuery
- Key: a hash from the JSON dump of a given KairosDB query.
- One exception: its start/end time values are missing.
- Value: JSON dump of the query, including ABSOLUTE timestamps matching it.
- The timestamps in the value will be updated whenever we update its constituent MTS.
- We also include a list of Redis keys for matching MTS (in use for the HOT scenario).

### MTS (Metric Time Series)
- Briefly, each Metric Time Series is a KairosDB **result** dict.
- Given that one KairosDB query may return N time series results, this represents one of them.
- Key: a subset hash: includes elements `name, group_by, tags`.
- Value: the full contents of the result dict.

## Algorithm Outline

You have received a query intended for KairosDB. What to do?

What we do depends on whether (and what) corresponding data exists in the KQuery Store.

### Cache MISS (cold)
Unfortunately for the user, tscached has never seen this exact query before.

To proceed:
- Forward the entire query to Kairos.
- Split Kairos result into discrete MTS; hash them; write them into Redis.
- Write KQuery (including set of MTS hashes) into Redis.
- No trimming old values needed, since we only queried for what we wanted.
- Return result (may as well use the pre-built Kairos version) to user.

### Cache HIT (hot)
The user is in luck: this data is extremely fresh. This is the postcache scenario.

To be a *hot* hit, three properties must be true:
- A KQuery entry must exist for the data.
- KQuery data must have a start timestamp before or equivalent to that requested.
- KQuery data must have an end timestamp within 10s of NOW (configurable).

To proceed:
- Do **not** query Kairos at all - this is explicit flood control!
- Pull all listed MTS out of Redis.
- For each MTS, trim any data older than the START.
- Return the rebuilt result (ts fixing, etc.) without updating Redis.


### Cache HIT (warm)
This is the key tscached advancement. Data that is already extant in Redis, but more
than 10s old, is **appended to** instead of overwritten.

This removes a ridiculously high burden from your KairosDB cluster: in example, reading
from an entire production environment and plotting its load average:

- 10 second resolution
- 24 hour chart
- 2,000 hosts in the environment
- Returns **17.28 MILLION** data points.

Needless to say, one requires a *ridiculously oversized* KairosDB cluster to handle
this kind of load. So why bother? Results from such a query total only a few megabytes
of JSON. With tscached, after the first (painful!) MISS, we may now query for as few as
**2,000** data points on each subsequent query.

The end goal, therefore, is to turn a bursty load into a constant load... for all the obvious
reasons!

To proceed:
- Mutate Request: Forward original request to Kairos for older/younger intervals.
- Pull relevant MTS (listed in KQuery) from Redis.
- Merge MTS in an all-pairs strategy. This will rely on Index-Hash lookup for KairosDB data.
- As merges occur, overwrite discrete MTS into Redis.
- Any new MTS (that just started reporting) will be merged with empty sets and written to Redis
- Update KQuery with new Start, End timestamps and with any new MTS hashes.
- If MTS Start retrieved is too old (compared to original request) trim it down.
- Return the combined result.


## Future work

### Start updating
We may at first not want to support start updating. That would be a strange use
case: you'd have a dashboard of 1h that you stretched out to 6h. Query -6 to -1 then the
last bit of clock time...

### Staleness prevention
If a KQuery was last requested 6 hours ago (and only for one hour's range) we should
not bother reading from it now. In other words, despite handling the same *semantic data*
as before, tscached is effectively cold. TTL expiry may be useful for this case.

### Preemptive caching (Read-before)
tscached is intended to minimize read load on KairosDB; the fact that it will make
dashboards built with Grafana, et al. much faster to load is a happy coincidence.

This leads to a natural next step: if a finite number of dashboards are built in a Grafana
installation, but then queried very rarely (i.e., only under emergency scenarios), why not
provide *shadow load* on them all the time? This would, in effect, keep tscached current and
result in a very high (hot) hit rate.

It could be achieved with a daemon, or with cron jobs, or with various other approaches.
Stay tuned...
