# Produce query traces from query logs

The `qtrace` tool makes it easy to find a specific query in query logs and
run them against `graph-node` to produce a trace of the query execution. It
assumes that logs are stored in Loki, and that `graph-node` has been set up
to produce query traces by setting the `GRAPH_GRAPHQL_TRACE_TOKEN`
environment variable.

## Usage

The `qtrace` tool requires a configuration file. The file
`config.sample.toml` explains which settings need to be made. The
configuration file can be specified using the `-c` flag or through the
`QTRACE_CONFIG` environment variable.

Running `qtrace` with just an IPFS hash will find a fairly random query for
that deployment and run it, producing this output:

```
> qtrace <IPFS hash>

Trace for qid "2c7f3a84b1109c1a-e5dd90239b353825"
 deployment QmZeCuoZeadgHkGwLwMeguyqUKz1WPWQYKcKyMCeQqGhsF

 root                                                1164ms
  ticks                                               1116ms [    797 entities]

query:         1116ms
other:           48ms
total:         1164ms
```

The output of `qtrace --help` explains what other options can be set. In
particular, it is possible to search for a query with a specific query ID,
and to only consider queries that took at least a certain time.

Besides printing a brief summary, `qtrace` can also store the trace and the
query output in a file for further inspection. The location of those files
can be either passed on the command line or set in the configuration file.

## Installation

1. Clone this git repository
2. Run `cargo install --path .` to install the `qtrace` binary
