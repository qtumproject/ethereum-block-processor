
# Qtum block hash to Ethereum block hash converter

Qtum (Bitcoin) block hashes are calculated using `SHA256`
Instead Ethereum uses `keccak256` as its hashing algorithm.
This program scans all the blocks from Qtum network (aprox 1.7 millions) and for each qtum block calculates the corresponding ethereum hash and stores the pair of hashes in a `Postgres` database

## Features

- Worker pool architecture
- Configurable number of workers (defaults to num of CPU cores)
- JSON RPC client over http
- http retry with backoff strategy and jitter schema
- Graceful termination for user interruption (^C)
- Loggin levels available
- Info and error data are saved to `output.log` and `error.log` files
- Multiple RPC providers endpoints are supported and distributed evenly among workers

## Command line options

```
usage: main [<flags>]

Flags:
      --help        Show context-sensitive help (also try --help-long and --help-man).
  -p, --providers=https://janus.qiswap.com ...  
                    qtum rpc providers
  -w, --workers=12  Number of workers. Defaults to system's number of CPUs.
  -d, --debug       debug mode
  -f, --from=0      block number to start scanning from (default: 'Latest'
  -t, --to=0        block number to stop scanning (default: 1)
      --version     Show application version.
```

## Usage example

```
go run main.go -p https://janus.qiswap.com/api/ -p http://34.66.201.0:23890 -w 6
```

```
 go run main.go -p https://janus.qiswap.com/api/ -p http://34.66.201.0:23890 -p http://127.0.0.1:23889 -p http://mainnet.qnode.meherett.com/77EKhIvlhGs1Jro4beyWH3KNxLZrSLgnyucHb -w 8
 ```

 ```
 go run main.go -p https://janus.qiswap.com/api/ -p http://34.66.201.0:23890 -p http://127.0.0.1:23889 -p http://mainnet.qnode.meherett.com/77EKhIvlhGs1Jro4beyWH3KNxLZrSLgnyucHb -w 8  -t 1500000
 ```

## To do

- Include options to use cloud based DB (i.e. AWS Postgres) or REDIS
- Improve tests coverage