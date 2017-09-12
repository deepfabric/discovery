# service discovery via etcd v3

## How to run unit test

- clone this repo

- install dep (refers to https://github.com/golang/dep)

```go get github.com/golang/dep```

- install dependencies

```dep ensure```

- start local etcd, which listens at 127.0.0.1:2379 by default

```etcd```

- run tests

```go test```

Documentation

    https://godoc.org/github.com/deepfabric/discovery
