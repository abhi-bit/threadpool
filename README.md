**Threadpool**

Generic thread pooling in Golang.

**Usage:**

Initialise thread pool with pool of 5 goroutines waiting to execute your job

```go
thp := threadpool.New(5)
thp.Init()
 ```

Create a new job for dispatching some work to the thread pool
```go
job := thp.NewJob()
```

Callback function that you want thread pool to start work with some set of inputs

```go
cbFunc := func(inputs []interface{}) error {
		// do some work
		return nil
	}
```

Add work for thread pool and map it to `job` created earlier. `job` handle could be used to know
if all the work mapped with that `job` has finished.

```go
thp.Add(j, cbFunc, 200)
thp.Add(j, cbFunc, 100)

```

`job.Wait()` is supposed to block until all the work mapped to it has finished and return value will
 inform if those jobs have successfully finished or not.
```go
err := tp.Wait(j)
if err != nil {
	assert.NoError(t, err)
}
```

**Benchmark:**

```go
$ go test -run=BenchmarkThreadPool -bench=. -v
goos: darwin
goarch: amd64
pkg: source.golabs.io/growth/threadpool
BenchmarkThreadPool-12           5000000               403 ns/op
PASS
ok      source.golabs.io/growth/threadpool      2.408s
```