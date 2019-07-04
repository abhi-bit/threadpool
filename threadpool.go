package threadpool

import (
	"sync"
	"sync/atomic"
)

type JobInfo struct {
	id     uint64
	wg     *sync.WaitGroup
	err    error
	errMtx *sync.RWMutex

	// stats
	callCount uint64
	errCount  uint64
}

type CallbackFunc func([]interface{}) error
type CbFuncInfo struct {
	cbFunc   CallbackFunc
	fnParams []interface{}
	job      *JobInfo
}

type ThreadPool struct {
	jobs     map[uint64]*JobInfo
	jobsMtx  *sync.RWMutex
	jobCount uint64

	dispatchCh chan *CbFuncInfo
	poolSize   int
}

func New(poolSize int) *ThreadPool {
	return &ThreadPool{
		jobs:       make(map[uint64]*JobInfo),
		jobsMtx:    &sync.RWMutex{},
		dispatchCh: make(chan *CbFuncInfo, poolSize),
		poolSize:   poolSize,
	}
}

func (t *ThreadPool) Init() {
	for i := 0; i < t.poolSize; i++ {
		go t.execute()
	}
}

func (t *ThreadPool) execute() {
	defer func() {
		if err := recover(); err != nil {
			t.execute()
		}
	}()

	for {
		select {
		case info := <-t.dispatchCh:
			func() {
				// needed to call callback function in new scope
				// as the cbFunc can panic as well
				defer info.job.wg.Done()

				err := info.cbFunc(info.fnParams)
				if err != nil {
					info.job.errMtx.Lock()
					defer info.job.errMtx.Unlock()
					info.job.err = err

					atomic.AddUint64(&info.job.errCount, 1)
				}
			}()
		}
	}
}

func (t *ThreadPool) NewJob() *JobInfo {
	atomic.AddUint64(&t.jobCount, 1)

	job := &JobInfo{
		id:     atomic.LoadUint64(&t.jobCount),
		wg:     &sync.WaitGroup{},
		err:    nil,
		errMtx: &sync.RWMutex{},
	}

	t.jobsMtx.Lock()
	defer t.jobsMtx.Unlock()
	t.jobs[job.id] = job

	return job
}

func (t *ThreadPool) Add(j *JobInfo, fn CallbackFunc, params ...interface{}) {
	j.wg.Add(1)
	atomic.AddUint64(&j.callCount, 1)

	t.dispatchCh <- &CbFuncInfo{
		cbFunc:   fn,
		fnParams: params,
		job:      j,
	}
}

func (t *ThreadPool) Wait(j *JobInfo) error {
	j.wg.Wait()

	t.jobsMtx.Lock()
	defer t.jobsMtx.Unlock()
	delete(t.jobs, j.id)

	return j.err
}

func (t *ThreadPool) Stats() map[string]uint64 {
	stats := make(map[string]uint64)

	stats["thread-pool-size"] = uint64(t.poolSize)
	stats["total-job-executed"] = atomic.LoadUint64(&t.jobCount)

	t.jobsMtx.RLock()
	defer t.jobsMtx.RUnlock()
	stats["active-job-count"] = uint64(len(t.jobs))
	stats["dispatch-chan-size"] = uint64(len(t.dispatchCh))

	var callCount, errCount uint64
	for _, info := range t.jobs {
		callCount += info.callCount
		errCount += info.errCount
	}
	stats["call-count"] = callCount
	stats["err-count"] = errCount

	return stats
}
