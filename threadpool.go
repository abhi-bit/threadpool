package threadpool

import (
	"encoding/json"
	"log"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	metrics "github.com/jamiealquiza/tachymeter"
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

type Pool struct {
	jobs     map[uint64]*JobInfo
	jobsMtx  *sync.RWMutex
	jobCount uint64

	dispatchCh chan *CbFuncInfo
	poolSize   int

	timingStats    *metrics.Tachymeter
	timingStatsMtx *sync.RWMutex
}

func New(poolSize int) *Pool {
	m := metrics.New(&metrics.Config{Size: int(1000)})

	return &Pool{
		jobs:           make(map[uint64]*JobInfo),
		jobsMtx:        &sync.RWMutex{},
		dispatchCh:     make(chan *CbFuncInfo, poolSize),
		poolSize:       poolSize,
		timingStats:    m,
		timingStatsMtx: &sync.RWMutex{},
	}
}

func (p *Pool) Init() {
	for i := 0; i < p.poolSize; i++ {
		go p.execute()
	}
}

func (p *Pool) execute() {
	defer func() {
		if err := recover(); err != nil {
			log.Println("stacktrace from panic: \n" + string(debug.Stack()))
			p.execute()
		}
	}()

	for {
		select {
		case info := <-p.dispatchCh:
			func() {
				// needed to call callback function in new scope
				// as the cbFunc can panic as well
				defer info.job.wg.Done()

				start := time.Now()
				p.timingStatsMtx.Lock()
				defer p.timingStatsMtx.Unlock()

				defer p.timingStats.AddTime(time.Since(start))

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

func (p *Pool) NewJob() *JobInfo {
	atomic.AddUint64(&p.jobCount, 1)

	job := &JobInfo{
		id:     atomic.LoadUint64(&p.jobCount),
		wg:     &sync.WaitGroup{},
		err:    nil,
		errMtx: &sync.RWMutex{},
	}

	p.jobsMtx.Lock()
	defer p.jobsMtx.Unlock()
	p.jobs[job.id] = job

	return job
}

func (p *Pool) Add(j *JobInfo, fn CallbackFunc, params ...interface{}) {
	j.wg.Add(1)
	atomic.AddUint64(&j.callCount, 1)

	p.dispatchCh <- &CbFuncInfo{
		cbFunc:   fn,
		fnParams: params,
		job:      j,
	}
}

func (p *Pool) Wait(j *JobInfo) error {
	j.wg.Wait()

	p.jobsMtx.Lock()
	defer p.jobsMtx.Unlock()
	delete(p.jobs, j.id)

	return j.err
}

func (p *Pool) Stats() (map[string]interface{}, error) {
	p.timingStatsMtx.RLock()
	defer p.timingStatsMtx.RUnlock()
	info, err := p.timingStats.Calc().MarshalJSON()
	if err != nil {
		return nil, err
	}

	stats := make(map[string]interface{})

	err = json.Unmarshal(info, &stats)
	if err != nil {
		return nil, err
	}

	stats["thread-pool-size"] = p.poolSize
	stats["total-job-executed"] = atomic.LoadUint64(&p.jobCount)

	p.jobsMtx.RLock()
	defer p.jobsMtx.RUnlock()
	stats["active-job-count"] = len(p.jobs)
	stats["dispatch-chan-size"] = len(p.dispatchCh)

	var callCount, errCount uint64
	for _, info := range p.jobs {
		callCount += info.callCount
		errCount += info.errCount
	}
	stats["call-count"] = callCount
	stats["err-count"] = errCount

	return stats, nil
}
