package threadpool

import (
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestThreadPool(t *testing.T) {
	var callBackFnCount uint64
	cbFunc := func(baseValue []interface{}) error {
		atomic.AddUint64(&callBackFnCount, baseValue[0].(uint64))
		return nil
	}

	tp := New(1)
	tp.Init()

	j := tp.NewJob()

	tp.Add(j, cbFunc, uint64(200))
	tp.Add(j, cbFunc, uint64(100))

	tStats, err := tp.Stats()
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), tStats["total-job-executed"].(uint64))
	assert.Equal(t, 1, tStats["thread-pool-size"].(int))
	assert.Equal(t, uint64(0), tStats["err-count"].(uint64))
	assert.Equal(t, uint64(2), tStats["call-count"].(uint64))

	err = tp.Wait(j)
	if err != nil {
		assert.NoError(t, err)
	}
	assert.Equal(t, uint64(300), atomic.LoadUint64(&callBackFnCount))

	tStats, err = tp.Stats()
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), tStats["total-job-executed"].(uint64))
	assert.Equal(t, 1, tStats["thread-pool-size"].(int))

	var callCounter int
	// pass callback func with forced panic
	cbPanicFunc := func(baseValue []interface{}) error {
		if callCounter == 0 {
			callCounter++
			panic("forced panic")
		}
		atomic.AddUint64(&callBackFnCount, baseValue[0].(uint64))
		return nil
	}

	j = tp.NewJob()
	tp.Add(j, cbPanicFunc, uint64(100))
	tp.Add(j, cbPanicFunc, uint64(100))

	err = tp.Wait(j)
	if err != nil {
		assert.NoError(t, err)
	}
	assert.Equal(t, uint64(400), atomic.LoadUint64(&callBackFnCount))
}

func TestThreadPoolScenarios(t *testing.T) {
	var counter uint64
	tests := []struct {
		name          string
		fail          bool
		err           error
		cFn           func([]interface{}) error
		jobCount      int
		subJobCount   int
		subJobParams  []uint64
		expectedValue uint64
	}{
		{
			name: "should be able to successfully execute callback fn with one job",
			cFn: func(baseValue []interface{}) error {
				atomic.AddUint64(&counter, baseValue[0].(uint64))
				return nil
			},
			jobCount:      1,
			subJobCount:   2,
			subJobParams:  []uint64{12, 100},
			expectedValue: 112,
		},
		{
			name: "should be able to successfully execute callback fn with two jobs",
			cFn: func(baseValue []interface{}) error {
				atomic.AddUint64(&counter, baseValue[0].(uint64))
				return nil
			},
			jobCount:      2,
			subJobCount:   2,
			subJobParams:  []uint64{12, 100},
			expectedValue: 224,
		},
		{
			name: "threadpool wait should return errors for respective jobs from callback functions",
			fail: true,
			err:  errors.New("err"),
			cFn: func(baseValue []interface{}) error {
				return errors.New("err")
			},
			jobCount:      2,
			subJobCount:   2,
			subJobParams:  []uint64{12, 100},
			expectedValue: 0,
		},
		{
			name: "should be able to successfully execute callback fn with one job",
			fail: true,
			err:  errors.New("err"),
			cFn: func(baseValue []interface{}) error {
				return errors.New("err")
			},
			jobCount:      2,
			subJobCount:   2,
			subJobParams:  []uint64{12, 100},
			expectedValue: 0,
		},
	}

	tp := New(5)
	tp.Init()

	for _, tt := range tests {
		counter = 0
		for i := 0; i < tt.jobCount; i++ {
			job := tp.NewJob()

			for j := 0; j < tt.subJobCount; j++ {
				tp.Add(job, tt.cFn, tt.subJobParams[j])
			}

			err := tp.Wait(job)
			if tt.fail {
				assert.EqualValues(t, tt.err, err)
			} else {
				assert.NoError(t, err)
			}
		}

		assert.EqualValues(t, tt.expectedValue, counter)
	}
}

func BenchmarkThreadPool(b *testing.B) {
	var callBackFnCount uint64
	cbFunc := func(baseValue []interface{}) error {
		atomic.AddUint64(&callBackFnCount, baseValue[0].(uint64))
		return nil
	}

	tp := New(5)
	tp.Init()

	j := tp.NewJob()

	for n := 0; n < b.N; n++ {
		tp.Add(j, cbFunc, uint64(1))
	}

	err := tp.Wait(j)
	if err != nil {
		assert.NoError(b, err)
	}
}
