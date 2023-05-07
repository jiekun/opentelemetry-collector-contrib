package sampling

import (
	"crypto/md5"
	"encoding/hex"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"sort"
	"sync"
	"time"
)

type uniqueSpan struct {
	spanIdentifier string
	ts             pcommon.Timestamp
}

type uniqueTracePathFilter struct {
	period    time.Duration
	frequency int64
	logger    *zap.Logger
	m         sync.Mutex
	counters  [2]map[string]int64
}

var _ PolicyEvaluator = (*uniqueTracePathFilter)(nil)

func NewUniqueTracePathFilter(logger *zap.Logger, period int32, frequency int64) PolicyEvaluator {
	p := time.Duration(period) * time.Second

	utp := &uniqueTracePathFilter{
		period:    p,
		frequency: frequency,
		logger:    logger,
		m:         sync.Mutex{},
		counters:  [2]map[string]int64{{}, {}},
	}

	go utp.rotate(p)

	return utp
}

// Evaluate looks at the trace data and returns a corresponding SamplingDecision.
func (utp *uniqueTracePathFilter) Evaluate(_ pcommon.TraceID, trace *TraceData) (Decision, error) {
	trace.Lock()
	batches := trace.ReceivedBatches
	trace.Unlock()

	uniqueID, err := utp.calculateUniqueID(batches)
	if err != nil {
		return NotSampled, nil
	}

	freq, err := utp.getFrequencyForUniqueID(uniqueID)
	if err != nil {
		return NotSampled, nil
	}

	if freq <= utp.frequency {
		return Sampled, nil
	}

	return NotSampled, nil
}

func (utp *uniqueTracePathFilter) calculateUniqueID(td ptrace.Traces) (string, error) {
	// sorting by end timestamp
	var sortSlice []uniqueSpan

	for i := 0; i < td.ResourceSpans().Len(); i++ {
		rs := td.ResourceSpans().At(i)
		for j := 0; j < rs.ScopeSpans().Len(); j++ {
			ss := rs.ScopeSpans().At(j)
			for k := 0; k < ss.Spans().Len(); k++ {
				span := ss.Spans().At(k)
				us := uniqueSpan{
					spanIdentifier: span.Name(),
					ts:             span.EndTimestamp(),
				}

				sortSlice = append(sortSlice, us)
			}
		}
	}

	sort.Slice(sortSlice, func(i, j int) bool {
		return sortSlice[i].ts < sortSlice[j].ts
	})

	// calculating
	hasher := md5.New()
	for i := 0; i < len(sortSlice); i++ {
		hasher.Write([]byte(sortSlice[i].spanIdentifier))
	}

	return hex.EncodeToString(hasher.Sum(nil)), nil
}

func (utp *uniqueTracePathFilter) getFrequencyForUniqueID(uniqueID string) (int64, error) {
	utp.m.Lock()
	defer utp.m.Unlock()

	utp.counters[0][uniqueID] += 1
	utp.counters[1][uniqueID] += 1

	return utp.counters[0][uniqueID], nil
}

func (utp *uniqueTracePathFilter) rotate(period time.Duration) {
	for {
		time.Sleep(period / 2)
		utp.sw()
	}
}

func (utp *uniqueTracePathFilter) sw() {
	utp.m.Lock()
	defer utp.m.Unlock()

	size := len(utp.counters[0])
	utp.counters[0] = utp.counters[1]
	utp.counters[1] = make(map[string]int64, size)

	return
}
