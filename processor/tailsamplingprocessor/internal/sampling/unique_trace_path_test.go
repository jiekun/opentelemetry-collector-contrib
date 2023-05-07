package sampling

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
	"testing"
	"time"
)

func TestEvaluate_UniqueTracePath(t *testing.T) {
	traceID := pcommon.TraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})

	cases := []struct {
		Desc         string
		spanCounts   []int
		sampledCount int
	}{
		{
			// policy
			"OTTL conditions not set",
			[]int{3, 6, 9},
			3,
		},
	}

	for _, c := range cases {
		t.Run(c.Desc, func(t *testing.T) {
			filter := NewUniqueTracePathFilter(zap.NewNop(), 10, 1)
			traces := newTracesWithSpans(c.spanCounts)

			sampleCount := 0
			for i := range traces {
				decision, err := filter.Evaluate(traceID, traces[i])
				assert.NoError(t, err)
				if decision == Sampled {
					sampleCount++
				}
			}

			assert.Equal(t, sampleCount, c.sampledCount)
		})
	}
}

func newTracesWithSpans(spanCounts []int) []*TraceData {
	var td []*TraceData

	for i := range spanCounts {
		traces := ptrace.NewTraces()
		rs := traces.ResourceSpans().AppendEmpty()
		ils := rs.ScopeSpans().AppendEmpty()

		for j := 0; j < spanCounts[i]; j++ {
			span := ils.Spans().AppendEmpty()
			span.SetTraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16})
			span.SetSpanID([8]byte{1, 2, 3, 4, 5, 6, 7, 8})
			span.SetName(fmt.Sprintf("span_%d", j))
			span.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		}
		td = append(td, &TraceData{
			ReceivedBatches: traces,
		})
	}

	return td
}
