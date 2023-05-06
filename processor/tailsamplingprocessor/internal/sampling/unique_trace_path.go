package sampling

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.uber.org/zap"
	"time"
)

type uniqueTracePathFilter struct {
	period    time.Duration
	frequency int32
	logger    *zap.Logger
}

var _ PolicyEvaluator = (*uniqueTracePathFilter)(nil)

func NewUniqueTracePathFilter(logger *zap.Logger, period int32, frequency int32) PolicyEvaluator {
	return &uniqueTracePathFilter{
		period:    time.Duration(period) * time.Second,
		frequency: frequency,
		logger:    logger,
	}
}

// Evaluate looks at the trace data and returns a corresponding SamplingDecision.
func (utp *uniqueTracePathFilter) Evaluate(_ pcommon.TraceID, trace *TraceData) (Decision, error) {
	return NotSampled, nil
}
