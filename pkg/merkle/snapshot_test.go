package merkle

import (
	"testing"

	"github.com/juanpablocruz/maep/pkg/metrics"
)

func TestMetricsLogger(t *testing.T) {
	logger := metrics.NewMetricsLogger()
	logger.Start()
	defer logger.Close()

	// Test initial state
	initialDesc := logger.ReadDesc()
	if initialDesc != (metrics.DescMetrics{}) {
		t.Errorf("Expected empty DescMetrics initially, got %v", initialDesc)
	}

	initialTransfer := logger.ReadTransfer()
	if initialTransfer != (metrics.TransferMetrics{}) {
		t.Errorf("Expected empty TransferMetrics initially, got %v", initialTransfer)
	}

	// Test logging DescMetrics
	descMet := metrics.DescMetrics{
		Fanout:          16,
		MaxDepth:        10,
		M:               100,
		NodesVisited:    5,
		LeavesTouched:   2,
		Delta:           10,
		Restarts:        1,
		DurationMS:      50.5,
		AvgDepth:        3.2,
		HashComparisons: 80,
		SummaryBytes:    1024,
	}

	logger.Log(descMet)
	logger.Wait()

	readDesc := logger.ReadDesc()
	if readDesc != descMet {
		t.Errorf("Expected %v, got %v", descMet, readDesc)
	}

	// Test logging TransferMetrics
	transferMet := metrics.TransferMetrics{
		M:             200,
		DeltaExact:    15,
		LeavesTouched: 4,
		SummaryBytes:  2048,
		LeafKeysBytes: 1024,
		OpsBytes:      512,
		TotalBytes:    3584,
	}

	logger.Log(transferMet)
	logger.Wait()

	readTransfer := logger.ReadTransfer()
	if readTransfer != transferMet {
		t.Errorf("Expected %v, got %v", transferMet, readTransfer)
	}

	// Verify that logging one type doesn't affect the other
	if logger.ReadDesc() != descMet {
		t.Error("Logging TransferMetrics affected DescMetrics")
	}

	if logger.ReadTransfer() != transferMet {
		t.Error("Logging DescMetrics affected TransferMetrics")
	}
}

func TestMetricsLoggerConcurrent(t *testing.T) {
	logger := metrics.NewMetricsLogger()
	logger.Start()
	defer logger.Close()

	// Test concurrent logging of both types
	done := make(chan bool, 20)

	// Start 10 goroutines logging DescMetrics
	for i := range 10 {
		go func(id int) {
			met := metrics.DescMetrics{
				Fanout:          16,
				MaxDepth:        10,
				M:               id * 100,
				NodesVisited:    id * 10,
				LeavesTouched:   id * 2,
				Delta:           id * 5,
				Restarts:        id,
				DurationMS:      float64(id * 10),
				AvgDepth:        float64(id) / 2.0,
				HashComparisons: id * 16,
				SummaryBytes:    int64(id * 512),
			}
			logger.Log(met)
			done <- true
		}(i)
	}

	// Start 10 goroutines logging TransferMetrics
	for i := range 10 {
		go func(id int) {
			met := metrics.TransferMetrics{
				M:             id * 50,
				DeltaExact:    id * 3,
				LeavesTouched: id * 2,
				SummaryBytes:  int64(id * 256),
				LeafKeysBytes: int64(id * 128),
				OpsBytes:      int64(id * 64),
				TotalBytes:    int64(id * 448),
			}
			logger.Log(met)
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for range 20 {
		<-done
	}

	// Wait for the logger to process all metrics
	logger.Wait()

	// Should have one of the logged metrics for each type
	finalDesc := logger.ReadDesc()
	if finalDesc.M < 0 || finalDesc.M > 900 {
		t.Errorf("Expected DescMetrics M between 0 and 900, got %d", finalDesc.M)
	}

	finalTransfer := logger.ReadTransfer()
	if finalTransfer.M < 0 || finalTransfer.M > 450 {
		t.Errorf("Expected TransferMetrics M between 0 and 450, got %d", finalTransfer.M)
	}

	// Verify the metrics structure is valid
	if finalDesc.Fanout != 16 || finalDesc.MaxDepth != 10 {
		t.Errorf("Expected DescMetrics Fanout=16, MaxDepth=10, got Fanout=%d, MaxDepth=%d", finalDesc.Fanout, finalDesc.MaxDepth)
	}
}
