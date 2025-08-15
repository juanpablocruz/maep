// Package testutils provides generic testing utilities and helpers.
package testutils

import (
	"crypto/rand"
	"fmt"
	"runtime"
	"testing"
	"time"
)

// TestFixture represents a generic test fixture interface
type TestFixture interface {
	Setup() error
	Teardown() error
	GetName() string
}

// TestSuite represents a generic test suite
type TestSuite struct {
	Name     string
	Fixtures []TestFixture
	t        *testing.T
}

// NewTestSuite creates a new test suite
func NewTestSuite(t *testing.T, name string) *TestSuite {
	return &TestSuite{
		Name:     name,
		Fixtures: make([]TestFixture, 0),
		t:        t,
	}
}

// AddFixture adds a fixture to the test suite
func (ts *TestSuite) AddFixture(fixture TestFixture) {
	ts.Fixtures = append(ts.Fixtures, fixture)
}

// Setup runs setup for all fixtures
func (ts *TestSuite) Setup() error {
	for _, fixture := range ts.Fixtures {
		if err := fixture.Setup(); err != nil {
			return fmt.Errorf("failed to setup fixture %s: %w", fixture.GetName(), err)
		}
	}
	return nil
}

// Teardown runs teardown for all fixtures
func (ts *TestSuite) Teardown() error {
	for i := len(ts.Fixtures) - 1; i >= 0; i-- {
		fixture := ts.Fixtures[i]
		if err := fixture.Teardown(); err != nil {
			return fmt.Errorf("failed to teardown fixture %s: %w", fixture.GetName(), err)
		}
	}
	return nil
}

// RunTest runs a test with proper setup and teardown
func (ts *TestSuite) RunTest(testName string, testFunc func() error) {
	ts.t.Run(testName, func(t *testing.T) {
		if err := ts.Setup(); err != nil {
			t.Fatalf("Test setup failed: %v", err)
		}
		defer func() {
			if err := ts.Teardown(); err != nil {
				t.Errorf("Test teardown failed: %v", err)
			}
		}()

		if err := testFunc(); err != nil {
			t.Errorf("Test failed: %v", err)
		}
	})
}

// WaitForCondition waits for a condition to be true with timeout
func WaitForCondition(condition func() bool, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("condition not met within timeout %v", timeout)
}

// GenerateRandomBytes generates random bytes of specified length
func GenerateRandomBytes(length int) ([]byte, error) {
	bytes := make([]byte, length)
	_, err := rand.Read(bytes)
	return bytes, err
}

// GenerateRandomString generates a random string of specified length
func GenerateRandomString(length int) (string, error) {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	bytes := make([]byte, length)
	_, err := rand.Read(bytes)
	if err != nil {
		return "", err
	}

	for i := range bytes {
		bytes[i] = charset[bytes[i]%byte(len(charset))]
	}
	return string(bytes), nil
}

// GenerateSequentialStrings generates sequential strings with a prefix
func GenerateSequentialStrings(prefix string, count int) []string {
	strings := make([]string, count)
	for i := 0; i < count; i++ {
		strings[i] = fmt.Sprintf("%s-%d", prefix, i)
	}
	return strings
}

// AssertNoError is a simple assertion helper
func AssertNoError(t *testing.T, err error, msg string) {
	if err != nil {
		t.Errorf("%s: %v", msg, err)
	}
}

// AssertError is a simple assertion helper for expected errors
func AssertError(t *testing.T, err error, msg string) {
	if err == nil {
		t.Errorf("%s: expected error but got none", msg)
	}
}

// AssertEqual is a simple assertion helper
func AssertEqual(t *testing.T, expected, actual interface{}, msg string) {
	if expected != actual {
		t.Errorf("%s: expected %v, got %v", msg, expected, actual)
	}
}

// AssertNotEqual is a simple assertion helper
func AssertNotEqual(t *testing.T, expected, actual interface{}, msg string) {
	if expected == actual {
		t.Errorf("%s: expected not equal to %v, but got %v", msg, expected, actual)
	}
}

// AssertTrue is a simple assertion helper
func AssertTrue(t *testing.T, condition bool, msg string) {
	if !condition {
		t.Errorf("%s: expected true, got false", msg)
	}
}

// AssertFalse is a simple assertion helper
func AssertFalse(t *testing.T, condition bool, msg string) {
	if condition {
		t.Errorf("%s: expected false, got true", msg)
	}
}

// BenchmarkHelper provides utilities for benchmarking
type BenchmarkHelper struct {
	t *testing.T
}

// NewBenchmarkHelper creates a new benchmark helper
func NewBenchmarkHelper(t *testing.T) *BenchmarkHelper {
	return &BenchmarkHelper{t: t}
}

// MeasureTime measures the time taken by a function
func (bh *BenchmarkHelper) MeasureTime(name string, fn func()) {
	start := time.Now()
	fn()
	duration := time.Since(start)
	bh.t.Logf("%s took %v", name, duration)
}

// MeasureMemory measures memory usage of a function
func (bh *BenchmarkHelper) MeasureMemory(name string, fn func()) {
	var m1, m2 runtime.MemStats
	runtime.ReadMemStats(&m1)
	fn()
	runtime.ReadMemStats(&m2)

	allocated := m2.TotalAlloc - m1.TotalAlloc
	bh.t.Logf("%s allocated %d bytes", name, allocated)
}
