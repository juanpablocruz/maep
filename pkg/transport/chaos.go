package transport

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type ChaosConfig struct {
	// Probabilities [0..1]
	Loss    float64 // drop frame
	Dup     float64 // duplicate once
	Reorder float64 // add extra delay to cause reordering

	// Latency model
	BaseDelay time.Duration // fixed base latency
	Jitter    time.Duration // +/- jitter uniformly
	MaxQueue  int           // cap inbound queue to avoid memory blowups

	// Link toggle
	Up bool

	// Seed (optional). If 0, uses time.Now().UnixNano()
	Seed int64
}

type ChaosEP struct {
	under EndpointIF

	in     chan []byte
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	up atomic.Bool

	cfgMu sync.RWMutex
	cfg   ChaosConfig

	rngMu sync.Mutex
	rng   *rand.Rand
}

// WrapChaos wraps an EndpointIF so both outbound (Send) and inbound (Recv)
// pass through the chaos model.
func WrapChaos(under EndpointIF, cfg ChaosConfig) *ChaosEP {
	if cfg.MaxQueue <= 0 {
		cfg.MaxQueue = 1024
	}
	if cfg.Seed == 0 {
		cfg.Seed = time.Now().UnixNano()
	}
	cep := &ChaosEP{
		under: under,
		in:    make(chan []byte, cfg.MaxQueue),
		cfg:   cfg,
		rng:   rand.New(rand.NewSource(cfg.Seed)),
	}
	cep.up.Store(cfg.Up)

	cep.ctx, cep.cancel = context.WithCancel(context.Background())
	cep.wg.Add(1)
	go cep.pumpRecv()
	return cep
}

func (c *ChaosEP) Close() {
	c.cancel()
	c.wg.Wait()
	c.under.Close()
	close(c.in)
}

func (c *ChaosEP) Recv(ctx context.Context) ([]byte, bool) {
	select {
	case <-ctx.Done():
		return nil, false
	case b, ok := <-c.in:
		return b, ok
	}
}

func (c *ChaosEP) Send(to MemAddr, frame []byte) error {
	if !c.up.Load() {
		// Pretend link is down: behave like an I/O error
		return context.Canceled
	}
	cfg := c.getCfg()

	// Drop?
	if c.roll() < cfg.Loss {
		return nil
	}

	deliver := func(copy []byte, extraDelay time.Duration) error {
		delay := c.delayWithJitter(cfg) + extraDelay
		if delay <= 0 {
			return c.under.Send(to, copy)
		}
		time.AfterFunc(delay, func() { _ = c.under.Send(to, copy) })
		return nil
	}

	// Normal send
	if err := deliver(clone(frame), 0); err != nil {
		return err
	}

	// Dup?
	if c.roll() < cfg.Dup {
		_ = deliver(clone(frame), c.delayWithJitter(cfg))
	}
	return nil
}

func (c *ChaosEP) pumpRecv() {
	defer c.wg.Done()
	for {
		frame, ok := c.under.Recv(c.ctx)
		if !ok {
			return
		}
		if !c.up.Load() {
			continue
		}
		cfg := c.getCfg()

		extra := time.Duration(0)
		// Reorder → add extra random delay
		if c.roll() < cfg.Reorder {
			extra = c.delayWithJitter(cfg)
		}

		delay := c.delayWithJitter(cfg) + extra
		copy := clone(frame)
		if delay <= 0 {
			select {
			case c.in <- copy:
			default:
				// drop if receiver queue full
			}
			continue
		}
		time.AfterFunc(delay, func() {
			select {
			case c.in <- copy:
			default:
				// drop if queue full
			}
		})
	}
}

// --- controls ---

func (c *ChaosEP) SetUp(up bool)        { c.up.Store(up) }
func (c *ChaosEP) SetLoss(p float64)    { c.cfgMu.Lock(); c.cfg.Loss = clamp01(p); c.cfgMu.Unlock() }
func (c *ChaosEP) SetDup(p float64)     { c.cfgMu.Lock(); c.cfg.Dup = clamp01(p); c.cfgMu.Unlock() }
func (c *ChaosEP) SetReorder(p float64) { c.cfgMu.Lock(); c.cfg.Reorder = clamp01(p); c.cfgMu.Unlock() }
func (c *ChaosEP) SetBaseDelay(d time.Duration) {
	c.cfgMu.Lock()
	c.cfg.BaseDelay = d
	c.cfgMu.Unlock()
}
func (c *ChaosEP) SetJitter(d time.Duration) { c.cfgMu.Lock(); c.cfg.Jitter = d; c.cfgMu.Unlock() }
func (c *ChaosEP) GetConfig() ChaosConfig {
	cfg := c.getCfg()
	cfg.Up = c.up.Load()
	return cfg
}

func (c *ChaosEP) getCfg() ChaosConfig { c.cfgMu.RLock(); defer c.cfgMu.RUnlock(); return c.cfg }

func (c *ChaosEP) delayWithJitter(cfg ChaosConfig) time.Duration {
	if cfg.Jitter <= 0 {
		return cfg.BaseDelay
	}
	c.rngMu.Lock()
	defer c.rngMu.Unlock()
	// Uniform in [-Jitter, +Jitter]
	j := time.Duration(c.rng.Int63n(int64(cfg.Jitter)*2)) - cfg.Jitter
	return cfg.BaseDelay + j
}

func (c *ChaosEP) roll() float64 {
	c.rngMu.Lock()
	x := c.rng.Float64()
	c.rngMu.Unlock()
	return x
}

func clamp01(f float64) float64 {
	if f < 0 {
		return 0
	}
	if f > 1 {
		return 1
	}
	return f
}

func clone(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}
