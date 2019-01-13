package prober

import (
	"fmt"
	"io"
	"net/http"
	"sync"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/route"
)

const (
	healthyEndpointPath  = "/-/healthy"
	readyEndpointPath    = "/-/ready"
	okProbeText          = "thanos %s is %s"
	probeErrorHTTPStatus = 500
	initialErrorText     = "thanos %s is initializing"
)

// Prober represents health and readriness status of given compoent.
type Prober struct {
	logger       log.Logger
	loggerMtx    sync.Mutex
	componentMtx sync.Mutex
	component    string
	readyMtx     sync.Mutex
	readiness    error
	healthyMtx   sync.Mutex
	healthiness  error
}

// SetLogger sets logger used by the Prober.
func (p *Prober) SetLogger(logger log.Logger) {
	p.loggerMtx.Lock()
	defer p.loggerMtx.Unlock()
	p.logger = logger
}

func (p *Prober) getLogger() log.Logger {
	p.loggerMtx.Lock()
	defer p.loggerMtx.Unlock()
	return p.logger
}

// SetComponent sets component name of the Prober displayed in responses.
func (p *Prober) SetComponent(component string) {
	p.componentMtx.Lock()
	defer p.componentMtx.Unlock()
	p.component = component
}

func (p *Prober) getComponent() string {
	p.componentMtx.Lock()
	defer p.componentMtx.Unlock()
	return p.component
}

// NewProber returns Prober reprezenting readiness and healthiness of given component.
func NewProber(component string, logger log.Logger) *Prober {
	initialErr := fmt.Errorf(initialErrorText, component)
	prober := &Prober{}
	prober.SetComponent(component)
	prober.SetLogger(logger)
	prober.SetNotHealthy(initialErr)
	prober.SetNotReady(initialErr)
	return prober
}

// NewProbeInRouter returns new Prober which registers it's ready and health endpoints to given router.
func NewProbeInRouter(component string, router *route.Router, logger log.Logger) *Prober {
	prober := NewProber(component, logger)
	router.Get(healthyEndpointPath, prober.probeHandlerFunc(prober.IsHealthy, "healthy"))
	router.Get(readyEndpointPath, prober.probeHandlerFunc(prober.IsReady, "ready"))
	return prober
}

// NewProbeInMux returns new Prober which registers it's ready and health endpoints to given mux.
func NewProbeInMux(component string, mux *http.ServeMux, logger log.Logger) *Prober {
	prober := NewProber(component, logger)
	mux.HandleFunc(healthyEndpointPath, prober.probeHandlerFunc(prober.IsHealthy, "healthy"))
	mux.HandleFunc(readyEndpointPath, prober.probeHandlerFunc(prober.IsReady, "ready"))
	return prober
}

func (p *Prober) probeHandlerFunc(probeFunc func() error, probeType string) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, _ *http.Request) {
		err := probeFunc()
		if err == nil {
			if _, e := io.WriteString(w, fmt.Sprintf(okProbeText, p.getComponent(), probeType)); e == nil {
				level.Error(p.getLogger()).Log("msg", "failed to write probe response", "probe type", probeType, "err", err)
			}
		} else {
			http.Error(w, err.Error(), probeErrorHTTPStatus)
		}
	}
}

// IsReady returns error if component is not ready and nil if it is.
func (p *Prober) IsReady() error {
	p.readyMtx.Lock()
	defer p.readyMtx.Unlock()
	return p.readiness
}

// SetReady sets components status to ready.
func (p *Prober) SetReady() {
	level.Debug(p.getLogger()).Log("msg", "changing probe status", "status", "ready")
	p.SetNotReady(nil)
}

// SetNotReady sets components status to not ready with given error as a cause.
func (p *Prober) SetNotReady(err error) {
	p.readyMtx.Lock()
	defer p.readyMtx.Unlock()
	if err != nil {
		level.Debug(p.getLogger()).Log("msg", "changing probe status", "status", "not-ready", "reason", err)
	}
	p.readiness = err
}

// IsHealthy returns error if component is not healthy and nil if it is.
func (p *Prober) IsHealthy() error {
	p.healthyMtx.Lock()
	defer p.healthyMtx.Unlock()
	return p.healthiness
}

// SetHealthy sets components status to healthy.
func (p *Prober) SetHealthy() {
	level.Debug(p.getLogger()).Log("msg", "changing probe status", "status", "healthy")
	p.SetNotHealthy(nil)
}

// SetNotHealthy sets components status to not healthy with given error as a cause.
func (p *Prober) SetNotHealthy(err error) {
	p.healthyMtx.Lock()
	defer p.healthyMtx.Unlock()
	if err != nil {
		level.Debug(p.getLogger()).Log("msg", "changing probe status", "status", "unhealthy", "reason", err)
	}
	p.healthiness = err
}
