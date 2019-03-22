package main

import (
	"fmt"
	stdlog "log"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/improbable-eng/thanos/pkg/component"
	"github.com/improbable-eng/thanos/pkg/prober"
	"github.com/improbable-eng/thanos/pkg/receive"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"github.com/improbable-eng/thanos/pkg/store"
	"github.com/improbable-eng/thanos/pkg/store/storepb"
	conntrack "github.com/mwitkow/go-conntrack"
	"github.com/oklog/run"
	"github.com/opentracing-contrib/go-stdlib/nethttp"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/storage/tsdb"
	"google.golang.org/grpc"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

func registerReceive(m map[string]setupFunc, app *kingpin.Application, name string) {
	cmd := app.Command(name, "Accept Prometheus remote write API requests and write to local tsdb (EXPERIMENTAL, this may change drastically without notice)")

	grpcBindAddr, cert, key, clientCA := regGRPCFlags(cmd)
	httpBindAddr := regHTTPAddrFlag(cmd)

	dataDir := cmd.Flag("tsdb.path", "Data directory of TSDB.").
		Default("./data").String()

	m[name] = func(g *run.Group, logger log.Logger, reg *prometheus.Registry, tracer opentracing.Tracer, _ bool) error {
		return runReceive(
			g,
			logger,
			reg,
			tracer,
			*grpcBindAddr,
			*cert,
			*key,
			*clientCA,
			*httpBindAddr,
			*dataDir,
		)
	}
}

func runReceive(
	g *run.Group,
	logger log.Logger,
	reg *prometheus.Registry,
	tracer opentracing.Tracer,
	grpcBindAddr string,
	cert string,
	key string,
	clientCA string,
	httpBindAddr string,
	dataDir string,
) error {
	logger = log.With(logger, "component", component.Receive.String())
	level.Warn(logger).Log("msg", "setting up receive; the Thanos receive component is EXPERIMENTAL, it may break significantly without notice")

	tsdbCfg := &tsdb.Options{
		Retention:        model.Duration(time.Hour * 24 * 15),
		NoLockfile:       true,
		MinBlockDuration: model.Duration(time.Hour * 2),
		MaxBlockDuration: model.Duration(time.Hour * 2),
	}

	localStorage := &tsdb.ReadyStorage{}
	receiver := receive.NewWriter(log.With(logger, "component", "receive-writer"), localStorage)

	// Start all components while we wait for TSDB to open but only load
	// initial config and mark ourselves as ready after it completed.
	dbOpen := make(chan struct{})

	readinessProber := prober.NewProber(component.Receive.String(), logger)
	webHandler := receive.NewHandler(log.With(logger, "component", "receive-handler"), &receive.Options{
		Receiver:        receiver,
		ReadinessProber: readinessProber,
		Registry:        reg,
		ReadyStorage:    localStorage,
	})

	// sync.Once is used to make sure we can close the channel at different execution stages(SIGTERM or when the config is loaded).
	type closeOnce struct {
		C     chan struct{}
		once  sync.Once
		Close func()
	}
	// Wait until the server is ready to handle reloading.
	reloadReady := &closeOnce{
		C: make(chan struct{}),
	}
	reloadReady.Close = func() {
		reloadReady.once.Do(func() {
			close(reloadReady.C)
		})
	}

	level.Debug(logger).Log("msg", "setting up endpoint readiness")
	{
		// Initial configuration loading.
		cancel := make(chan struct{})
		g.Add(
			func() error {
				select {
				case <-dbOpen:
					break
				case <-cancel:
					reloadReady.Close()
					return nil
				}

				reloadReady.Close()

				webHandler.Ready()
				level.Info(logger).Log("msg", "server is ready to receive web requests.")
				<-cancel
				return nil
			},
			func(err error) {
				close(cancel)
			},
		)
	}

	level.Debug(logger).Log("msg", "setting up tsdb")
	{
		// TSDB.
		cancel := make(chan struct{})
		g.Add(
			func() error {
				level.Info(logger).Log("msg", "starting TSDB ...")
				db, err := tsdb.Open(
					dataDir,
					log.With(logger, "component", "tsdb"),
					reg,
					tsdbCfg,
				)
				if err != nil {
					return fmt.Errorf("opening storage failed: %s", err)
				}
				level.Info(logger).Log("msg", "tsdb started")

				startTimeMargin := int64(2 * time.Duration(tsdbCfg.MinBlockDuration).Seconds() * 1000)
				localStorage.Set(db, startTimeMargin)
				close(dbOpen)
				<-cancel
				return nil
			},
			func(err error) {
				if err := localStorage.Close(); err != nil {
					level.Error(logger).Log("msg", "error stopping storage", "err", err)
				}
				close(cancel)
			},
		)
	}

	level.Debug(logger).Log("msg", "setting up grpc server")
	{
		var (
			s   *grpc.Server
			l   net.Listener
			err error
		)
		g.Add(func() error {
			select {
			case <-dbOpen:
				break
			}

			l, err = net.Listen("tcp", grpcBindAddr)
			if err != nil {
				return errors.Wrap(err, "listen API address")
			}

			db := localStorage.Get()
			tsdbStore := store.NewTSDBStore(log.With(logger, "component", "thanos-tsdb-store"), reg, db, component.Receive, nil)

			opts, err := defaultGRPCServerOpts(logger, reg, tracer, cert, key, clientCA)
			if err != nil {
				return errors.Wrap(err, "setup gRPC server")
			}
			s = grpc.NewServer(opts...)
			storepb.RegisterStoreServer(s, tsdbStore)

			level.Info(logger).Log("msg", "listening for StoreAPI gRPC", "address", grpcBindAddr)
			return errors.Wrap(s.Serve(l), "serve gRPC")
		}, func(error) {
			if s != nil {
				s.Stop()
			}
			if l != nil {
				runutil.CloseWithLogOnErr(logger, l, "store gRPC listener")
			}
		})
	}

	level.Debug(logger).Log("msg", "setting up http handler")
	{

		mux := http.NewServeMux()
		registerMetrics(mux, reg)
		registerProfile(mux)
		readinessProber.RegisterInMux(mux)
		webHandler.HandleInMux("/", mux)

		listener, err := net.Listen("tcp", httpBindAddr)
		if err != nil {
			return errors.Wrapf(err, "listen HTTP on address %s", httpBindAddr)
		}

		// Monitor incoming connections with conntrack.
		listener = conntrack.NewListener(listener,
			conntrack.TrackWithName("http"),
			conntrack.TrackWithTracing())

		operationName := nethttp.OperationNameFunc(func(r *http.Request) string {
			return fmt.Sprintf("%s %s", r.Method, r.URL.Path)
		})

		errlog := stdlog.New(log.NewStdlibAdapter(level.Error(logger)), "", 0)

		httpSrv := &http.Server{
			Handler:  nethttp.Middleware(opentracing.GlobalTracer(), mux, operationName),
			ErrorLog: errlog,
		}

		g.Add(func() error {
			level.Info(logger).Log("msg", "Listening for query and metrics", "address", httpBindAddr)
			webHandler.Healthy()
			return httpSrv.Serve(listener)
		}, func(err error) {
			runutil.CloseWithLogOnErr(logger, listener, "receive HTTP listener")
		})
	}
	level.Info(logger).Log("msg", "starting receiver")

	return nil
}
