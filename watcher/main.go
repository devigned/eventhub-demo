package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/fsnotify/fsnotify"
	"context"
	"github.com/Azure/azure-event-hubs-go"
	"github.com/Azure/azure-amqp-common-go/sas"
	"github.com/Azure/azure-amqp-common-go/conn"
	"github.com/uber/jaeger-client-go/config"
	"github.com/uber/jaeger-client-go"
	"io"
	jaegerlog "github.com/uber/jaeger-client-go/log"
	"github.com/opentracing/opentracing-go"
	tag "github.com/opentracing/opentracing-go/ext"
	"github.com/Azure/azure-amqp-common-go/log"
)

//
var watcher *fsnotify.Watcher

// main
func main() {
	closer := setupTracing()
	defer closer.Close()
	// creates a new file watcher
	watcher, _ = fsnotify.NewWatcher()
	defer watcher.Close()

	ctx := context.Background()
	span, ctx := startSpanFromContext(ctx, "main")

	// starting at the root of the project, walk each file/directory searching for
	// directories
	if err := filepath.Walk(".", watchDir); err != nil {
		fmt.Println("ERROR", err)
	}

	hub, err := getHub(ctx)
	if err != nil {
		fmt.Println("ERROR", err)
		return
	}

	done := make(chan bool)
	//
	go func() {
		defer span.Finish()

		for {
			select {
			// watch for events
			case event := <-watcher.Events:
				log.For(ctx).Debug(fmt.Sprintf("sending event: %#v\n", event))
				err := sendEvent(ctx, hub, event)
				if err != nil {
					log.For(ctx).Error(err)
				}
				log.For(ctx).Debug(fmt.Sprintf("sent event: %#v\n", event))
			case err := <-watcher.Errors:
				log.For(ctx).Error(err)
			}
		}
	}()

	<-done
}

func sendEvent(ctx context.Context, hub *eventhub.Hub, fileEvent fsnotify.Event) error {
	span, ctx := startSpanFromContext(ctx, "sendEvent")
	defer span.Finish()

	return hub.Send(ctx, eventhub.NewEventFromString(fileEvent.String()))
}

func getHub(ctx context.Context) (*eventhub.Hub, error) {
	span, ctx := startSpanFromContext(ctx, "getHub")
	defer span.Finish()

	parsed, err := conn.ParsedConnectionFromStr(os.Getenv("EH_DEMO_CONN_FILES"))
	if err != nil {
		return nil, err
	}
	tokenProvider, err := sas.NewTokenProvider(sas.TokenProviderWithNamespaceAndKey(parsed.Namespace, parsed.KeyName, parsed.Key))
	if err != nil {
		return nil, err
	}
	hub, err := eventhub.NewHub(parsed.Namespace, parsed.HubName, tokenProvider)
	if err != nil {
		return nil, err
	}

	return hub, nil
}

// watchDir gets run as a walk func, searching for directories to add watchers to
func watchDir(path string, fi os.FileInfo, err error) error {
	// since fsnotify can watch all the files in a directory, watchers only need
	// to be added to each nested directory
	if fi.Mode().IsDir() {
		return watcher.Add(path)
	}

	return nil
}

func setupTracing() io.Closer {
	// Sample configuration for testing. Use constant sampling to sample every trace
	// and enable LogSpan to log every span via configured Logger.
	cfg := config.Configuration{
		Sampler: &config.SamplerConfig{
			Type:  jaeger.SamplerTypeConst,
			Param: 1,
		},
		Reporter: &config.ReporterConfig{
			LocalAgentHostPort: "0.0.0.0:6831",
		},
	}

	// Example logger and metrics factory. Use github.com/uber/jaeger-client-go/log
	// and github.com/uber/jaeger-lib/metrics respectively to bind to real logging and metrics
	// frameworks.
	jLogger := jaegerlog.StdLogger

	closer, _ := cfg.InitGlobalTracer(
		"filesystem",
		config.Logger(jLogger),
	)

	return closer
}

func startSpanFromContext(ctx context.Context, operationName string, opts ...opentracing.StartSpanOption) (opentracing.Span, context.Context) {
	span, ctx := opentracing.StartSpanFromContext(ctx, operationName, opts...)
	applyComponentInfo(span)
	return span, ctx
}

// applyComponentInfo applies eventhub library and network info to the span
func applyComponentInfo(span opentracing.Span) {
	tag.Component.Set(span, "watcher")
	span.SetTag("version", "1.0.1")
}