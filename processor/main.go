package main

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-event-hubs-go"
	"os"
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

func main() {
	closer := setupTracing()
	defer closer.Close()
	ctx := context.Background()
	span, ctx := startSpanFromContext(ctx, "main")
	defer span.Finish()

	hub, err := getHub(ctx)
	if err != nil {
		log.For(ctx).Error(err)
		os.Exit(1)
	}

	exit := make(chan struct{})

	handler := func(ctx context.Context, event *eventhub.Event) error {
		text := string(event.Data)
		log.For(ctx).Debug(fmt.Sprintf("got event with content: %s", text))
		fmt.Println(text)
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	for _, partitionID := range []string{"0", "1", "2", "3"} {
		hub.Receive(ctx, partitionID, handler, eventhub.ReceiveWithLatestOffset())
	}
	cancel()

	fmt.Println("I am listening...")

	select {
	case <-exit:
		fmt.Println("closing after 2 seconds")
		select {
		case <-time.After(2 * time.Second):
			return
		}
	}
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
	tag.Component.Set(span, "processor")
	span.SetTag("version", "1.0.2")
}

