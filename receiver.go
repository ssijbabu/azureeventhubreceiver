// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package azureeventhubreceiver // import "github.com/open-telemetry/opentelemetry-collector-contrib/receiver/azureeventhubreceiver"

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pipeline"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receiverhelper"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/azureeventhubreceiver/internal/metadata"
)

type dataConsumer interface {
	consume(ctx context.Context, event *azureEvent) error
	setNextLogsConsumer(nextLogsConsumer consumer.Logs)
	setNextMetricsConsumer(nextLogsConsumer consumer.Metrics)
	setNextTracesConsumer(nextTracesConsumer consumer.Traces)
}

type eventhubReceiver struct {
	eventHandler        *eventhubHandler
	signal              pipeline.Signal
	logger              *zap.Logger
	nextLogsConsumer    consumer.Logs
	nextMetricsConsumer consumer.Metrics
	nextTracesConsumer  consumer.Traces
	obsrecv             *receiverhelper.ObsReport
}

func (receiver *eventhubReceiver) Start(ctx context.Context, host component.Host) error {
	return receiver.eventHandler.run(ctx, host)
}

func (receiver *eventhubReceiver) Shutdown(ctx context.Context) error {
	return receiver.eventHandler.close(ctx)
}

func (receiver *eventhubReceiver) setNextLogsConsumer(nextLogsConsumer consumer.Logs) {
	receiver.nextLogsConsumer = nextLogsConsumer
}

func (receiver *eventhubReceiver) setNextMetricsConsumer(nextMetricsConsumer consumer.Metrics) {
	receiver.nextMetricsConsumer = nextMetricsConsumer
}

func (receiver *eventhubReceiver) setNextTracesConsumer(nextTracesConsumer consumer.Traces) {
	receiver.nextTracesConsumer = nextTracesConsumer
}

func (receiver *eventhubReceiver) consume(ctx context.Context, event *azureEvent) error {
	switch receiver.signal {
	case pipeline.SignalLogs:
		return receiver.consumeLogs(ctx, event)
	case pipeline.SignalMetrics:
		return receiver.consumeMetrics(ctx, event)
	case pipeline.SignalTraces:
		return receiver.consumeTraces(ctx, event)
	default:
		return fmt.Errorf("invalid data type: %v", receiver.signal)
	}
}

func (receiver *eventhubReceiver) consumeLogs(ctx context.Context, event *azureEvent) error {
	if receiver.nextLogsConsumer == nil {
		return nil
	}

	logsContext := receiver.obsrecv.StartLogsOp(ctx)

	logs, err := (&plog.JSONUnmarshaler{}).UnmarshalLogs(event.Data())
	if err != nil {
		return fmt.Errorf("failed to unmarshal logs: %w", err)
	}

	receiver.logger.Debug("Log Records", zap.Any("logs", logs))
	err = receiver.nextLogsConsumer.ConsumeLogs(logsContext, logs)
	receiver.obsrecv.EndLogsOp(logsContext, metadata.Type.String(), 1, err)

	return err
}

func (receiver *eventhubReceiver) consumeMetrics(ctx context.Context, event *azureEvent) error {
	if receiver.nextMetricsConsumer == nil {
		return nil
	}

	metricsContext := receiver.obsrecv.StartMetricsOp(ctx)

	metrics, err := (&pmetric.JSONUnmarshaler{}).UnmarshalMetrics(event.Data())
	if err != nil {
		return fmt.Errorf("failed to unmarshal metrics: %w", err)
	}

	receiver.logger.Debug("Metric Records", zap.Any("metrics", metrics))
	err = receiver.nextMetricsConsumer.ConsumeMetrics(metricsContext, metrics)
	receiver.obsrecv.EndMetricsOp(metricsContext, metadata.Type.String(), 1, err)

	return err
}

func (receiver *eventhubReceiver) consumeTraces(ctx context.Context, event *azureEvent) error {
	if receiver.nextTracesConsumer == nil {
		return nil
	}

	tracesContext := receiver.obsrecv.StartTracesOp(ctx)

	traces, err := (&ptrace.JSONUnmarshaler{}).UnmarshalTraces(event.Data())
	if err != nil {
		return fmt.Errorf("failed to unmarshal traces: %w", err)
	}

	receiver.logger.Debug("Trace Records", zap.Any("traces", traces))
	err = receiver.nextTracesConsumer.ConsumeTraces(tracesContext, traces)
	receiver.obsrecv.EndTracesOp(tracesContext, metadata.Type.String(), 1, err)

	return err
}

func newReceiver(
	signal pipeline.Signal,
	eventHandler *eventhubHandler,
	settings receiver.Settings,
) (component.Component, error) {
	obsrecv, err := receiverhelper.NewObsReport(receiverhelper.ObsReportSettings{
		ReceiverID:             settings.ID,
		Transport:              "event",
		ReceiverCreateSettings: settings,
	})
	if err != nil {
		return nil, err
	}

	r := &eventhubReceiver{
		signal:       signal,
		eventHandler: eventHandler,
		logger:       settings.Logger,
		obsrecv:      obsrecv,
	}

	eventHandler.setDataConsumer(r)

	return r, nil
}
