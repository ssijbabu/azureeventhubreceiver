// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package azureeventhubreceiver // import "github.com/ssijbabu/azureeventhubreceiver"

import (
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pipeline"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/ssijbabu/azureeventhubreceiver/internal/metadata"
)

const testConnection = "Endpoint=sb://namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=superSecret1234=;EntityPath=hubName"

func newTestEvent(data string) *azureEvent {
	return &azureEvent{
		AzEventData: &azeventhubs.ReceivedEventData{
			EventData: azeventhubs.EventData{
				Body: []byte(data),
			},
		},
	}
}

func otlpLogsJSON(t *testing.T) []byte {
	t.Helper()
	ld := plog.NewLogs()
	ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("hello")
	b, err := (&plog.JSONMarshaler{}).MarshalLogs(ld)
	require.NoError(t, err)
	return b
}

func otlpMetricsJSON(t *testing.T) []byte {
	t.Helper()
	md := pmetric.NewMetrics()
	md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty().SetName("cpu")
	b, err := (&pmetric.JSONMarshaler{}).MarshalMetrics(md)
	require.NoError(t, err)
	return b
}

func otlpTracesJSON(t *testing.T) []byte {
	t.Helper()
	td := ptrace.NewTraces()
	td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty().SetName("op")
	b, err := (&ptrace.JSONMarshaler{}).MarshalTraces(td)
	require.NoError(t, err)
	return b
}

func newTestReceiver(t *testing.T, signal pipeline.Signal) (*eventhubReceiver, func()) {
	t.Helper()
	config := createDefaultConfig().(*Config)
	config.Connection = testConnection

	settings := receivertest.NewNopSettings(metadata.Type)
	eventHandler := newEventhubHandler(config, settings)
	eventHandler.hub = &mockHubWrapper{}

	rcvr, err := newReceiver(signal, eventHandler, settings)
	require.NoError(t, err)

	r := rcvr.(*eventhubReceiver)
	require.NoError(t, r.Start(t.Context(), componenttest.NewNopHost()))
	return r, func() { require.NoError(t, r.Shutdown(t.Context())) }
}

func TestConsumeLogs_OTLPJson(t *testing.T) {
	r, stop := newTestReceiver(t, pipeline.SignalLogs)
	defer stop()

	sink := new(consumertest.LogsSink)
	r.setNextLogsConsumer(sink)

	require.NoError(t, r.consume(t.Context(), newTestEvent(string(otlpLogsJSON(t)))))
	require.Len(t, sink.AllLogs(), 1)
	assert.Equal(t, 1, sink.AllLogs()[0].LogRecordCount())
}

func TestConsumeMetrics_OTLPJson(t *testing.T) {
	r, stop := newTestReceiver(t, pipeline.SignalMetrics)
	defer stop()

	sink := new(consumertest.MetricsSink)
	r.setNextMetricsConsumer(sink)

	require.NoError(t, r.consume(t.Context(), newTestEvent(string(otlpMetricsJSON(t)))))
	require.Len(t, sink.AllMetrics(), 1)
	assert.Equal(t, 1, sink.AllMetrics()[0].MetricCount())
}

func TestConsumeTraces_OTLPJson(t *testing.T) {
	r, stop := newTestReceiver(t, pipeline.SignalTraces)
	defer stop()

	sink := new(consumertest.TracesSink)
	r.setNextTracesConsumer(sink)

	require.NoError(t, r.consume(t.Context(), newTestEvent(string(otlpTracesJSON(t)))))
	require.Len(t, sink.AllTraces(), 1)
	assert.Equal(t, 1, sink.AllTraces()[0].SpanCount())
}

func TestConsumeInvalidJSON_ReturnsError(t *testing.T) {
	r, stop := newTestReceiver(t, pipeline.SignalLogs)
	defer stop()

	sink := new(consumertest.LogsSink)
	r.setNextLogsConsumer(sink)

	err := r.consume(t.Context(), newTestEvent("not valid otlp json"))
	assert.ErrorContains(t, err, "failed to unmarshal logs")
}

func TestNewAzureEventFromBytes(t *testing.T) {
	body := []byte("kafka payload")
	event := newAzureEventFromBytes(body)
	assert.Equal(t, body, event.Data())
	assert.Nil(t, event.EnqueueTime())
	assert.Nil(t, event.Properties())
}
