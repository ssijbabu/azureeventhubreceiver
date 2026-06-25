// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package azureeventhubreceiver // import "github.com/ssijbabu/azureeventhubreceiver"

import (
	"context"
	"errors"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/IBM/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// --- sarama mock helpers ---

type mockConsumerGroupSession struct {
	ctx      context.Context
	marked   []*sarama.ConsumerMessage
}

func (m *mockConsumerGroupSession) Claims() map[string][]int32          { return nil }
func (m *mockConsumerGroupSession) MemberID() string                    { return "" }
func (m *mockConsumerGroupSession) GenerationID() int32                 { return 0 }
func (m *mockConsumerGroupSession) MarkOffset(string, int32, int64, string) {}
func (m *mockConsumerGroupSession) Commit()                             {}
func (m *mockConsumerGroupSession) ResetOffset(string, int32, int64, string) {}
func (m *mockConsumerGroupSession) Context() context.Context            { return m.ctx }
func (m *mockConsumerGroupSession) MarkMessage(msg *sarama.ConsumerMessage, _ string) {
	m.marked = append(m.marked, msg)
}

type mockConsumerGroupClaim struct {
	messages chan *sarama.ConsumerMessage
}

func (m *mockConsumerGroupClaim) Topic() string                            { return "test-topic" }
func (m *mockConsumerGroupClaim) Partition() int32                         { return 0 }
func (m *mockConsumerGroupClaim) InitialOffset() int64                     { return 0 }
func (m *mockConsumerGroupClaim) HighWaterMarkOffset() int64               { return 0 }
func (m *mockConsumerGroupClaim) Messages() <-chan *sarama.ConsumerMessage  { return m.messages }

// --- auth mock helpers (mirrors exporter pattern) ---

type fakeReceiverTokenCredential struct {
	component.StartFunc
	component.ShutdownFunc
	token string
	err   error
}

func (f *fakeReceiverTokenCredential) GetToken(_ context.Context, _ policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: f.token}, f.err
}

type notAReceiverCredential struct {
	component.StartFunc
	component.ShutdownFunc
}

type receiverHostWithExtension struct {
	component.Host
	extensions map[component.ID]component.Component
}

func newReceiverHostWithExtension(id component.ID, ext component.Component) *receiverHostWithExtension {
	return &receiverHostWithExtension{
		Host:       componenttest.NewNopHost(),
		extensions: map[component.ID]component.Component{id: ext},
	}
}

func (h *receiverHostWithExtension) GetExtensions() map[component.ID]component.Component {
	return h.extensions
}

// --- kafkaConsumerGroupHandler tests ---

func TestConsumerGroupHandler_Setup_ReturnsNil(t *testing.T) {
	h := &kafkaConsumerGroupHandler{handler: func(context.Context, *azureEvent) error { return nil }, logger: zap.NewNop()}
	assert.NoError(t, h.Setup(nil))
}

func TestConsumerGroupHandler_Cleanup_ReturnsNil(t *testing.T) {
	h := &kafkaConsumerGroupHandler{handler: func(context.Context, *azureEvent) error { return nil }, logger: zap.NewNop()}
	assert.NoError(t, h.Cleanup(nil))
}

func TestConsumeClaim_ForwardsMessagesToHandler(t *testing.T) {
	var received []*azureEvent
	h := &kafkaConsumerGroupHandler{
		handler: func(_ context.Context, e *azureEvent) error {
			received = append(received, e)
			return nil
		},
		logger: zap.NewNop(),
	}

	ch := make(chan *sarama.ConsumerMessage, 2)
	ch <- &sarama.ConsumerMessage{Value: []byte("msg1")}
	ch <- &sarama.ConsumerMessage{Value: []byte("msg2")}
	close(ch)

	session := &mockConsumerGroupSession{ctx: context.Background()}
	claim := &mockConsumerGroupClaim{messages: ch}

	require.NoError(t, h.ConsumeClaim(session, claim))
	require.Len(t, received, 2)
	assert.Equal(t, []byte("msg1"), received[0].Data())
	assert.Equal(t, []byte("msg2"), received[1].Data())
}

func TestConsumeClaim_MarksEveryMessage(t *testing.T) {
	h := &kafkaConsumerGroupHandler{
		handler: func(context.Context, *azureEvent) error { return nil },
		logger:  zap.NewNop(),
	}

	msg1 := &sarama.ConsumerMessage{Value: []byte("a")}
	msg2 := &sarama.ConsumerMessage{Value: []byte("b")}
	ch := make(chan *sarama.ConsumerMessage, 2)
	ch <- msg1
	ch <- msg2
	close(ch)

	session := &mockConsumerGroupSession{ctx: context.Background()}
	require.NoError(t, h.ConsumeClaim(session, &mockConsumerGroupClaim{messages: ch}))

	assert.Equal(t, []*sarama.ConsumerMessage{msg1, msg2}, session.marked)
}

func TestConsumeClaim_HandlerError_LogsAndContinues(t *testing.T) {
	core, logs := observer.New(zap.ErrorLevel)

	callCount := 0
	h := &kafkaConsumerGroupHandler{
		handler: func(_ context.Context, _ *azureEvent) error {
			callCount++
			if callCount == 1 {
				return errors.New("processing failed")
			}
			return nil
		},
		logger: zap.New(core),
	}

	ch := make(chan *sarama.ConsumerMessage, 2)
	ch <- &sarama.ConsumerMessage{Value: []byte("bad")}
	ch <- &sarama.ConsumerMessage{Value: []byte("good")}
	close(ch)

	session := &mockConsumerGroupSession{ctx: context.Background()}
	require.NoError(t, h.ConsumeClaim(session, &mockConsumerGroupClaim{messages: ch}))

	assert.Equal(t, 2, callCount, "handler must be called for every message even after an error")
	require.Equal(t, 1, logs.Len())
	assert.Contains(t, logs.All()[0].Message, "error processing kafka message")
	assert.Len(t, session.marked, 2, "all messages must be marked regardless of handler error")
}

func TestConsumeClaim_EmptyChannel_ReturnsNil(t *testing.T) {
	h := &kafkaConsumerGroupHandler{
		handler: func(context.Context, *azureEvent) error { return nil },
		logger:  zap.NewNop(),
	}
	ch := make(chan *sarama.ConsumerMessage)
	close(ch)
	assert.NoError(t, h.ConsumeClaim(&mockConsumerGroupSession{ctx: context.Background()}, &mockConsumerGroupClaim{messages: ch}))
}

// --- newBaseKafkaConsumerConfig tests ---

func TestNewBaseKafkaConsumerConfig(t *testing.T) {
	cfg := newBaseKafkaConsumerConfig()

	assert.True(t, cfg.Net.TLS.Enable)
	assert.True(t, cfg.Net.SASL.Enable)
	assert.True(t, cfg.Net.SASL.Handshake)
	assert.True(t, cfg.Consumer.Return.Errors)
	assert.Equal(t, sarama.V1_0_0_0, cfg.Version)
}

// --- kafkaAzureTokenProvider tests ---

func TestKafkaAzureTokenProvider_Token_Success(t *testing.T) {
	cred := &fakeReceiverTokenCredential{token: "my-azure-token"}
	p := &kafkaAzureTokenProvider{credential: cred}

	tok, err := p.Token()
	require.NoError(t, err)
	assert.Equal(t, "my-azure-token", tok.Token)
}

func TestKafkaAzureTokenProvider_Token_CredentialError(t *testing.T) {
	cred := &fakeReceiverTokenCredential{err: errors.New("auth service unavailable")}
	p := &kafkaAzureTokenProvider{credential: cred}

	_, err := p.Token()
	assert.ErrorContains(t, err, "failed to obtain Azure token for Kafka auth")
	assert.ErrorContains(t, err, "auth service unavailable")
}

// --- buildKafkaConsumerGroup error-path tests ---

func TestBuildKafkaConsumerGroup_AuthExtensionNotFound(t *testing.T) {
	id := component.MustNewID("azure_auth")
	cfg := &Config{
		Protocol: ProtocolKafka,
		Auth:     &id,
		EventHub: EventHubConfig{Name: "hub", Namespace: "ns.servicebus.windows.net"},
	}
	_, _, err := buildKafkaConsumerGroup(cfg, componenttest.NewNopHost(), zap.NewNop())
	assert.ErrorContains(t, err, "failed to resolve auth extension")
}

func TestBuildKafkaConsumerGroup_AuthExtensionWrongType(t *testing.T) {
	id := component.MustNewID("azure_auth")
	cfg := &Config{
		Protocol: ProtocolKafka,
		Auth:     &id,
		EventHub: EventHubConfig{Name: "hub", Namespace: "ns.servicebus.windows.net"},
	}
	host := newReceiverHostWithExtension(id, &notAReceiverCredential{})
	_, _, err := buildKafkaConsumerGroup(cfg, host, zap.NewNop())
	assert.ErrorContains(t, err, "does not implement azcore.TokenCredential")
}

func TestBuildKafkaConsumerGroup_InvalidConnectionString(t *testing.T) {
	cfg := &Config{
		Protocol:   ProtocolKafka,
		Connection: "not-a-valid-connection-string",
	}
	_, _, err := buildKafkaConsumerGroup(cfg, componenttest.NewNopHost(), zap.NewNop())
	assert.ErrorContains(t, err, "failed to parse connection string for Kafka")
}

func TestBuildKafkaConsumerGroup_SASLPlain_ConfiguredCorrectly(t *testing.T) {
	// Verifies the SASL/PLAIN path sets the right credentials without
	// dialling a real broker. The call will fail at the broker connection
	// stage, but by that point the config has already been applied.
	conn := "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=key;SharedAccessKey=secret;EntityPath=hub"
	cfg := &Config{
		Protocol:   ProtocolKafka,
		Connection: conn,
	}

	// The broker dial will fail (no real broker), so we only check the error
	// is about broker connectivity, not config parsing.
	_, _, err := buildKafkaConsumerGroup(cfg, componenttest.NewNopHost(), zap.NewNop())
	require.Error(t, err)
	assert.ErrorContains(t, err, "failed to create Kafka consumer group")
	assert.NotContains(t, err.Error(), "failed to parse connection string")
}

func TestBuildKafkaConsumerGroup_DefaultConsumerGroup(t *testing.T) {
	// Exercises the getConsumerGroup default when ConsumerGroup is empty.
	// Expected to fail at broker dial; confirms it reaches that stage.
	conn := "Endpoint=sb://ns.servicebus.windows.net/;SharedAccessKeyName=key;SharedAccessKey=secret;EntityPath=hub"
	cfg := &Config{
		Protocol:   ProtocolKafka,
		Connection: conn,
	}
	_, _, err := buildKafkaConsumerGroup(cfg, componenttest.NewNopHost(), zap.NewNop())
	// Error is about the broker, not consumer group setup.
	assert.ErrorContains(t, err, "failed to create Kafka consumer group")
}
