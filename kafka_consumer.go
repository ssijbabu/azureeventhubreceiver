// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package azureeventhubreceiver // import "github.com/ssijbabu/azureeventhubreceiver"

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/IBM/sarama"
	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"
)

// kafkaConsumerGroupHandler implements sarama.ConsumerGroupHandler, forwarding
// each Kafka message as an azureEvent to the hub handler.
type kafkaConsumerGroupHandler struct {
	handler hubHandler
	logger  *zap.Logger
}

func (k *kafkaConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (k *kafkaConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }

func (k *kafkaConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		if err := k.handler(session.Context(), newAzureEventFromBytes(msg.Value)); err != nil {
			k.logger.Error("error processing kafka message", zap.Error(err))
		}
		session.MarkMessage(msg, "")
	}
	return nil
}

// buildKafkaConsumerGroup creates a sarama ConsumerGroup and returns it along with
// the topic name derived from the config. Auth via SAS credentials or AAD token.
func buildKafkaConsumerGroup(config *Config, host component.Host, logger *zap.Logger) (sarama.ConsumerGroup, string, error) {
	cfg := newBaseKafkaConsumerConfig()

	var brokers []string
	var topic string

	if config.Auth != nil {
		ext, ok := host.GetExtensions()[*config.Auth]
		if !ok {
			return nil, "", fmt.Errorf("failed to resolve auth extension %q", *config.Auth)
		}
		cred, ok := ext.(azcore.TokenCredential)
		if !ok {
			return nil, "", fmt.Errorf("extension %q does not implement azcore.TokenCredential", *config.Auth)
		}
		cfg.Net.SASL.Mechanism = sarama.SASLTypeOAuth
		cfg.Net.SASL.TokenProvider = &kafkaAzureTokenProvider{credential: cred}
		brokers = []string{config.EventHub.Namespace + ":9093"}
		topic = config.EventHub.Name
	} else {
		cfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		cfg.Net.SASL.User = "$ConnectionString"
		cfg.Net.SASL.Password = config.EventHub.toConnectionString()
		brokers = []string{config.EventHub.Namespace + ":9093"}
		topic = config.EventHub.Name
	}

	group := getConsumerGroup(config)
	cg, err := sarama.NewConsumerGroup(brokers, group, cfg)
	if err != nil {
		return nil, "", fmt.Errorf("failed to create Kafka consumer group: %w", err)
	}

	logger.Info("Kafka consumer group created",
		zap.Strings("brokers", brokers),
		zap.String("topic", topic),
		zap.String("group", group),
	)
	return cg, topic, nil
}

// newBaseKafkaConsumerConfig returns a sarama.Config pre-configured for the Azure
// Event Hubs Kafka endpoint: TLS on, SASL on, version pinned to 1.0.0.0.
func newBaseKafkaConsumerConfig() *sarama.Config {
	cfg := sarama.NewConfig()
	cfg.Version = sarama.V1_0_0_0
	cfg.Net.TLS.Enable = true
	cfg.Net.SASL.Enable = true
	cfg.Net.SASL.Handshake = true
	cfg.Consumer.Return.Errors = true
	return cfg
}

// kafkaAzureTokenProvider fetches Azure AD OAuth tokens for SASL/OAUTHBEARER auth.
type kafkaAzureTokenProvider struct {
	credential azcore.TokenCredential
}

func (p *kafkaAzureTokenProvider) Token() (*sarama.AccessToken, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	token, err := p.credential.GetToken(ctx, policy.TokenRequestOptions{
		Scopes: []string{"https://eventhubs.azure.net/.default"},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to obtain Azure token for Kafka auth: %w", err)
	}
	return &sarama.AccessToken{Token: token.Token}, nil
}
