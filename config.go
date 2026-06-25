// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package azureeventhubreceiver // import "github.com/ssijbabu/azureeventhubreceiver"

import (
	"errors"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azeventhubs/v2"
	"go.opentelemetry.io/collector/component"
)

// Protocol selects the wire protocol used to receive messages from Azure Event Hubs.
type Protocol string

const (
	// ProtocolAMQP uses the native Azure Event Hubs AMQP protocol (default).
	ProtocolAMQP Protocol = "amqp"
	// ProtocolKafka uses the Azure Event Hubs Kafka-compatible endpoint.
	ProtocolKafka Protocol = "kafka"
)

type Config struct {
	// Protocol selects the wire protocol: "amqp" (default) or "kafka".
	Protocol Protocol `mapstructure:"protocol"`

	// Connection is the full Event Hub connection string. Required when Auth is not set.
	Connection string `mapstructure:"connection"`

	// EventHub holds Event Hub identity fields used when Auth is set.
	EventHub EventHubConfig `mapstructure:"event_hub"`

	// Auth is the component ID of an auth extension implementing azcore.TokenCredential.
	// When set, Connection is ignored for credential purposes.
	Auth *component.ID `mapstructure:"auth"`

	// ConsumerGroup is the Kafka / AMQP consumer group name. Defaults to "$Default".
	ConsumerGroup string `mapstructure:"group"`

	// AMQP-only: listen to a single partition instead of all partitions.
	Partition string `mapstructure:"partition"`
	// AMQP-only: starting offset within the partition specified by Partition.
	Offset string `mapstructure:"offset"`

	// AMQP-only: storage extension for per-partition checkpoint persistence.
	// Mutually exclusive with BlobCheckpointStore.
	StorageID *component.ID `mapstructure:"storage"`

	// AMQP-only: Azure Blob Storage checkpoint store for distributed consumption.
	// Mutually exclusive with StorageID, Partition, and Offset.
	BlobCheckpointStore *BlobCheckpointStoreConfig `mapstructure:"blob_checkpoint_store"`

	// AMQP-only tuning parameters.
	PollRate      int   `mapstructure:"poll_rate"`
	MaxPollEvents int   `mapstructure:"max_poll_events"`
	PrefetchCount int32 `mapstructure:"prefetch_count"`
}

// BlobCheckpointStoreConfig defines Azure Blob Storage checkpoint coordination
// for distributed AMQP consumption.
type BlobCheckpointStoreConfig struct {
	Connection        string `mapstructure:"connection"`
	StorageAccountURL string `mapstructure:"storage_account_url"`
	ContainerName     string `mapstructure:"container_name"`
}

// EventHubConfig holds the Event Hub identity fields used when Auth is set.
type EventHubConfig struct {
	Name      string `mapstructure:"name"`
	Namespace string `mapstructure:"namespace"`
}

func (config *Config) Validate() error {
	if config.Protocol != "" && config.Protocol != ProtocolAMQP && config.Protocol != ProtocolKafka {
		return fmt.Errorf("unsupported protocol %q: must be %q or %q", config.Protocol, ProtocolAMQP, ProtocolKafka)
	}

	if config.Protocol == ProtocolKafka {
		return config.validateKafka()
	}
	return config.validateAMQP()
}

func (config *Config) validateAMQP() error {
	if config.Auth != nil {
		if config.EventHub.Name == "" {
			return errors.New("event_hub.name is required when using auth")
		}
		if config.EventHub.Namespace == "" {
			return errors.New("event_hub.namespace is required when using auth")
		}
	} else {
		if config.Connection == "" {
			return errors.New("missing connection")
		}
		if _, err := azeventhubs.ParseConnectionString(config.Connection); err != nil {
			return err
		}
	}

	if config.Partition == "" && config.Offset != "" {
		return errors.New("cannot use 'offset' without 'partition'")
	}

	if config.BlobCheckpointStore != nil {
		if config.BlobCheckpointStore.ContainerName == "" {
			return errors.New("blob_checkpoint_store.container_name is required")
		}
		if config.Auth == nil && config.BlobCheckpointStore.Connection == "" {
			return errors.New("blob_checkpoint_store.connection is required when not using auth")
		}
		if config.Auth != nil && config.BlobCheckpointStore.StorageAccountURL == "" {
			return errors.New("blob_checkpoint_store.storage_account_url is required when using auth")
		}
		if config.Partition != "" || config.Offset != "" {
			return errors.New("blob_checkpoint_store is mutually exclusive with partition and offset")
		}
		if config.StorageID != nil {
			return errors.New("blob_checkpoint_store is mutually exclusive with storage")
		}
	}

	return nil
}

func (config *Config) validateKafka() error {
	if config.BlobCheckpointStore != nil {
		return errors.New("blob_checkpoint_store is not supported with the Kafka protocol")
	}
	if config.StorageID != nil {
		return errors.New("storage is not supported with the Kafka protocol")
	}
	if config.Partition != "" || config.Offset != "" {
		return errors.New("partition and offset are not supported with the Kafka protocol")
	}

	if config.Auth != nil {
		if config.EventHub.Name == "" {
			return errors.New("event_hub.name is required when using auth")
		}
		if config.EventHub.Namespace == "" {
			return errors.New("event_hub.namespace is required when using auth")
		}
	} else {
		if config.Connection == "" {
			return errors.New("missing connection")
		}
		if _, err := azeventhubs.ParseConnectionString(config.Connection); err != nil {
			return err
		}
	}

	return nil
}
