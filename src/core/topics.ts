/**
 * Common Kafka Topics Configuration
 * Defines all the topics used across the application
 */

export interface TopicConfig {
  name: string;
  partitions?: number;
  replicationFactor?: number;
  configs?: Record<string, string>;
}

export const TOPICS = {
  // Demo topic for basic examples
  DEMO: {
    name: process.env.DEMO_TOPIC || 'demo-topic',
    partitions: 3,
    replicationFactor: 1,
    configs: {
      'cleanup.policy': 'delete',
      'retention.ms': '604800000' // 7 days
    }
  },

  // User events topic
  USER_EVENTS: {
    name: process.env.USER_EVENTS_TOPIC || 'user-events',
    partitions: 6,
    replicationFactor: 1,
    configs: {
      'cleanup.policy': 'compact',
      'retention.ms': '2678400000' // 31 days
    }
  },

  // Order events topic
  ORDER_EVENTS: {
    name: process.env.ORDER_EVENTS_TOPIC || 'order-events',
    partitions: 3,
    replicationFactor: 1,
    configs: {
      'cleanup.policy': 'delete',
      'retention.ms': '2678400000' // 31 days
    }
  },

  // Transaction topic for transactional examples
  TRANSACTION: {
    name: process.env.TRANSACTION_TOPIC || 'transaction-topic',
    partitions: 3,
    replicationFactor: 1,
    configs: {
      'cleanup.policy': 'delete',
      'retention.ms': '604800000' // 7 days
    }
  }
} as const;

export type TopicName = keyof typeof TOPICS;

/**
 * Get all topic configurations as an array
 */
export function getAllTopics(): TopicConfig[] {
  return Object.values(TOPICS);
}

/**
 * Get topic configuration by name
 */
export function getTopicConfig(topicName: TopicName): TopicConfig {
  return TOPICS[topicName];
}

/**
 * Get topic name by key
 */
export function getTopicName(topicName: TopicName): string {
  return TOPICS[topicName].name;
}