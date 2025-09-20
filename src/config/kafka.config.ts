import { Kafka, KafkaConfig, logLevel } from 'kafkajs';
import { config } from 'dotenv';
import logger from '../utils/logger';

config();

/**
 * Kafka Configuration
 * Central configuration for Kafka connection
 */
export class KafkaConnection {
  private static instance: Kafka;
  
  private constructor() {}

  /**
   * Get Kafka instance (Singleton pattern)
   */
  public static getInstance(): Kafka {
    if (!KafkaConnection.instance) {
      const kafkaConfig: KafkaConfig = {
        clientId: process.env.KAFKA_CLIENT_ID || 'kafka-ts-demo',
        brokers: (process.env.KAFKA_BROKERS || 'localhost:9092').split(','),
        logLevel: logLevel.INFO,
        retry: {
          initialRetryTime: 100,
          retries: 8
        },
        connectionTimeout: 3000,
        requestTimeout: 30000
      };

      KafkaConnection.instance = new Kafka(kafkaConfig);
      logger.info('Kafka connection initialized', { 
        clientId: kafkaConfig.clientId,
        brokers: kafkaConfig.brokers 
      });
    }

    return KafkaConnection.instance;
  }

  /**
   * Create a producer instance
   */
  public static createProducer() {
    const kafka = KafkaConnection.getInstance();
    return kafka.producer({
      maxInFlightRequests: 1,
      idempotent: false,
      transactionTimeout: 30000,
      retry: {
        retries: 5
      }
    });
  }

  /**
   * Create a consumer instance
   */
  public static createConsumer(groupId?: string) {
    const kafka = KafkaConnection.getInstance();
    return kafka.consumer({
      groupId: groupId || process.env.KAFKA_GROUP_ID || 'kafka-ts-demo-group',
      sessionTimeout: parseInt(process.env.CONSUMER_SESSION_TIMEOUT || '30000'),
      heartbeatInterval: parseInt(process.env.CONSUMER_HEARTBEAT_INTERVAL || '3000'),
      retry: {
        retries: 5
      }
    });
  }

  /**
   * Create an admin instance
   */
  public static createAdmin() {
    const kafka = KafkaConnection.getInstance();
    return kafka.admin({
      retry: {
        retries: 5
      }
    });
  }

  /**
   * Create a transactional producer
   */
  public static createTransactionalProducer(transactionalId: string) {
    const kafka = KafkaConnection.getInstance();
    return kafka.producer({
      maxInFlightRequests: 1,
      idempotent: true,
      transactionTimeout: 30000,
      transactionalId,
      retry: {
        retries: 5
      }
    });
  }
}

export default KafkaConnection;