import { Producer } from 'kafkajs';
import KafkaConnection from '../../config/kafka.config';
import { getTopicName } from '../../core/topics';
import logger from '../../utils/logger';

/**
 * Basic Producer
 * Demonstrates simple message sending to Kafka topics
 */
export class BasicProducer {
  private producer: Producer;
  private isConnected: boolean = false;

  constructor() {
    this.producer = KafkaConnection.createProducer();
  }

  /**
   * Connect to Kafka
   */
  async connect(): Promise<void> {
    try {
      await this.producer.connect();
      this.isConnected = true;
      logger.info('Basic producer connected successfully');
    } catch (error) {
      logger.error('Failed to connect basic producer', { error });
      throw error;
    }
  }

  /**
   * Disconnect from Kafka
   */
  async disconnect(): Promise<void> {
    try {
      await this.producer.disconnect();
      this.isConnected = false;
      logger.info('Basic producer disconnected');
    } catch (error) {
      logger.error('Failed to disconnect basic producer', { error });
      throw error;
    }
  }

  /**
   * Send a simple message
   */
  async sendMessage(topic: string, message: any, key?: string): Promise<void> {
    if (!this.isConnected) {
      throw new Error('Producer is not connected');
    }

    try {
      const result = await this.producer.send({
        topic,
        messages: [
          {
            key: key || null,
            value: JSON.stringify(message),
            timestamp: Date.now().toString()
          }
        ]
      });

      logger.info('Message sent successfully', {
        topic,
        partition: result[0].partition,
        offset: result[0].baseOffset,
        key
      });
    } catch (error) {
      logger.error('Failed to send message', { error, topic, key });
      throw error;
    }
  }

  /**
   * Send multiple messages at once
   */
  async sendMessages(topic: string, messages: Array<{ key?: string; value: any }>): Promise<void> {
    if (!this.isConnected) {
      throw new Error('Producer is not connected');
    }

    try {
      const kafkaMessages = messages.map(msg => ({
        key: msg.key || null,
        value: JSON.stringify(msg.value),
        timestamp: Date.now().toString()
      }));

      const result = await this.producer.send({
        topic,
        messages: kafkaMessages
      });

      logger.info('Multiple messages sent successfully', {
        topic,
        messageCount: messages.length,
        partitions: result.map(r => r.partition)
      });
    } catch (error) {
      logger.error('Failed to send multiple messages', { error, topic });
      throw error;
    }
  }

  /**
   * Send a demo message to demo topic
   */
  async sendDemoMessage(message: string, key?: string): Promise<void> {
    const demoMessage = {
      id: Math.random().toString(36).substr(2, 9),
      message,
      timestamp: new Date().toISOString(),
      source: 'basic-producer'
    };

    await this.sendMessage(getTopicName('DEMO'), demoMessage, key);
  }
}

export default BasicProducer;