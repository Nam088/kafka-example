import { Producer } from 'kafkajs';
import KafkaConnection from '../../config/kafka.config';
import { getTopicName } from '../../core/topics';
import logger from '../../utils/logger';

/**
 * Partition Producer
 * Demonstrates sending messages to specific partitions
 */
export class PartitionProducer {
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
      logger.info('Partition producer connected successfully');
    } catch (error) {
      logger.error('Failed to connect partition producer', { error });
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
      logger.info('Partition producer disconnected');
    } catch (error) {
      logger.error('Failed to disconnect partition producer', { error });
      throw error;
    }
  }

  /**
   * Send message to specific partition
   */
  async sendToPartition(
    topic: string, 
    partition: number, 
    message: any, 
    key?: string
  ): Promise<void> {
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
            partition,
            timestamp: Date.now().toString()
          }
        ]
      });

      logger.info('Message sent to specific partition', {
        topic,
        partition,
        offset: result[0].baseOffset,
        key
      });
    } catch (error) {
      logger.error('Failed to send message to partition', { error, topic, partition });
      throw error;
    }
  }

  /**
   * Send message using key-based partitioning
   */
  async sendWithKeyPartitioning(topic: string, message: any, key: string): Promise<void> {
    if (!this.isConnected) {
      throw new Error('Producer is not connected');
    }

    try {
      const result = await this.producer.send({
        topic,
        messages: [
          {
            key,
            value: JSON.stringify(message),
            timestamp: Date.now().toString()
          }
        ]
      });

      logger.info('Message sent with key partitioning', {
        topic,
        key,
        partition: result[0].partition,
        offset: result[0].baseOffset
      });
    } catch (error) {
      logger.error('Failed to send message with key partitioning', { error, topic, key });
      throw error;
    }
  }

  /**
   * Send messages to different partitions in round-robin fashion
   */
  async sendRoundRobin(
    topic: string, 
    messages: Array<{ key?: string; value: any }>, 
    partitionCount: number
  ): Promise<void> {
    if (!this.isConnected) {
      throw new Error('Producer is not connected');
    }

    try {
      const kafkaMessages = messages.map((msg, index) => ({
        key: msg.key || null,
        value: JSON.stringify(msg.value),
        partition: index % partitionCount,
        timestamp: Date.now().toString()
      }));

      const result = await this.producer.send({
        topic,
        messages: kafkaMessages
      });

      logger.info('Messages sent in round-robin fashion', {
        topic,
        messageCount: messages.length,
        partitions: result.map(r => r.partition)
      });
    } catch (error) {
      logger.error('Failed to send messages round-robin', { error, topic });
      throw error;
    }
  }

  /**
   * Demonstrate user-based partitioning
   */
  async sendUserEvent(userId: string, event: any): Promise<void> {
    const userEvent = {
      userId,
      event,
      timestamp: new Date().toISOString(),
      source: 'partition-producer'
    };

    // Use userId as key for consistent partitioning
    await this.sendWithKeyPartitioning(
      getTopicName('USER_EVENTS'), 
      userEvent, 
      userId
    );
  }

  /**
   * Demonstrate order-based partitioning by region
   */
  async sendOrderEvent(orderId: string, region: string, orderData: any): Promise<void> {
    const orderEvent = {
      orderId,
      region,
      orderData,
      timestamp: new Date().toISOString(),
      source: 'partition-producer'
    };

    // Calculate partition based on region hash
    const regionHash = this.hashCode(region);
    const partition = Math.abs(regionHash) % 3; // Assuming 3 partitions

    await this.sendToPartition(
      getTopicName('ORDER_EVENTS'),
      partition,
      orderEvent,
      orderId
    );
  }

  /**
   * Simple hash function for string
   */
  private hashCode(str: string): number {
    let hash = 0;
    for (let i = 0; i < str.length; i++) {
      const char = str.charCodeAt(i);
      hash = ((hash << 5) - hash) + char;
      hash = hash & hash; // Convert to 32-bit integer
    }
    return hash;
  }
}

export default PartitionProducer;