import { Consumer, EachMessagePayload } from 'kafkajs';
import KafkaConnection from '../../config/kafka.config';
import { getTopicName } from '../../core/topics';
import logger from '../../utils/logger';

/**
 * Group Consumer
 * Demonstrates consumer group functionality and load balancing
 */
export class GroupConsumer {
  private consumer: Consumer;
  private isConnected: boolean = false;
  private isRunning: boolean = false;
  private groupId: string;

  constructor(groupId: string) {
    this.groupId = groupId;
    this.consumer = KafkaConnection.createConsumer(groupId);
  }

  /**
   * Connect to Kafka
   */
  async connect(): Promise<void> {
    try {
      await this.consumer.connect();
      this.isConnected = true;
      logger.info('Group consumer connected successfully', { groupId: this.groupId });
    } catch (error) {
      logger.error('Failed to connect group consumer', { error, groupId: this.groupId });
      throw error;
    }
  }

  /**
   * Disconnect from Kafka
   */
  async disconnect(): Promise<void> {
    try {
      if (this.isRunning) {
        await this.stop();
      }
      await this.consumer.disconnect();
      this.isConnected = false;
      logger.info('Group consumer disconnected', { groupId: this.groupId });
    } catch (error) {
      logger.error('Failed to disconnect group consumer', { error, groupId: this.groupId });
      throw error;
    }
  }

  /**
   * Subscribe to topics with consumer group
   */
  async subscribeToTopics(topics: string[]): Promise<void> {
    if (!this.isConnected) {
      throw new Error('Consumer is not connected');
    }

    try {
      for (const topic of topics) {
        await this.consumer.subscribe({ topic, fromBeginning: false });
      }
      logger.info('Group consumer subscribed to topics', { 
        groupId: this.groupId, 
        topics 
      });
    } catch (error) {
      logger.error('Failed to subscribe to topics', { 
        error, 
        groupId: this.groupId, 
        topics 
      });
      throw error;
    }
  }

  /**
   * Start consuming with consumer group coordination
   */
  async runWithGroup(): Promise<void> {
    if (!this.isConnected) {
      throw new Error('Consumer is not connected');
    }

    try {
      this.isRunning = true;
      
      await this.consumer.run({
        // Each message handler
        eachMessage: async (payload: EachMessagePayload) => {
          await this.handleGroupMessage(payload);
        },

        // Batch processing for higher throughput
        eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
          logger.info('Processing batch', {
            groupId: this.groupId,
            topic: batch.topic,
            partition: batch.partition,
            messageCount: batch.messages.length,
            firstOffset: batch.firstOffset(),
            lastOffset: batch.lastOffset()
          });

          for (const message of batch.messages) {
            if (!isRunning() || isStale()) break;

            await this.handleBatchMessage({
              topic: batch.topic,
              partition: batch.partition,
              message
            });

            // Resolve offset after processing each message
            resolveOffset(message.offset);
            
            // Send heartbeat periodically
            await heartbeat();
          }
        }
      });
    } catch (error) {
      logger.error('Failed to start group consumer', { error, groupId: this.groupId });
      this.isRunning = false;
      throw error;
    }
  }

  /**
   * Handle individual message in group context
   */
  private async handleGroupMessage(payload: EachMessagePayload): Promise<void> {
    const { topic, partition, message } = payload;
    
    try {
      const value = message.value?.toString();
      const key = message.key?.toString();
      
      logger.info('Group message received', {
        groupId: this.groupId,
        topic,
        partition,
        offset: message.offset,
        key,
        consumerInstance: this.getConsumerInstanceId()
      });

      if (value) {
        const parsedMessage = JSON.parse(value);
        await this.processGroupMessage(parsedMessage, topic, partition);
      }

    } catch (error) {
      logger.error('Error processing group message', {
        error,
        groupId: this.groupId,
        topic,
        partition,
        offset: message.offset
      });
    }
  }

  /**
   * Handle batch message processing
   */
  private async handleBatchMessage(payload: {
    topic: string;
    partition: number;
    message: any;
  }): Promise<void> {
    const { topic, partition, message } = payload;
    
    try {
      const value = message.value?.toString();
      
      if (value) {
        const parsedMessage = JSON.parse(value);
        await this.processBatchMessage(parsedMessage, topic, partition);
      }

    } catch (error) {
      logger.error('Error processing batch message', {
        error,
        groupId: this.groupId,
        topic,
        partition,
        offset: message.offset
      });
    }
  }

  /**
   * Process message in group context
   */
  private async processGroupMessage(message: any, topic: string, partition: number): Promise<void> {
    // Simulate processing time based on message type
    const processingTime = this.getProcessingTime(message);
    await new Promise(resolve => setTimeout(resolve, processingTime));

    logger.info('Group message processed', {
      groupId: this.groupId,
      topic,
      partition,
      messageId: message.id || 'unknown',
      processingTime: `${processingTime}ms`,
      consumerInstance: this.getConsumerInstanceId()
    });
  }

  /**
   * Process batch message
   */
  private async processBatchMessage(message: any, topic: string, partition: number): Promise<void> {
    // Faster processing for batch mode
    const processingTime = 10;
    await new Promise(resolve => setTimeout(resolve, processingTime));

    logger.debug('Batch message processed', {
      groupId: this.groupId,
      topic,
      partition,
      messageId: message.id || 'unknown'
    });
  }

  /**
   * Stop the consumer group
   */
  async stop(): Promise<void> {
    try {
      await this.consumer.stop();
      this.isRunning = false;
      logger.info('Group consumer stopped', { groupId: this.groupId });
    } catch (error) {
      logger.error('Failed to stop group consumer', { error, groupId: this.groupId });
      throw error;
    }
  }

  /**
   * Create multiple consumers in the same group for load balancing demo
   */
  static async createConsumerGroup(
    groupId: string, 
    consumerCount: number, 
    topics: string[]
  ): Promise<GroupConsumer[]> {
    const consumers: GroupConsumer[] = [];

    for (let i = 0; i < consumerCount; i++) {
      const consumer = new GroupConsumer(`${groupId}-instance-${i}`);
      await consumer.connect();
      await consumer.subscribeToTopics(topics);
      consumers.push(consumer);
    }

    logger.info('Consumer group created', {
      groupId,
      consumerCount,
      topics
    });

    return consumers;
  }

  /**
   * Demonstrate load balancing across consumer group
   */
  async demonstrateLoadBalancing(): Promise<void> {
    const topics = [getTopicName('DEMO'), getTopicName('USER_EVENTS')];
    await this.subscribeToTopics(topics);
    
    await this.runWithGroup();
  }

  /**
   * Get simulated processing time based on message content
   */
  private getProcessingTime(message: any): number {
    if (message.type === 'heavy') {
      return 1000; // 1 second for heavy processing
    } else if (message.type === 'medium') {
      return 500; // 500ms for medium processing
    }
    return 100; // 100ms for light processing
  }

  /**
   * Get consumer instance identifier
   */
  private getConsumerInstanceId(): string {
    return `${this.groupId}-${process.pid}`;
  }

  /**
   * Get group ID
   */
  getGroupId(): string {
    return this.groupId;
  }

  /**
   * Check if running
   */
  isConsumerRunning(): boolean {
    return this.isRunning;
  }
}

export default GroupConsumer;