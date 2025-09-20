import { Consumer, EachMessagePayload } from 'kafkajs';
import KafkaConnection from '../../config/kafka.config';
import { getTopicName } from '../../core/topics';
import logger from '../../utils/logger';

/**
 * Offset Consumer
 * Demonstrates manual offset management and commit strategies
 */
export class OffsetConsumer {
  private consumer: Consumer;
  private isConnected: boolean = false;
  private isRunning: boolean = false;
  private groupId: string;
  private processedOffsets: Map<string, string> = new Map();

  constructor(groupId?: string) {
    this.groupId = groupId || `offset-consumer-${Date.now()}`;
    this.consumer = KafkaConnection.createConsumer(this.groupId);
  }

  /**
   * Connect to Kafka
   */
  async connect(): Promise<void> {
    try {
      await this.consumer.connect();
      this.isConnected = true;
      logger.info('Offset consumer connected successfully', { groupId: this.groupId });
    } catch (error) {
      logger.error('Failed to connect offset consumer', { error, groupId: this.groupId });
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
      logger.info('Offset consumer disconnected', { groupId: this.groupId });
    } catch (error) {
      logger.error('Failed to disconnect offset consumer', { error, groupId: this.groupId });
      throw error;
    }
  }

  /**
   * Subscribe to topic with manual offset management
   */
  async subscribe(topic: string, fromBeginning: boolean = false): Promise<void> {
    if (!this.isConnected) {
      throw new Error('Consumer is not connected');
    }

    try {
      await this.consumer.subscribe({ topic, fromBeginning });
      logger.info('Subscribed to topic with manual offset control', { 
        topic, 
        fromBeginning,
        groupId: this.groupId 
      });
    } catch (error) {
      logger.error('Failed to subscribe to topic', { error, topic, groupId: this.groupId });
      throw error;
    }
  }

  /**
   * Run consumer with manual offset commits
   */
  async runWithManualCommit(): Promise<void> {
    if (!this.isConnected) {
      throw new Error('Consumer is not connected');
    }

    try {
      this.isRunning = true;
      
      await this.consumer.run({
        // Disable auto-commit
        autoCommit: false,
        
        eachMessage: async (payload: EachMessagePayload) => {
          await this.handleMessageWithManualCommit(payload);
        }
      });
    } catch (error) {
      logger.error('Failed to start offset consumer', { error, groupId: this.groupId });
      this.isRunning = false;
      throw error;
    }
  }

  /**
   * Run consumer with batch offset commits
   */
  async runWithBatchCommit(batchSize: number = 10): Promise<void> {
    if (!this.isConnected) {
      throw new Error('Consumer is not connected');
    }

    try {
      this.isRunning = true;
      let messageCount = 0;
      
      await this.consumer.run({
        autoCommit: false,
        
        eachMessage: async (payload: EachMessagePayload) => {
          await this.handleMessage(payload);
          messageCount++;
          
          // Commit after processing batch
          if (messageCount >= batchSize) {
            await this.commitCurrentOffsets();
            messageCount = 0;
            logger.info('Batch commit completed', { 
              batchSize, 
              groupId: this.groupId 
            });
          }
        }
      });
    } catch (error) {
      logger.error('Failed to start batch offset consumer', { error, groupId: this.groupId });
      this.isRunning = false;
      throw error;
    }
  }

  /**
   * Run consumer with time-based offset commits
   */
  async runWithTimeBasedCommit(commitIntervalMs: number = 5000): Promise<void> {
    if (!this.isConnected) {
      throw new Error('Consumer is not connected');
    }

    try {
      this.isRunning = true;
      
      // Set up periodic commit
      const commitInterval = setInterval(async () => {
        if (this.isRunning) {
          await this.commitCurrentOffsets();
          logger.info('Periodic commit completed', { 
            intervalMs: commitIntervalMs,
            groupId: this.groupId 
          });
        } else {
          clearInterval(commitInterval);
        }
      }, commitIntervalMs);
      
      await this.consumer.run({
        autoCommit: false,
        
        eachMessage: async (payload: EachMessagePayload) => {
          await this.handleMessage(payload);
        }
      });
    } catch (error) {
      logger.error('Failed to start time-based offset consumer', { error, groupId: this.groupId });
      this.isRunning = false;
      throw error;
    }
  }

  /**
   * Handle message with manual commit after processing
   */
  private async handleMessageWithManualCommit(payload: EachMessagePayload): Promise<void> {
    const { topic, partition, message } = payload;
    
    try {
      const value = message.value?.toString();
      const key = message.key?.toString();
      
      logger.info('Processing message with manual commit', {
        topic,
        partition,
        offset: message.offset,
        key,
        groupId: this.groupId
      });

      // Process the message
      if (value) {
        const parsedMessage = JSON.parse(value);
        await this.processMessage(parsedMessage, topic, partition);
      }

      // Manually commit offset after successful processing
      await this.consumer.commitOffsets([
        {
          topic,
          partition,
          offset: (parseInt(message.offset) + 1).toString()
        }
      ]);

      logger.info('Message processed and offset committed', {
        topic,
        partition,
        offset: message.offset,
        groupId: this.groupId
      });

    } catch (error) {
      logger.error('Error processing message with manual commit', {
        error,
        topic,
        partition,
        offset: message.offset,
        groupId: this.groupId
      });
      
      // Don't commit offset on error - message will be reprocessed
      throw error;
    }
  }

  /**
   * Handle message and track offset for later commit
   */
  private async handleMessage(payload: EachMessagePayload): Promise<void> {
    const { topic, partition, message } = payload;
    
    try {
      const value = message.value?.toString();
      const key = message.key?.toString();
      
      logger.debug('Processing message for later commit', {
        topic,
        partition,
        offset: message.offset,
        key,
        groupId: this.groupId
      });

      // Process the message
      if (value) {
        const parsedMessage = JSON.parse(value);
        await this.processMessage(parsedMessage, topic, partition);
      }

      // Track processed offset
      const partitionKey = `${topic}-${partition}`;
      this.processedOffsets.set(partitionKey, (parseInt(message.offset) + 1).toString());

    } catch (error) {
      logger.error('Error processing message', {
        error,
        topic,
        partition,
        offset: message.offset,
        groupId: this.groupId
      });
      
      // Remove from processed offsets on error
      const partitionKey = `${topic}-${partition}`;
      this.processedOffsets.delete(partitionKey);
      
      throw error;
    }
  }

  /**
   * Process individual message (simulate business logic)
   */
  private async processMessage(message: any, topic: string, partition: number): Promise<void> {
    // Simulate different processing times
    const processingTime = Math.random() * 200 + 50; // 50-250ms
    await new Promise(resolve => setTimeout(resolve, processingTime));

    logger.debug('Message processed successfully', {
      messageId: message.id || 'unknown',
      topic,
      partition,
      processingTime: `${processingTime.toFixed(0)}ms`,
      groupId: this.groupId
    });
  }

  /**
   * Commit all tracked offsets
   */
  private async commitCurrentOffsets(): Promise<void> {
    if (this.processedOffsets.size === 0) {
      return;
    }

    try {
      const offsetsToCommit = Array.from(this.processedOffsets.entries()).map(
        ([partitionKey, offset]) => {
          const [topic, partition] = partitionKey.split('-');
          return {
            topic,
            partition: parseInt(partition),
            offset
          };
        }
      );

      await this.consumer.commitOffsets(offsetsToCommit);
      
      logger.info('Committed offsets', {
        commitCount: offsetsToCommit.length,
        offsets: offsetsToCommit,
        groupId: this.groupId
      });

      // Clear tracked offsets
      this.processedOffsets.clear();

    } catch (error) {
      logger.error('Failed to commit offsets', { 
        error, 
        pendingOffsets: this.processedOffsets.size,
        groupId: this.groupId 
      });
      throw error;
    }
  }

  /**
   * Seek to specific offset
   */
  async seekToOffset(topic: string, partition: number, offset: string): Promise<void> {
    try {
      await this.consumer.seek({ topic, partition, offset });
      logger.info('Seeked to offset', { topic, partition, offset, groupId: this.groupId });
    } catch (error) {
      logger.error('Failed to seek to offset', { error, topic, partition, offset });
      throw error;
    }
  }

  /**
   * Seek to beginning of topic
   */
  async seekToBeginning(topic: string, partition?: number): Promise<void> {
    try {
      if (partition !== undefined) {
        await this.consumer.seek({ topic, partition, offset: '0' });
      } else {
        // Seek all partitions to beginning
        // This would need partition discovery first
        logger.info('Seeking all partitions to beginning', { topic });
      }
      
      logger.info('Seeked to beginning', { topic, partition, groupId: this.groupId });
    } catch (error) {
      logger.error('Failed to seek to beginning', { error, topic, partition });
      throw error;
    }
  }

  /**
   * Stop the consumer
   */
  async stop(): Promise<void> {
    try {
      // Commit any pending offsets before stopping
      await this.commitCurrentOffsets();
      
      await this.consumer.stop();
      this.isRunning = false;
      logger.info('Offset consumer stopped', { groupId: this.groupId });
    } catch (error) {
      logger.error('Failed to stop offset consumer', { error, groupId: this.groupId });
      throw error;
    }
  }

  /**
   * Demonstrate reliable message processing with offset management
   */
  async demonstrateReliableProcessing(): Promise<void> {
    await this.subscribe(getTopicName('DEMO'), true);
    
    logger.info('Starting reliable processing demo', { groupId: this.groupId });
    await this.runWithManualCommit();
  }

  /**
   * Get group ID
   */
  getGroupId(): string {
    return this.groupId;
  }

  /**
   * Get pending offset count
   */
  getPendingOffsetCount(): number {
    return this.processedOffsets.size;
  }

  /**
   * Check if running
   */
  isConsumerRunning(): boolean {
    return this.isRunning;
  }
}

export default OffsetConsumer;