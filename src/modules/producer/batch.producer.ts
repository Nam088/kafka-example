import { Producer } from 'kafkajs';
import KafkaConnection from '../../config/kafka.config';
import { getTopicName } from '../../core/topics';
import logger from '../../utils/logger';

/**
 * Batch Producer
 * Demonstrates batch processing and optimized message sending
 */
export class BatchProducer {
  private producer: Producer;
  private isConnected: boolean = false;
  private messageBuffer: Array<{ topic: string; key?: string; value: any }> = [];
  private batchSize: number;
  private flushInterval: NodeJS.Timeout | null = null;

  constructor(batchSize: number = 100, autoFlushIntervalMs: number = 5000) {
    this.producer = KafkaConnection.createProducer();
    this.batchSize = batchSize;
    
    // Auto-flush interval
    if (autoFlushIntervalMs > 0) {
      this.flushInterval = setInterval(() => {
        this.flush().catch(error => {
          logger.error('Auto-flush failed', { error });
        });
      }, autoFlushIntervalMs);
    }
  }

  /**
   * Connect to Kafka
   */
  async connect(): Promise<void> {
    try {
      await this.producer.connect();
      this.isConnected = true;
      logger.info('Batch producer connected successfully');
    } catch (error) {
      logger.error('Failed to connect batch producer', { error });
      throw error;
    }
  }

  /**
   * Disconnect from Kafka
   */
  async disconnect(): Promise<void> {
    try {
      // Flush remaining messages before disconnecting
      await this.flush();
      
      if (this.flushInterval) {
        clearInterval(this.flushInterval);
        this.flushInterval = null;
      }

      await this.producer.disconnect();
      this.isConnected = false;
      logger.info('Batch producer disconnected');
    } catch (error) {
      logger.error('Failed to disconnect batch producer', { error });
      throw error;
    }
  }

  /**
   * Add message to batch buffer
   */
  async addMessage(topic: string, message: any, key?: string): Promise<void> {
    this.messageBuffer.push({ topic, key, value: message });

    // Auto-flush if batch size reached
    if (this.messageBuffer.length >= this.batchSize) {
      await this.flush();
    }
  }

  /**
   * Send all buffered messages
   */
  async flush(): Promise<void> {
    if (!this.isConnected) {
      throw new Error('Producer is not connected');
    }

    if (this.messageBuffer.length === 0) {
      return;
    }

    try {
      // Group messages by topic
      const messagesByTopic = this.groupMessagesByTopic(this.messageBuffer);
      
      // Send to each topic
      const sendPromises = Object.entries(messagesByTopic).map(
        async ([topic, messages]) => {
          const kafkaMessages = messages.map(msg => ({
            key: msg.key || null,
            value: JSON.stringify(msg.value),
            timestamp: Date.now().toString()
          }));

          return this.producer.send({
            topic,
            messages: kafkaMessages
          });
        }
      );

      const results = await Promise.all(sendPromises);
      
      logger.info('Batch messages sent successfully', {
        totalMessages: this.messageBuffer.length,
        topics: Object.keys(messagesByTopic),
        results: results.map(r => r.map(res => ({ 
          partition: res.partition, 
          offset: res.baseOffset 
        })))
      });

      // Clear buffer
      this.messageBuffer = [];
    } catch (error) {
      logger.error('Failed to flush batch messages', { 
        error, 
        bufferSize: this.messageBuffer.length 
      });
      throw error;
    }
  }

  /**
   * Send a large number of messages efficiently
   */
  async sendBulkMessages(
    topic: string, 
    messages: Array<{ key?: string; value: any }>,
    chunkSize: number = 1000
  ): Promise<void> {
    if (!this.isConnected) {
      throw new Error('Producer is not connected');
    }

    try {
      // Process messages in chunks
      for (let i = 0; i < messages.length; i += chunkSize) {
        const chunk = messages.slice(i, i + chunkSize);
        
        const kafkaMessages = chunk.map(msg => ({
          key: msg.key || null,
          value: JSON.stringify(msg.value),
          timestamp: Date.now().toString()
        }));

        await this.producer.send({
          topic,
          messages: kafkaMessages
        });

        logger.info('Bulk chunk sent', {
          topic,
          chunkStart: i,
          chunkSize: chunk.length,
          totalMessages: messages.length
        });
      }

      logger.info('All bulk messages sent successfully', {
        topic,
        totalMessages: messages.length
      });
    } catch (error) {
      logger.error('Failed to send bulk messages', { error, topic });
      throw error;
    }
  }

  /**
   * Send high-throughput demo messages
   */
  async sendHighThroughputDemo(messageCount: number): Promise<void> {
    const startTime = Date.now();
    
    for (let i = 0; i < messageCount; i++) {
      const message = {
        id: `batch-${i}`,
        sequence: i,
        data: `High throughput message ${i}`,
        timestamp: new Date().toISOString(),
        source: 'batch-producer'
      };

      await this.addMessage(getTopicName('DEMO'), message, `key-${i % 10}`);
    }

    // Ensure all messages are sent
    await this.flush();

    const endTime = Date.now();
    const duration = endTime - startTime;
    const throughput = messageCount / (duration / 1000);

    logger.info('High throughput demo completed', {
      messageCount,
      duration: `${duration}ms`,
      throughput: `${throughput.toFixed(2)} msg/sec`
    });
  }

  /**
   * Get current buffer size
   */
  getBufferSize(): number {
    return this.messageBuffer.length;
  }

  /**
   * Group messages by topic
   */
  private groupMessagesByTopic(
    messages: Array<{ topic: string; key?: string; value: any }>
  ): Record<string, Array<{ key?: string; value: any }>> {
    return messages.reduce((groups, message) => {
      const { topic, key, value } = message;
      if (!groups[topic]) {
        groups[topic] = [];
      }
      groups[topic].push({ key, value });
      return groups;
    }, {} as Record<string, Array<{ key?: string; value: any }>>);
  }
}

export default BatchProducer;