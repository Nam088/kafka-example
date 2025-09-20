import { Consumer, EachMessagePayload } from 'kafkajs';
import KafkaConnection from '../../config/kafka.config';
import { getTopicName } from '../../core/topics';
import logger from '../../utils/logger';

/**
 * Basic Consumer
 * Demonstrates simple message consumption from Kafka topics
 */
export class BasicConsumer {
  private consumer: Consumer;
  private isConnected: boolean = false;
  private isRunning: boolean = false;

  constructor(groupId?: string) {
    this.consumer = KafkaConnection.createConsumer(groupId);
  }

  /**
   * Connect to Kafka
   */
  async connect(): Promise<void> {
    try {
      await this.consumer.connect();
      this.isConnected = true;
      logger.info('Basic consumer connected successfully');
    } catch (error) {
      logger.error('Failed to connect basic consumer', { error });
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
      logger.info('Basic consumer disconnected');
    } catch (error) {
      logger.error('Failed to disconnect basic consumer', { error });
      throw error;
    }
  }

  /**
   * Subscribe to a topic and start consuming
   */
  async subscribe(topic: string): Promise<void> {
    if (!this.isConnected) {
      throw new Error('Consumer is not connected');
    }

    try {
      await this.consumer.subscribe({ topic });
      logger.info('Subscribed to topic', { topic });
    } catch (error) {
      logger.error('Failed to subscribe to topic', { error, topic });
      throw error;
    }
  }

  /**
   * Subscribe to multiple topics
   */
  async subscribeToTopics(topics: string[]): Promise<void> {
    if (!this.isConnected) {
      throw new Error('Consumer is not connected');
    }

    try {
      for (const topic of topics) {
        await this.consumer.subscribe({ topic });
      }
      logger.info('Subscribed to multiple topics', { topics });
    } catch (error) {
      logger.error('Failed to subscribe to topics', { error, topics });
      throw error;
    }
  }

  /**
   * Start consuming messages
   */
  async run(messageHandler?: (payload: EachMessagePayload) => Promise<void>): Promise<void> {
    if (!this.isConnected) {
      throw new Error('Consumer is not connected');
    }

    try {
      this.isRunning = true;
      
      await this.consumer.run({
        eachMessage: async (payload: EachMessagePayload) => {
          const { topic, partition, message } = payload;
          
          try {
            const value = message.value?.toString();
            const key = message.key?.toString();
            
            logger.info('Message received', {
              topic,
              partition,
              offset: message.offset,
              key,
              value: value ? JSON.parse(value) : null,
              timestamp: message.timestamp
            });

            // Call custom message handler if provided
            if (messageHandler) {
              await messageHandler(payload);
            } else {
              // Default handler - just log the message
              await this.defaultMessageHandler(payload);
            }

          } catch (error) {
            logger.error('Error processing message', {
              error,
              topic,
              partition,
              offset: message.offset
            });
            // Don't rethrow - continue processing other messages
          }
        }
      });
    } catch (error) {
      logger.error('Failed to start consumer', { error });
      this.isRunning = false;
      throw error;
    }
  }

  /**
   * Stop consuming messages
   */
  async stop(): Promise<void> {
    try {
      await this.consumer.stop();
      this.isRunning = false;
      logger.info('Consumer stopped');
    } catch (error) {
      logger.error('Failed to stop consumer', { error });
      throw error;
    }
  }

  /**
   * Default message handler
   */
  private async defaultMessageHandler(payload: EachMessagePayload): Promise<void> {
    const { message } = payload;
    const value = message.value?.toString();
    
    if (value) {
      try {
        const parsedMessage = JSON.parse(value);
        logger.info('Processed message with default handler', { 
          data: parsedMessage,
          processingTime: new Date().toISOString()
        });
      } catch (parseError) {
        logger.warn('Message is not valid JSON', { value });
      }
    }
  }

  /**
   * Consume demo messages
   */
  async consumeDemoMessages(): Promise<void> {
    await this.subscribe(getTopicName('DEMO'));
    
    await this.run(async (payload) => {
      const { message } = payload;
      const value = message.value?.toString();
      
      if (value) {
        const demoMessage = JSON.parse(value);
        logger.info('Demo message processed', {
          messageId: demoMessage.id,
          content: demoMessage.message,
          source: demoMessage.source,
          receivedAt: new Date().toISOString()
        });
      }
    });
  }

  /**
   * Consume user events
   */
  async consumeUserEvents(): Promise<void> {
    await this.subscribe(getTopicName('USER_EVENTS'));
    
    await this.run(async (payload) => {
      const { message } = payload;
      const value = message.value?.toString();
      
      if (value) {
        const userEvent = JSON.parse(value);
        logger.info('User event processed', {
          userId: userEvent.userId,
          event: userEvent.event,
          timestamp: userEvent.timestamp
        });
        
        // Simulate user event processing
        await this.processUserEvent(userEvent);
      }
    });
  }

  /**
   * Process user event (simulate business logic)
   */
  private async processUserEvent(userEvent: any): Promise<void> {
    // Simulate processing time
    await new Promise(resolve => setTimeout(resolve, 100));
    
    logger.info('User event processing completed', {
      userId: userEvent.userId,
      processed: true
    });
  }

  /**
   * Check if consumer is running
   */
  isConsumerRunning(): boolean {
    return this.isRunning;
  }

  /**
   * Check if consumer is connected
   */
  isConsumerConnected(): boolean {
    return this.isConnected;
  }
}

export default BasicConsumer;