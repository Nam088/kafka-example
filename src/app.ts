import { config } from 'dotenv';
import logger from './utils/logger';

// Import all modules
import BasicProducer from './modules/producer/basic.producer';
import PartitionProducer from './modules/producer/partition.producer';
import BatchProducer from './modules/producer/batch.producer';
import TransactionProducer from './modules/producer/transaction.producer';

import BasicConsumer from './modules/consumer/basic.consumer';
import GroupConsumer from './modules/consumer/group.consumer';
import OffsetConsumer from './modules/consumer/offset.consumer';
import StreamingConsumer from './modules/consumer/streaming.consumer';

import TopicAdmin from './modules/admin/topic.admin';
import DescribeAdmin from './modules/admin/describe.admin';
import ACLAdmin from './modules/admin/acl.admin';

import MetricsCollector from './modules/monitoring/metrics';

// Load environment variables
config();

/**
 * Kafka Demo Application
 * Registers and demonstrates all Kafka functionality
 */
export class KafkaDemoApp {
  private metricsCollector: MetricsCollector;
  private activeComponents: Map<string, any> = new Map();

  constructor() {
    this.metricsCollector = new MetricsCollector(
      parseInt(process.env.METRICS_INTERVAL || '10000')
    );
  }

  /**
   * Initialize the demo application
   */
  async initialize(): Promise<void> {
    try {
      logger.info('Initializing Kafka Demo Application');

      // Start metrics collection
      await this.metricsCollector.startCollection();
      logger.info('Metrics collection started');

      // Initialize topics
      await this.initializeTopics();

      logger.info('Kafka Demo Application initialized successfully');
    } catch (error) {
      logger.error('Failed to initialize Kafka Demo Application', { error });
      throw error;
    }
  }

  /**
   * Initialize all required topics
   */
  private async initializeTopics(): Promise<void> {
    try {
      const topicAdmin = new TopicAdmin();
      await topicAdmin.connect();
      
      await topicAdmin.initializeApplicationTopics();
      
      await topicAdmin.disconnect();
      logger.info('Topics initialization completed');
    } catch (error) {
      logger.error('Failed to initialize topics', { error });
      throw error;
    }
  }

  /**
   * Producer Demos
   */
  async runProducerDemos(): Promise<void> {
    logger.info('Starting Producer Demos');

    try {
      // Basic Producer Demo
      await this.runBasicProducerDemo();
      
      // Partition Producer Demo
      await this.runPartitionProducerDemo();
      
      // Batch Producer Demo
      await this.runBatchProducerDemo();
      
      // Transaction Producer Demo
      await this.runTransactionProducerDemo();

      logger.info('All Producer demos completed');
    } catch (error) {
      logger.error('Producer demos failed', { error });
      throw error;
    }
  }

  /**
   * Basic Producer Demo
   */
  private async runBasicProducerDemo(): Promise<void> {
    logger.info('Running Basic Producer Demo');
    
    const producer = new BasicProducer();
    this.activeComponents.set('basic-producer', producer);
    
    try {
      await producer.connect();
      
      // Send some demo messages
      for (let i = 0; i < 5; i++) {
        await producer.sendDemoMessage(
          `Basic demo message ${i + 1}`,
          `key-${i}`
        );
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
      
      await producer.disconnect();
      this.activeComponents.delete('basic-producer');
      
      logger.info('Basic Producer Demo completed');
    } catch (error) {
      logger.error('Basic Producer Demo failed', { error });
      throw error;
    }
  }

  /**
   * Partition Producer Demo
   */
  private async runPartitionProducerDemo(): Promise<void> {
    logger.info('Running Partition Producer Demo');
    
    const producer = new PartitionProducer();
    this.activeComponents.set('partition-producer', producer);
    
    try {
      await producer.connect();
      
      // Send user events with key-based partitioning
      const userIds = ['user1', 'user2', 'user3'];
      for (const userId of userIds) {
        await producer.sendUserEvent(userId, {
          action: 'login',
          timestamp: new Date().toISOString()
        });
      }
      
      // Send order events with region-based partitioning
      const regions = ['US', 'EU', 'ASIA'];
      for (let i = 0; i < regions.length; i++) {
        await producer.sendOrderEvent(
          `order-${i + 1}`,
          regions[i],
          { amount: 100 * (i + 1), items: i + 1 }
        );
      }
      
      await producer.disconnect();
      this.activeComponents.delete('partition-producer');
      
      logger.info('Partition Producer Demo completed');
    } catch (error) {
      logger.error('Partition Producer Demo failed', { error });
      throw error;
    }
  }

  /**
   * Batch Producer Demo
   */
  private async runBatchProducerDemo(): Promise<void> {
    logger.info('Running Batch Producer Demo');
    
    const producer = new BatchProducer(10, 5000); // Batch size 10, flush every 5 seconds
    this.activeComponents.set('batch-producer', producer);
    
    try {
      await producer.connect();
      
      // Send high throughput demo
      await producer.sendHighThroughputDemo(50);
      
      await producer.disconnect();
      this.activeComponents.delete('batch-producer');
      
      logger.info('Batch Producer Demo completed');
    } catch (error) {
      logger.error('Batch Producer Demo failed', { error });
      throw error;
    }
  }

  /**
   * Transaction Producer Demo
   */
  private async runTransactionProducerDemo(): Promise<void> {
    logger.info('Running Transaction Producer Demo');
    
    const producer = new TransactionProducer();
    this.activeComponents.set('transaction-producer', producer);
    
    try {
      await producer.connect();
      
      // Demo money transfer
      await producer.transferMoney(
        'account-1',
        'account-2',
        100,
        `transfer-${Date.now()}`
      );
      
      // Demo order processing
      await producer.processOrder(
        `order-${Date.now()}`,
        'customer-1',
        [
          { productId: 'product-1', quantity: 2, price: 50 },
          { productId: 'product-2', quantity: 1, price: 30 }
        ]
      );
      
      // Demo failed transaction
      await producer.simulateFailedTransaction();
      
      await producer.disconnect();
      this.activeComponents.delete('transaction-producer');
      
      logger.info('Transaction Producer Demo completed');
    } catch (error) {
      logger.error('Transaction Producer Demo failed', { error });
      throw error;
    }
  }

  /**
   * Consumer Demos
   */
  async runConsumerDemos(durationSeconds: number = 30): Promise<void> {
    logger.info('Starting Consumer Demos', { durationSeconds });

    try {
      // Run consumers for specified duration
      const consumerPromises = [
        this.runBasicConsumerDemo(durationSeconds),
        this.runGroupConsumerDemo(durationSeconds),
        this.runOffsetConsumerDemo(durationSeconds),
        this.runStreamingConsumerDemo(durationSeconds)
      ];

      await Promise.allSettled(consumerPromises);
      
      logger.info('All Consumer demos completed');
    } catch (error) {
      logger.error('Consumer demos failed', { error });
      throw error;
    }
  }

  /**
   * Basic Consumer Demo
   */
  private async runBasicConsumerDemo(durationSeconds: number): Promise<void> {
    logger.info('Running Basic Consumer Demo');
    
    const consumer = new BasicConsumer('basic-consumer-group');
    this.activeComponents.set('basic-consumer', consumer);
    
    try {
      await consumer.connect();
      
      // Start consuming (this will run until stopped)
      const consumePromise = consumer.consumeDemoMessages();
      
      // Stop after duration
      setTimeout(async () => {
        await consumer.stop();
        this.activeComponents.delete('basic-consumer');
        logger.info('Basic Consumer Demo completed');
      }, durationSeconds * 1000);
      
      await consumePromise;
    } catch (error) {
      logger.error('Basic Consumer Demo failed', { error });
      this.activeComponents.delete('basic-consumer');
    }
  }

  /**
   * Group Consumer Demo
   */
  private async runGroupConsumerDemo(durationSeconds: number): Promise<void> {
    logger.info('Running Group Consumer Demo');
    
    const consumers = await GroupConsumer.createConsumerGroup(
      'demo-group',
      2, // 2 consumers in the group
      ['demo-topic', 'user-events']
    );
    
    try {
      // Start all consumers in the group
      const consumePromises = consumers.map(async (consumer, index) => {
        this.activeComponents.set(`group-consumer-${index}`, consumer);
        return consumer.demonstrateLoadBalancing();
      });
      
      // Stop after duration
      setTimeout(async () => {
        for (let i = 0; i < consumers.length; i++) {
          try {
            await consumers[i].stop();
            await consumers[i].disconnect();
            this.activeComponents.delete(`group-consumer-${i}`);
          } catch (error) {
            logger.error(`Failed to stop group consumer ${i}`, { error });
          }
        }
        logger.info('Group Consumer Demo completed');
      }, durationSeconds * 1000);
      
      await Promise.allSettled(consumePromises);
    } catch (error) {
      logger.error('Group Consumer Demo failed', { error });
      // Clean up
      for (let i = 0; i < consumers.length; i++) {
        this.activeComponents.delete(`group-consumer-${i}`);
      }
    }
  }

  /**
   * Offset Consumer Demo
   */
  private async runOffsetConsumerDemo(durationSeconds: number): Promise<void> {
    logger.info('Running Offset Consumer Demo');
    
    const consumer = new OffsetConsumer('offset-demo-group');
    this.activeComponents.set('offset-consumer', consumer);
    
    try {
      await consumer.connect();
      
      // Start reliable processing demo
      const consumePromise = consumer.demonstrateReliableProcessing();
      
      // Stop after duration
      setTimeout(async () => {
        await consumer.stop();
        await consumer.disconnect();
        this.activeComponents.delete('offset-consumer');
        logger.info('Offset Consumer Demo completed');
      }, durationSeconds * 1000);
      
      await consumePromise;
    } catch (error) {
      logger.error('Offset Consumer Demo failed', { error });
      this.activeComponents.delete('offset-consumer');
    }
  }

  /**
   * Streaming Consumer Demo
   */
  private async runStreamingConsumerDemo(durationSeconds: number): Promise<void> {
    logger.info('Running Streaming Consumer Demo');
    
    const consumer = new StreamingConsumer('streaming-demo-group', 50, 5000);
    this.activeComponents.set('streaming-consumer', consumer);
    
    try {
      await consumer.connect();
      
      // Start stream processing demo
      const streamPromise = consumer.demonstrateStreamProcessing();
      
      // Stop after duration
      setTimeout(async () => {
        await consumer.stop();
        await consumer.disconnect();
        this.activeComponents.delete('streaming-consumer');
        logger.info('Streaming Consumer Demo completed');
      }, durationSeconds * 1000);
      
      await streamPromise;
    } catch (error) {
      logger.error('Streaming Consumer Demo failed', { error });
      this.activeComponents.delete('streaming-consumer');
    }
  }

  /**
   * Admin Demos
   */
  async runAdminDemos(): Promise<void> {
    logger.info('Starting Admin Demos');

    try {
      await this.runTopicAdminDemo();
      await this.runDescribeAdminDemo();
      await this.runACLAdminDemo();
      
      logger.info('All Admin demos completed');
    } catch (error) {
      logger.error('Admin demos failed', { error });
      throw error;
    }
  }

  /**
   * Topic Admin Demo
   */
  private async runTopicAdminDemo(): Promise<void> {
    logger.info('Running Topic Admin Demo');
    
    const admin = new TopicAdmin();
    
    try {
      await admin.connect();
      
      // Get topic statistics
      const stats = await admin.getTopicStats();
      logger.info('Topic statistics', { stats });
      
      // List all topics
      const topics = await admin.listTopics();
      logger.info('All topics listed', { topicCount: topics.length });
      
      await admin.disconnect();
      logger.info('Topic Admin Demo completed');
    } catch (error) {
      logger.error('Topic Admin Demo failed', { error });
      throw error;
    }
  }

  /**
   * Describe Admin Demo
   */
  private async runDescribeAdminDemo(): Promise<void> {
    logger.info('Running Describe Admin Demo');
    
    const admin = new DescribeAdmin();
    
    try {
      await admin.connect();
      
      // Get cluster overview
      const overview = await admin.getClusterOverview();
      logger.info('Cluster overview', { overview });
      
      // Get health status
      const health = await admin.getHealthStatus();
      logger.info('Health status', { health });
      
      await admin.disconnect();
      logger.info('Describe Admin Demo completed');
    } catch (error) {
      logger.error('Describe Admin Demo failed', { error });
      throw error;
    }
  }

  /**
   * ACL Admin Demo
   */
  private async runACLAdminDemo(): Promise<void> {
    logger.info('Running ACL Admin Demo');
    
    const admin = new ACLAdmin();
    
    try {
      await admin.connect();
      
      // Check security status
      const securityStatus = await admin.getSecurityStatus();
      logger.info('Security status', { securityStatus });
      
      // List ACLs (this will show mock data if authorization is not enabled)
      const acls = await admin.listAcls();
      logger.info('ACL rules', { aclCount: acls.length });
      
      await admin.disconnect();
      logger.info('ACL Admin Demo completed');
    } catch (error) {
      logger.error('ACL Admin Demo failed', { error });
      throw error;
    }
  }

  /**
   * Get current metrics
   */
  getMetrics(): any {
    return this.metricsCollector.getMetricsSummary();
  }

  /**
   * Export metrics
   */
  exportMetrics(format: 'json' | 'prometheus' = 'json'): string {
    return this.metricsCollector.exportMetrics(format);
  }

  /**
   * Shutdown the application
   */
  async shutdown(): Promise<void> {
    logger.info('Shutting down Kafka Demo Application');
    
    try {
      // Stop metrics collection
      this.metricsCollector.stopCollection();
      
      // Stop all active components
      for (const [name, component] of this.activeComponents) {
        try {
          if (component.stop) {
            await component.stop();
          }
          if (component.disconnect) {
            await component.disconnect();
          }
          logger.info('Component shutdown', { component: name });
        } catch (error) {
          logger.error('Failed to shutdown component', { component: name, error });
        }
      }
      
      this.activeComponents.clear();
      
      logger.info('Kafka Demo Application shutdown completed');
    } catch (error) {
      logger.error('Error during shutdown', { error });
      throw error;
    }
  }

  /**
   * Get active components
   */
  getActiveComponents(): string[] {
    return Array.from(this.activeComponents.keys());
  }
}

export default KafkaDemoApp;