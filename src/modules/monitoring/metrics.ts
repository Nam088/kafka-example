import KafkaConnection from '../../config/kafka.config';
import logger from '../../utils/logger';

/**
 * Metrics Collector
 * Monitors Kafka performance and health metrics
 */
export class MetricsCollector {
  private isCollecting: boolean = false;
  private metricsInterval: NodeJS.Timeout | null = null;
  private intervalMs: number;
  
  // Metrics storage
  private metrics = {
    producers: new Map<string, ProducerMetrics>(),
    consumers: new Map<string, ConsumerMetrics>(),
    topics: new Map<string, TopicMetrics>(),
    cluster: null as ClusterMetrics | null,
    system: null as SystemMetrics | null
  };

  constructor(intervalMs: number = 10000) {
    this.intervalMs = intervalMs;
  }

  /**
   * Start collecting metrics
   */
  async startCollection(): Promise<void> {
    if (this.isCollecting) {
      logger.warn('Metrics collection is already running');
      return;
    }

    try {
      this.isCollecting = true;
      
      logger.info('Starting metrics collection', { 
        intervalMs: this.intervalMs 
      });

      // Initial collection
      await this.collectAllMetrics();

      // Set up periodic collection
      this.metricsInterval = setInterval(async () => {
        try {
          await this.collectAllMetrics();
        } catch (error) {
          logger.error('Error during periodic metrics collection', { error });
        }
      }, this.intervalMs);

    } catch (error) {
      logger.error('Failed to start metrics collection', { error });
      this.isCollecting = false;
      throw error;
    }
  }

  /**
   * Stop collecting metrics
   */
  stopCollection(): void {
    if (this.metricsInterval) {
      clearInterval(this.metricsInterval);
      this.metricsInterval = null;
    }
    
    this.isCollecting = false;
    
    logger.info('Metrics collection stopped');
  }

  /**
   * Collect all types of metrics
   */
  private async collectAllMetrics(): Promise<void> {
    const startTime = Date.now();

    try {
      await Promise.all([
        this.collectClusterMetrics(),
        this.collectTopicMetrics(),
        this.collectSystemMetrics()
      ]);

      const collectionTime = Date.now() - startTime;
      
      logger.debug('Metrics collection completed', {
        collectionTimeMs: collectionTime,
        timestamp: new Date().toISOString()
      });

    } catch (error) {
      logger.error('Error collecting metrics', { error });
    }
  }

  /**
   * Collect cluster-level metrics
   */
  private async collectClusterMetrics(): Promise<void> {
    try {
      const admin = KafkaConnection.createAdmin();
      await admin.connect();

      const cluster = await admin.describeCluster();
      const topics = await admin.listTopics();

      this.metrics.cluster = {
        timestamp: Date.now(),
        brokerCount: cluster.brokers.length,
        controllerId: cluster.controller,
        topicCount: topics.length,
        brokers: cluster.brokers.map(broker => ({
          nodeId: broker.nodeId,
          host: broker.host,
          port: broker.port,
          rack: (broker as any).rack
        }))
      };

      await admin.disconnect();
      
    } catch (error) {
      logger.error('Failed to collect cluster metrics', { error });
    }
  }

  /**
   * Collect topic-level metrics
   */
  private async collectTopicMetrics(): Promise<void> {
    try {
      const admin = KafkaConnection.createAdmin();
      await admin.connect();

      const topics = await admin.listTopics();
      
      // Sample a few topics for detailed metrics
      const sampleTopics = topics.slice(0, 10);
      
      for (const topicName of sampleTopics) {
        try {
          const offsets = await admin.fetchTopicOffsets(topicName);
          const metadata = await admin.fetchTopicMetadata({ topics: [topicName] });
          
          const topic = metadata.topics[0];
          const totalMessages = offsets.reduce((sum: number, partition: any) => {
            return sum + (parseInt(partition.high) - parseInt(partition.low));
          }, 0);

          this.metrics.topics.set(topicName, {
            timestamp: Date.now(),
            name: topicName,
            partitionCount: topic?.partitions.length || 0,
            totalMessages,
            partitions: offsets.map((offset: any) => ({
              partition: offset.partition,
              earliestOffset: parseInt(offset.low),
              latestOffset: parseInt(offset.high),
              messageCount: parseInt(offset.high) - parseInt(offset.low)
            }))
          });
          
        } catch (error) {
          logger.debug('Failed to collect metrics for topic', { 
            topic: topicName, 
            error: error instanceof Error ? error.message : String(error) 
          });
        }
      }

      await admin.disconnect();
      
    } catch (error) {
      logger.error('Failed to collect topic metrics', { error });
    }
  }

  /**
   * Collect system metrics
   */
  private async collectSystemMetrics(): Promise<void> {
    try {
      const memoryUsage = process.memoryUsage();
      const cpuUsage = process.cpuUsage();

      this.metrics.system = {
        timestamp: Date.now(),
        memory: {
          rss: memoryUsage.rss,
          heapTotal: memoryUsage.heapTotal,
          heapUsed: memoryUsage.heapUsed,
          external: memoryUsage.external,
          arrayBuffers: memoryUsage.arrayBuffers
        },
        cpu: {
          user: cpuUsage.user,
          system: cpuUsage.system
        },
        uptime: process.uptime(),
        pid: process.pid
      };
      
    } catch (error) {
      logger.error('Failed to collect system metrics', { error });
    }
  }

  /**
   * Record producer metrics
   */
  recordProducerMetrics(
    producerId: string,
    topic: string,
    messagesProduced: number,
    bytesProduced: number,
    errors: number = 0
  ): void {
    const existing = this.metrics.producers.get(producerId) || {
      producerId,
      totalMessages: 0,
      totalBytes: 0,
      totalErrors: 0,
      topicStats: new Map(),
      lastUpdated: Date.now()
    };

    existing.totalMessages += messagesProduced;
    existing.totalBytes += bytesProduced;
    existing.totalErrors += errors;
    existing.lastUpdated = Date.now();

    // Update topic-specific stats
    const topicStats = existing.topicStats.get(topic) || {
      messages: 0,
      bytes: 0,
      errors: 0
    };
    
    topicStats.messages += messagesProduced;
    topicStats.bytes += bytesProduced;
    topicStats.errors += errors;
    
    existing.topicStats.set(topic, topicStats);
    this.metrics.producers.set(producerId, existing);
  }

  /**
   * Record consumer metrics
   */
  recordConsumerMetrics(
    consumerId: string,
    groupId: string,
    topic: string,
    messagesConsumed: number,
    bytesConsumed: number,
    processingTimeMs: number,
    errors: number = 0
  ): void {
    const existing = this.metrics.consumers.get(consumerId) || {
      consumerId,
      groupId,
      totalMessages: 0,
      totalBytes: 0,
      totalErrors: 0,
      avgProcessingTimeMs: 0,
      topicStats: new Map(),
      lastUpdated: Date.now()
    };

    existing.totalMessages += messagesConsumed;
    existing.totalBytes += bytesConsumed;
    existing.totalErrors += errors;
    
    // Update average processing time
    existing.avgProcessingTimeMs = 
      (existing.avgProcessingTimeMs * (existing.totalMessages - messagesConsumed) + 
       processingTimeMs * messagesConsumed) / existing.totalMessages;
    
    existing.lastUpdated = Date.now();

    // Update topic-specific stats
    const topicStats = existing.topicStats.get(topic) || {
      messages: 0,
      bytes: 0,
      errors: 0,
      avgProcessingTimeMs: 0
    };
    
    topicStats.messages += messagesConsumed;
    topicStats.bytes += bytesConsumed;
    topicStats.errors += errors;
    topicStats.avgProcessingTimeMs = 
      (topicStats.avgProcessingTimeMs * (topicStats.messages - messagesConsumed) + 
       processingTimeMs * messagesConsumed) / topicStats.messages;
    
    existing.topicStats.set(topic, topicStats);
    this.metrics.consumers.set(consumerId, existing);
  }

  /**
   * Get all current metrics
   */
  getAllMetrics(): any {
    return {
      cluster: this.metrics.cluster,
      system: this.metrics.system,
      topics: Array.from(this.metrics.topics.values()),
      producers: Array.from(this.metrics.producers.values()).map(p => ({
        ...p,
        topicStats: Array.from(p.topicStats.entries()).map(([topic, stats]) => ({
          topic,
          ...stats
        }))
      })),
      consumers: Array.from(this.metrics.consumers.values()).map(c => ({
        ...c,
        topicStats: Array.from(c.topicStats.entries()).map(([topic, stats]) => ({
          topic,
          ...stats
        }))
      })),
      lastUpdated: Date.now()
    };
  }

  /**
   * Get metrics summary
   */
  getMetricsSummary(): any {
    const allMetrics = this.getAllMetrics();
    
    return {
      cluster: {
        brokerCount: allMetrics.cluster?.brokerCount || 0,
        topicCount: allMetrics.cluster?.topicCount || 0
      },
      producers: {
        count: allMetrics.producers.length,
        totalMessages: allMetrics.producers.reduce((sum: number, p: any) => sum + p.totalMessages, 0),
        totalErrors: allMetrics.producers.reduce((sum: number, p: any) => sum + p.totalErrors, 0)
      },
      consumers: {
        count: allMetrics.consumers.length,
        totalMessages: allMetrics.consumers.reduce((sum: number, c: any) => sum + c.totalMessages, 0),
        totalErrors: allMetrics.consumers.reduce((sum: number, c: any) => sum + c.totalErrors, 0),
        avgProcessingTime: allMetrics.consumers.length > 0 
          ? allMetrics.consumers.reduce((sum: number, c: any) => sum + c.avgProcessingTimeMs, 0) / allMetrics.consumers.length
          : 0
      },
      topics: {
        count: allMetrics.topics.length,
        totalMessages: allMetrics.topics.reduce((sum: number, t: any) => sum + t.totalMessages, 0)
      },
      system: allMetrics.system,
      timestamp: Date.now()
    };
  }

  /**
   * Export metrics for external monitoring systems
   */
  exportMetrics(format: 'json' | 'prometheus' = 'json'): string {
    const metrics = this.getAllMetrics();
    
    if (format === 'json') {
      return JSON.stringify(metrics, null, 2);
    } else if (format === 'prometheus') {
      return this.formatPrometheusMetrics(metrics);
    }
    
    return JSON.stringify(metrics);
  }

  /**
   * Format metrics for Prometheus
   */
  private formatPrometheusMetrics(metrics: any): string {
    const lines = [];
    
    // Cluster metrics
    if (metrics.cluster) {
      lines.push(`kafka_cluster_brokers_total ${metrics.cluster.brokerCount}`);
      lines.push(`kafka_cluster_topics_total ${metrics.cluster.topicCount}`);
    }
    
    // Producer metrics
    for (const producer of metrics.producers) {
      lines.push(`kafka_producer_messages_total{producer_id="${producer.producerId}"} ${producer.totalMessages}`);
      lines.push(`kafka_producer_bytes_total{producer_id="${producer.producerId}"} ${producer.totalBytes}`);
      lines.push(`kafka_producer_errors_total{producer_id="${producer.producerId}"} ${producer.totalErrors}`);
    }
    
    // Consumer metrics
    for (const consumer of metrics.consumers) {
      lines.push(`kafka_consumer_messages_total{consumer_id="${consumer.consumerId}",group_id="${consumer.groupId}"} ${consumer.totalMessages}`);
      lines.push(`kafka_consumer_bytes_total{consumer_id="${consumer.consumerId}",group_id="${consumer.groupId}"} ${consumer.totalBytes}`);
      lines.push(`kafka_consumer_errors_total{consumer_id="${consumer.consumerId}",group_id="${consumer.groupId}"} ${consumer.totalErrors}`);
      lines.push(`kafka_consumer_processing_time_avg_ms{consumer_id="${consumer.consumerId}",group_id="${consumer.groupId}"} ${consumer.avgProcessingTimeMs}`);
    }
    
    // System metrics
    if (metrics.system) {
      lines.push(`nodejs_memory_heap_used_bytes ${metrics.system.memory.heapUsed}`);
      lines.push(`nodejs_memory_heap_total_bytes ${metrics.system.memory.heapTotal}`);
      lines.push(`nodejs_uptime_seconds ${metrics.system.uptime}`);
    }
    
    return lines.join('\n');
  }

  /**
   * Clear metrics data
   */
  clearMetrics(): void {
    this.metrics.producers.clear();
    this.metrics.consumers.clear();
    this.metrics.topics.clear();
    this.metrics.cluster = null;
    this.metrics.system = null;
    
    logger.info('Metrics data cleared');
  }

  /**
   * Check if collecting
   */
  isCollectionActive(): boolean {
    return this.isCollecting;
  }
}

// Type definitions
interface ProducerMetrics {
  producerId: string;
  totalMessages: number;
  totalBytes: number;
  totalErrors: number;
  topicStats: Map<string, {
    messages: number;
    bytes: number;
    errors: number;
  }>;
  lastUpdated: number;
}

interface ConsumerMetrics {
  consumerId: string;
  groupId: string;
  totalMessages: number;
  totalBytes: number;
  totalErrors: number;
  avgProcessingTimeMs: number;
  topicStats: Map<string, {
    messages: number;
    bytes: number;
    errors: number;
    avgProcessingTimeMs: number;
  }>;
  lastUpdated: number;
}

interface TopicMetrics {
  timestamp: number;
  name: string;
  partitionCount: number;
  totalMessages: number;
  partitions: Array<{
    partition: number;
    earliestOffset: number;
    latestOffset: number;
    messageCount: number;
  }>;
}

interface ClusterMetrics {
  timestamp: number;
  brokerCount: number;
  controllerId: number | null;
  topicCount: number;
  brokers: Array<{
    nodeId: number;
    host: string;
    port: number;
    rack?: string;
  }>;
}

interface SystemMetrics {
  timestamp: number;
  memory: {
    rss: number;
    heapTotal: number;
    heapUsed: number;
    external: number;
    arrayBuffers: number;
  };
  cpu: {
    user: number;
    system: number;
  };
  uptime: number;
  pid: number;
}

export default MetricsCollector;