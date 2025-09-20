import { Consumer, EachMessagePayload } from 'kafkajs';
import KafkaConnection from '../../config/kafka.config';
import { getTopicName } from '../../core/topics';
import logger from '../../utils/logger';

/**
 * Streaming Consumer
 * Demonstrates stream processing patterns and real-time data processing
 */
export class StreamingConsumer {
  private consumer: Consumer;
  private isConnected: boolean = false;
  private isRunning: boolean = false;
  private groupId: string;
  
  // Stream processing state
  private messageBuffer: Array<any> = [];
  private windowSize: number;
  private windowDurationMs: number;
  private lastWindowFlush: number = Date.now();
  
  // Metrics tracking
  private processingMetrics = {
    totalMessages: 0,
    errorCount: 0,
    avgProcessingTime: 0,
    throughput: 0
  };

  constructor(groupId?: string, windowSize: number = 100, windowDurationMs: number = 10000) {
    this.groupId = groupId || `streaming-consumer-${Date.now()}`;
    this.consumer = KafkaConnection.createConsumer(this.groupId);
    this.windowSize = windowSize;
    this.windowDurationMs = windowDurationMs;
  }

  /**
   * Connect to Kafka
   */
  async connect(): Promise<void> {
    try {
      await this.consumer.connect();
      this.isConnected = true;
      logger.info('Streaming consumer connected successfully', { 
        groupId: this.groupId,
        windowSize: this.windowSize,
        windowDurationMs: this.windowDurationMs
      });
    } catch (error) {
      logger.error('Failed to connect streaming consumer', { error, groupId: this.groupId });
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
      
      // Flush any remaining messages
      await this.flushWindow();
      
      await this.consumer.disconnect();
      this.isConnected = false;
      logger.info('Streaming consumer disconnected', { groupId: this.groupId });
    } catch (error) {
      logger.error('Failed to disconnect streaming consumer', { error, groupId: this.groupId });
      throw error;
    }
  }

  /**
   * Subscribe to topics for streaming
   */
  async subscribeForStreaming(topics: string[]): Promise<void> {
    if (!this.isConnected) {
      throw new Error('Consumer is not connected');
    }

    try {
      for (const topic of topics) {
        await this.consumer.subscribe({ topic, fromBeginning: false });
      }
      logger.info('Subscribed for streaming', { 
        topics, 
        groupId: this.groupId 
      });
    } catch (error) {
      logger.error('Failed to subscribe for streaming', { error, topics, groupId: this.groupId });
      throw error;
    }
  }

  /**
   * Start streaming processing
   */
  async startStreaming(): Promise<void> {
    if (!this.isConnected) {
      throw new Error('Consumer is not connected');
    }

    try {
      this.isRunning = true;
      
      // Set up window flushing timer
      const flushTimer = setInterval(async () => {
        if (this.shouldFlushWindow()) {
          await this.flushWindow();
        }
      }, 1000); // Check every second

      await this.consumer.run({
        eachMessage: async (payload: EachMessagePayload) => {
          await this.handleStreamMessage(payload);
        },
        
        // Use batch processing for better throughput
        eachBatch: async ({ batch, resolveOffset, heartbeat, isRunning, isStale }) => {
          for (const message of batch.messages) {
            if (!isRunning() || isStale()) break;

            await this.handleStreamMessage({
              topic: batch.topic,
              partition: batch.partition,
              message
            });

            resolveOffset(message.offset);
            await heartbeat();
          }
        }
      });

      clearInterval(flushTimer);
    } catch (error) {
      logger.error('Failed to start streaming', { error, groupId: this.groupId });
      this.isRunning = false;
      throw error;
    }
  }

  /**
   * Handle stream message
   */
  private async handleStreamMessage(payload: EachMessagePayload): Promise<void> {
    const startTime = Date.now();
    
    try {
      const { topic, partition, message } = payload;
      const value = message.value?.toString();
      
      if (!value) return;

      const parsedMessage = JSON.parse(value);
      
      // Add to stream buffer
      this.messageBuffer.push({
        ...parsedMessage,
        metadata: {
          topic,
          partition,
          offset: message.offset,
          timestamp: message.timestamp,
          receivedAt: Date.now()
        }
      });

      // Update metrics
      this.processingMetrics.totalMessages++;
      
      // Flush window if size threshold reached
      if (this.messageBuffer.length >= this.windowSize) {
        await this.flushWindow();
      }

      // Calculate processing time
      const processingTime = Date.now() - startTime;
      this.updateProcessingMetrics(processingTime);

    } catch (error) {
      logger.error('Error handling stream message', { 
        error, 
        groupId: this.groupId 
      });
      this.processingMetrics.errorCount++;
    }
  }

  /**
   * Process windowed data
   */
  private async flushWindow(): Promise<void> {
    if (this.messageBuffer.length === 0) {
      return;
    }

    try {
      const windowData = [...this.messageBuffer];
      this.messageBuffer = [];
      this.lastWindowFlush = Date.now();

      logger.info('Processing stream window', {
        messageCount: windowData.length,
        groupId: this.groupId,
        windowDuration: this.windowDurationMs
      });

      // Process different stream patterns
      await Promise.all([
        this.processAggregations(windowData),
        this.processPatternDetection(windowData),
        this.processRealTimeAlerts(windowData)
      ]);

      logger.info('Stream window processed successfully', {
        messageCount: windowData.length,
        groupId: this.groupId
      });

    } catch (error) {
      logger.error('Error processing stream window', { error, groupId: this.groupId });
      throw error;
    }
  }

  /**
   * Process aggregations (count, sum, average, etc.)
   */
  private async processAggregations(messages: any[]): Promise<void> {
    const aggregations = {
      totalCount: messages.length,
      byTopic: {} as Record<string, number>,
      bySource: {} as Record<string, number>,
      avgProcessingDelay: 0
    };

    let totalDelay = 0;

    for (const message of messages) {
      // Count by topic
      const topic = message.metadata.topic;
      aggregations.byTopic[topic] = (aggregations.byTopic[topic] || 0) + 1;

      // Count by source
      const source = message.source || 'unknown';
      aggregations.bySource[source] = (aggregations.bySource[source] || 0) + 1;

      // Calculate processing delay
      if (message.timestamp) {
        const delay = message.metadata.receivedAt - new Date(message.timestamp).getTime();
        totalDelay += delay;
      }
    }

    aggregations.avgProcessingDelay = totalDelay / messages.length;

    logger.info('Stream aggregations computed', {
      aggregations,
      groupId: this.groupId
    });

    // Store or forward aggregations
    await this.storeAggregations(aggregations);
  }

  /**
   * Detect patterns in the stream
   */
  private async processPatternDetection(messages: any[]): Promise<void> {
    const patterns = {
      errorSpikes: this.detectErrorSpikes(messages),
      userActivityPatterns: this.detectUserActivityPatterns(messages),
      anomalies: this.detectAnomalies(messages)
    };

    if (patterns.errorSpikes.length > 0 || patterns.anomalies.length > 0) {
      logger.warn('Stream patterns detected', {
        patterns,
        groupId: this.groupId
      });
    }

    logger.debug('Pattern detection completed', {
      patternCount: Object.values(patterns).flat().length,
      groupId: this.groupId
    });
  }

  /**
   * Process real-time alerts
   */
  private async processRealTimeAlerts(messages: any[]): Promise<void> {
    const alerts = [];

    for (const message of messages) {
      // Check for critical events
      if (message.level === 'error' || message.severity === 'critical') {
        alerts.push({
          type: 'CRITICAL_EVENT',
          message: message,
          timestamp: Date.now()
        });
      }

      // Check for threshold violations
      if (message.value && typeof message.value === 'number' && message.value > 1000) {
        alerts.push({
          type: 'THRESHOLD_VIOLATION',
          message: message,
          threshold: 1000,
          timestamp: Date.now()
        });
      }
    }

    if (alerts.length > 0) {
      logger.warn('Real-time alerts generated', {
        alertCount: alerts.length,
        alerts,
        groupId: this.groupId
      });

      // Send alerts to monitoring system
      await this.sendAlerts(alerts);
    }
  }

  /**
   * Detect error spikes
   */
  private detectErrorSpikes(messages: any[]): any[] {
    const errorMessages = messages.filter(msg => 
      msg.level === 'error' || msg.type === 'error'
    );

    if (errorMessages.length > messages.length * 0.1) { // More than 10% errors
      return [{
        type: 'ERROR_SPIKE',
        errorCount: errorMessages.length,
        totalCount: messages.length,
        percentage: (errorMessages.length / messages.length * 100).toFixed(2)
      }];
    }

    return [];
  }

  /**
   * Detect user activity patterns
   */
  private detectUserActivityPatterns(messages: any[]): any[] {
    const userActivity = {} as Record<string, number>;
    
    messages.forEach(msg => {
      if (msg.userId) {
        userActivity[msg.userId] = (userActivity[msg.userId] || 0) + 1;
      }
    });

    // Detect unusually active users
    const avgActivity = Object.values(userActivity).reduce((sum, count) => sum + count, 0) / Object.keys(userActivity).length;
    const highActivityUsers = Object.entries(userActivity)
      .filter(([_, count]) => count > avgActivity * 3)
      .map(([userId, count]) => ({ userId, activityCount: count }));

    return highActivityUsers.map(user => ({
      type: 'HIGH_USER_ACTIVITY',
      ...user
    }));
  }

  /**
   * Detect anomalies
   */
  private detectAnomalies(messages: any[]): any[] {
    const anomalies = [];

    // Detect unusual message sizes
    const messageSizes = messages.map(msg => JSON.stringify(msg).length);
    const avgSize = messageSizes.reduce((sum, size) => sum + size, 0) / messageSizes.length;
    const threshold = avgSize * 2;

    messages.forEach((msg, index) => {
      if (messageSizes[index] > threshold) {
        anomalies.push({
          type: 'LARGE_MESSAGE_ANOMALY',
          messageSize: messageSizes[index],
          threshold,
          message: msg
        });
      }
    });

    return anomalies;
  }

  /**
   * Store aggregations
   */
  private async storeAggregations(aggregations: any): Promise<void> {
    // Simulate storing to database or cache
    logger.debug('Storing aggregations', { aggregations });
  }

  /**
   * Send alerts
   */
  private async sendAlerts(alerts: any[]): Promise<void> {
    // Simulate sending alerts to monitoring system
    logger.info('Sending alerts to monitoring system', { 
      alertCount: alerts.length 
    });
  }

  /**
   * Check if window should be flushed
   */
  private shouldFlushWindow(): boolean {
    const timeSinceLastFlush = Date.now() - this.lastWindowFlush;
    return timeSinceLastFlush >= this.windowDurationMs;
  }

  /**
   * Update processing metrics
   */
  private updateProcessingMetrics(processingTime: number): void {
    const { totalMessages, avgProcessingTime } = this.processingMetrics;
    
    this.processingMetrics.avgProcessingTime = 
      (avgProcessingTime * (totalMessages - 1) + processingTime) / totalMessages;
    
    // Calculate throughput (messages per second)
    const now = Date.now();
    this.processingMetrics.throughput = totalMessages / ((now - this.lastWindowFlush) / 1000);
  }

  /**
   * Get processing metrics
   */
  getMetrics(): any {
    return {
      ...this.processingMetrics,
      bufferSize: this.messageBuffer.length,
      groupId: this.groupId
    };
  }

  /**
   * Stop streaming
   */
  async stop(): Promise<void> {
    try {
      await this.flushWindow();
      await this.consumer.stop();
      this.isRunning = false;
      logger.info('Streaming consumer stopped', { groupId: this.groupId });
    } catch (error) {
      logger.error('Failed to stop streaming consumer', { error, groupId: this.groupId });
      throw error;
    }
  }

  /**
   * Demonstrate real-time stream processing
   */
  async demonstrateStreamProcessing(): Promise<void> {
    const topics = [getTopicName('USER_EVENTS'), getTopicName('ORDER_EVENTS')];
    await this.subscribeForStreaming(topics);
    
    logger.info('Starting stream processing demo', { 
      topics,
      groupId: this.groupId 
    });
    
    await this.startStreaming();
  }

  /**
   * Check if running
   */
  isConsumerRunning(): boolean {
    return this.isRunning;
  }
}

export default StreamingConsumer;