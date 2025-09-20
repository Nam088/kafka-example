import { Admin, ITopicConfig } from 'kafkajs';
import KafkaConnection from '../../config/kafka.config';
import { getAllTopics, TopicConfig } from '../../core/topics';
import logger from '../../utils/logger';

/**
 * Topic Admin
 * Manages Kafka topics (create, delete, list, configure)
 */
export class TopicAdmin {
  private admin: Admin;
  private isConnected: boolean = false;

  constructor() {
    this.admin = KafkaConnection.createAdmin();
  }

  /**
   * Connect to Kafka
   */
  async connect(): Promise<void> {
    try {
      await this.admin.connect();
      this.isConnected = true;
      logger.info('Topic admin connected successfully');
    } catch (error) {
      logger.error('Failed to connect topic admin', { error });
      throw error;
    }
  }

  /**
   * Disconnect from Kafka
   */
  async disconnect(): Promise<void> {
    try {
      await this.admin.disconnect();
      this.isConnected = false;
      logger.info('Topic admin disconnected');
    } catch (error) {
      logger.error('Failed to disconnect topic admin', { error });
      throw error;
    }
  }

  /**
   * Create a single topic
   */
  async createTopic(topicConfig: TopicConfig): Promise<void> {
    if (!this.isConnected) {
      throw new Error('Admin is not connected');
    }

    try {
      const topicConfiguration: ITopicConfig = {
        topic: topicConfig.name,
        numPartitions: topicConfig.partitions || 1,
        replicationFactor: topicConfig.replicationFactor || 1,
        configEntries: topicConfig.configs ? Object.entries(topicConfig.configs).map(
          ([key, value]) => ({ name: key, value })
        ) : undefined
      };

      await this.admin.createTopics({
        topics: [topicConfiguration],
        waitForLeaders: true,
        timeout: 30000
      });

      logger.info('Topic created successfully', {
        topic: topicConfig.name,
        partitions: topicConfig.partitions,
        replicationFactor: topicConfig.replicationFactor
      });
    } catch (error) {
      logger.error('Failed to create topic', { error, topic: topicConfig.name });
      throw error;
    }
  }

  /**
   * Create multiple topics
   */
  async createTopics(topicConfigs: TopicConfig[]): Promise<void> {
    if (!this.isConnected) {
      throw new Error('Admin is not connected');
    }

    try {
      const topicConfigurations: ITopicConfig[] = topicConfigs.map(config => ({
        topic: config.name,
        numPartitions: config.partitions || 1,
        replicationFactor: config.replicationFactor || 1,
        configEntries: config.configs ? Object.entries(config.configs).map(
          ([key, value]) => ({ name: key, value })
        ) : undefined
      }));

      await this.admin.createTopics({
        topics: topicConfigurations,
        waitForLeaders: true,
        timeout: 30000
      });

      logger.info('Multiple topics created successfully', {
        topicCount: topicConfigs.length,
        topics: topicConfigs.map(c => c.name)
      });
    } catch (error) {
      logger.error('Failed to create topics', { 
        error, 
        topics: topicConfigs.map(c => c.name) 
      });
      throw error;
    }
  }

  /**
   * Delete a topic
   */
  async deleteTopic(topicName: string): Promise<void> {
    if (!this.isConnected) {
      throw new Error('Admin is not connected');
    }

    try {
      await this.admin.deleteTopics({
        topics: [topicName],
        timeout: 30000
      });

      logger.info('Topic deleted successfully', { topic: topicName });
    } catch (error) {
      logger.error('Failed to delete topic', { error, topic: topicName });
      throw error;
    }
  }

  /**
   * Delete multiple topics
   */
  async deleteTopics(topicNames: string[]): Promise<void> {
    if (!this.isConnected) {
      throw new Error('Admin is not connected');
    }

    try {
      await this.admin.deleteTopics({
        topics: topicNames,
        timeout: 30000
      });

      logger.info('Multiple topics deleted successfully', { 
        topicCount: topicNames.length,
        topics: topicNames 
      });
    } catch (error) {
      logger.error('Failed to delete topics', { error, topics: topicNames });
      throw error;
    }
  }

  /**
   * List all topics
   */
  async listTopics(): Promise<string[]> {
    if (!this.isConnected) {
      throw new Error('Admin is not connected');
    }

    try {
      const topics = await this.admin.listTopics();
      
      logger.info('Topics listed successfully', { 
        topicCount: topics.length,
        topics 
      });
      
      return topics;
    } catch (error) {
      logger.error('Failed to list topics', { error });
      throw error;
    }
  }

  /**
   * Get topic metadata
   */
  async getTopicMetadata(topicName: string): Promise<any> {
    if (!this.isConnected) {
      throw new Error('Admin is not connected');
    }

    try {
      const metadata = await this.admin.fetchTopicMetadata({
        topics: [topicName]
      });

      logger.info('Topic metadata fetched', { 
        topic: topicName,
        metadata 
      });
      
      return metadata.topics[0];
    } catch (error) {
      logger.error('Failed to get topic metadata', { error, topic: topicName });
      throw error;
    }
  }

  /**
   * Create partitions for a topic
   */
  async createPartitions(topicName: string, totalPartitions: number): Promise<void> {
    if (!this.isConnected) {
      throw new Error('Admin is not connected');
    }

    try {
      await this.admin.createPartitions({
        topicPartitions: [
          {
            topic: topicName,
            count: totalPartitions
          }
        ],
        timeout: 30000
      });

      logger.info('Partitions created successfully', { 
        topic: topicName,
        totalPartitions 
      });
    } catch (error) {
      logger.error('Failed to create partitions', { 
        error, 
        topic: topicName, 
        totalPartitions 
      });
      throw error;
    }
  }

  /**
   * Alter topic configuration
   */
  async alterTopicConfig(
    topicName: string, 
    configEntries: Record<string, string>
  ): Promise<void> {
    if (!this.isConnected) {
      throw new Error('Admin is not connected');
    }

    try {
      await this.admin.alterConfigs({
        resources: [
          {
            type: 2, // TOPIC
            name: topicName,
            configEntries: Object.entries(configEntries).map(
              ([name, value]) => ({ name, value })
            )
          }
        ]
      });

      logger.info('Topic configuration altered successfully', { 
        topic: topicName,
        configEntries 
      });
    } catch (error) {
      logger.error('Failed to alter topic configuration', { 
        error, 
        topic: topicName, 
        configEntries 
      });
      throw error;
    }
  }

  /**
   * Get topic configuration
   */
  async getTopicConfig(topicName: string): Promise<any> {
    if (!this.isConnected) {
      throw new Error('Admin is not connected');
    }

    try {
      const configs = await this.admin.describeConfigs({
        resources: [
          {
            type: 2, // TOPIC
            name: topicName
          }
        ]
      });

      logger.info('Topic configuration retrieved', { 
        topic: topicName 
      });
      
      return configs.resources[0];
    } catch (error) {
      logger.error('Failed to get topic configuration', { error, topic: topicName });
      throw error;
    }
  }

  /**
   * Check if topic exists
   */
  async topicExists(topicName: string): Promise<boolean> {
    try {
      const topics = await this.listTopics();
      return topics.includes(topicName);
    } catch (error) {
      logger.error('Failed to check if topic exists', { error, topic: topicName });
      return false;
    }
  }

  /**
   * Initialize all application topics
   */
  async initializeApplicationTopics(): Promise<void> {
    try {
      const topicConfigs = getAllTopics();
      const existingTopics = await this.listTopics();
      
      const topicsToCreate = topicConfigs.filter(
        config => !existingTopics.includes(config.name)
      );

      if (topicsToCreate.length > 0) {
        logger.info('Creating missing application topics', {
          topicsToCreate: topicsToCreate.map(t => t.name)
        });
        
        await this.createTopics(topicsToCreate);
      } else {
        logger.info('All application topics already exist');
      }
    } catch (error) {
      logger.error('Failed to initialize application topics', { error });
      throw error;
    }
  }

  /**
   * Clean up test topics (topics starting with test-)
   */
  async cleanupTestTopics(): Promise<void> {
    try {
      const allTopics = await this.listTopics();
      const testTopics = allTopics.filter(topic => topic.startsWith('test-'));
      
      if (testTopics.length > 0) {
        logger.info('Cleaning up test topics', { testTopics });
        await this.deleteTopics(testTopics);
      } else {
        logger.info('No test topics to clean up');
      }
    } catch (error) {
      logger.error('Failed to cleanup test topics', { error });
      throw error;
    }
  }

  /**
   * Reset topic (delete and recreate)
   */
  async resetTopic(topicConfig: TopicConfig): Promise<void> {
    try {
      const exists = await this.topicExists(topicConfig.name);
      
      if (exists) {
        logger.info('Deleting existing topic for reset', { topic: topicConfig.name });
        await this.deleteTopic(topicConfig.name);
        
        // Wait a bit for deletion to propagate
        await new Promise(resolve => setTimeout(resolve, 2000));
      }
      
      logger.info('Creating topic after reset', { topic: topicConfig.name });
      await this.createTopic(topicConfig);
      
    } catch (error) {
      logger.error('Failed to reset topic', { error, topic: topicConfig.name });
      throw error;
    }
  }

  /**
   * Get topic statistics
   */
  async getTopicStats(): Promise<any> {
    try {
      const topics = await this.listTopics();
      const stats = {
        totalTopics: topics.length,
        applicationTopics: 0,
        testTopics: 0,
        otherTopics: 0
      };

      const appTopicNames = getAllTopics().map(t => t.name);
      
      for (const topic of topics) {
        if (appTopicNames.includes(topic)) {
          stats.applicationTopics++;
        } else if (topic.startsWith('test-')) {
          stats.testTopics++;
        } else {
          stats.otherTopics++;
        }
      }

      logger.info('Topic statistics', { stats });
      return stats;
    } catch (error) {
      logger.error('Failed to get topic statistics', { error });
      throw error;
    }
  }
}

export default TopicAdmin;