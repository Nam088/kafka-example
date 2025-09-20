import { Admin } from 'kafkajs';
import KafkaConnection from '../../config/kafka.config';
import logger from '../../utils/logger';

/**
 * Describe Admin
 * Provides detailed information about Kafka cluster, topics, and consumer groups
 */
export class DescribeAdmin {
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
      logger.info('Describe admin connected successfully');
    } catch (error) {
      logger.error('Failed to connect describe admin', { error });
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
      logger.info('Describe admin disconnected');
    } catch (error) {
      logger.error('Failed to disconnect describe admin', { error });
      throw error;
    }
  }

  /**
   * Describe cluster information
   */
  async describeCluster(): Promise<any> {
    if (!this.isConnected) {
      throw new Error('Admin is not connected');
    }

    try {
      const cluster = await this.admin.describeCluster();
      
      logger.info('Cluster description retrieved', {
        brokersCount: cluster.brokers.length,
        controllerId: cluster.controller
      });

      const clusterInfo = {
        clusterId: cluster.clusterId,
        controller: cluster.controller,
        brokers: cluster.brokers.map(broker => ({
          nodeId: broker.nodeId,
          host: broker.host,
          port: broker.port,
          rack: (broker as any).rack
        }))
      };

      return clusterInfo;
    } catch (error) {
      logger.error('Failed to describe cluster', { error });
      throw error;
    }
  }

  /**
   * Describe topic details
   */
  async describeTopic(topicName: string): Promise<any> {
    if (!this.isConnected) {
      throw new Error('Admin is not connected');
    }

    try {
      const metadata = await this.admin.fetchTopicMetadata({
        topics: [topicName]
      });

      const topic = metadata.topics[0];
      
      if (!topic) {
        throw new Error(`Topic ${topicName} not found`);
      }

      const topicInfo = {
        name: topic.name,
        partitions: topic.partitions.map(partition => ({
          partitionId: partition.partitionId,
          leader: partition.leader,
          replicas: partition.replicas,
          isr: partition.isr,
          offlineReplicas: partition.offlineReplicas
        }))
      };

      logger.info('Topic description retrieved', {
        topic: topicName,
        partitionCount: topicInfo.partitions.length
      });

      return topicInfo;
    } catch (error) {
      logger.error('Failed to describe topic', { error, topic: topicName });
      throw error;
    }
  }

  /**
   * Get topic offsets (latest and earliest)
   */
  async getTopicOffsets(topicName: string): Promise<any> {
    if (!this.isConnected) {
      throw new Error('Admin is not connected');
    }

    try {
      const topicOffsets = await this.admin.fetchTopicOffsets(topicName);
      
      logger.info('Topic offsets retrieved', {
        topic: topicName,
        partitionCount: topicOffsets.length
      });

      return topicOffsets.map(offset => ({
        partition: offset.partition,
        low: offset.low,
        high: offset.high,
        messageCount: parseInt(offset.high) - parseInt(offset.low)
      }));
    } catch (error) {
      logger.error('Failed to get topic offsets', { error, topic: topicName });
      throw error;
    }
  }

  /**
   * List all consumer groups
   */
  async listConsumerGroups(): Promise<any[]> {
    if (!this.isConnected) {
      throw new Error('Admin is not connected');
    }

    try {
      const groups = await this.admin.listGroups();
      
      logger.info('Consumer groups listed', {
        groupCount: groups.groups.length
      });

      return groups.groups.map(group => ({
        groupId: group.groupId,
        protocolType: group.protocolType
      }));
    } catch (error) {
      logger.error('Failed to list consumer groups', { error });
      throw error;
    }
  }

  /**
   * Describe consumer group
   */
  async describeConsumerGroup(groupId: string): Promise<any> {
    if (!this.isConnected) {
      throw new Error('Admin is not connected');
    }

    try {
      const groupDescription = await this.admin.describeGroups([groupId]);
      const group = groupDescription.groups[0];

      if (!group) {
        throw new Error(`Consumer group ${groupId} not found`);
      }

      const groupInfo = {
        groupId: group.groupId,
        state: group.state,
        protocolType: group.protocolType,
        protocol: group.protocol,
        members: group.members.map(member => ({
          memberId: member.memberId,
          clientId: member.clientId,
          clientHost: member.clientHost,
          memberMetadata: member.memberMetadata,
          memberAssignment: member.memberAssignment
        }))
      };

      logger.info('Consumer group described', {
        groupId,
        state: group.state,
        memberCount: group.members.length
      });

      return groupInfo;
    } catch (error) {
      logger.error('Failed to describe consumer group', { error, groupId });
      throw error;
    }
  }

  /**
   * Get consumer group offsets
   */
  async getConsumerGroupOffsets(groupId: string, topics?: string[]): Promise<any> {
    if (!this.isConnected) {
      throw new Error('Admin is not connected');
    }

    try {
      const offsetsData = await this.admin.fetchOffsets(
        topics ? { groupId, topics } : { groupId }
      );

      logger.info('Consumer group offsets retrieved', {
        groupId,
        topicCount: offsetsData.length
      });

      return offsetsData.map(topicData => ({
        topic: topicData.topic,
        partitions: topicData.partitions.map(partition => ({
          partition: partition.partition,
          offset: partition.offset,
          metadata: partition.metadata
        }))
      }));
    } catch (error) {
      logger.error('Failed to get consumer group offsets', { error, groupId });
      throw error;
    }
  }

  /**
   * Get consumer lag for a group
   */
  async getConsumerLag(groupId: string, topicName: string): Promise<any[]> {
    if (!this.isConnected) {
      throw new Error('Admin is not connected');
    }

    try {
      // Get topic offsets (latest)
      const topicOffsets = await this.getTopicOffsets(topicName);
      
      // Get consumer group offsets
      const consumerOffsets = await this.getConsumerGroupOffsets(groupId, [topicName]);
      
      const topicConsumerData = consumerOffsets.find((t: any) => t.topic === topicName);
      
      if (!topicConsumerData) {
        return [];
      }

      const lagData = topicOffsets.map((topicPartition: any) => {
        const consumerPartition = topicConsumerData.partitions.find(
          (p: any) => p.partition === topicPartition.partition
        );

        const consumerOffset = consumerPartition ? parseInt(consumerPartition.offset) : 0;
        const latestOffset = parseInt(topicPartition.high);
        const lag = latestOffset - consumerOffset;

        return {
          partition: topicPartition.partition,
          latestOffset,
          consumerOffset,
          lag
        };
      });

      logger.info('Consumer lag calculated', {
        groupId,
        topic: topicName,
        totalLag: lagData.reduce((sum: number, p: any) => sum + p.lag, 0)
      });

      return lagData;
    } catch (error) {
      logger.error('Failed to get consumer lag', { error, groupId, topic: topicName });
      throw error;
    }
  }

  /**
   * Get broker configurations
   */
  async getBrokerConfigs(brokerId?: number): Promise<any> {
    if (!this.isConnected) {
      throw new Error('Admin is not connected');
    }

    try {
      const resources = [];
      
      if (brokerId !== undefined) {
        resources.push({
          type: 4, // BROKER
          name: brokerId.toString()
        });
      } else {
        // Get all brokers
        const cluster = await this.describeCluster();
        for (const broker of cluster.brokers) {
          resources.push({
            type: 4, // BROKER
            name: broker.nodeId.toString()
          });
        }
      }

      const configs = await this.admin.describeConfigs({ 
        resources,
        includeSynonyms: false 
      });

      logger.info('Broker configurations retrieved', {
        brokerCount: configs.resources.length
      });

      return configs.resources.map(resource => ({
        brokerId: resource.resourceName,
        configs: resource.configEntries.map(entry => ({
          name: entry.configName,
          value: entry.configValue,
          source: entry.configSource,
          isSensitive: entry.isSensitive,
          isReadOnly: entry.readOnly
        }))
      }));
    } catch (error) {
      logger.error('Failed to get broker configurations', { error, brokerId });
      throw error;
    }
  }

  /**
   * Get comprehensive cluster overview
   */
  async getClusterOverview(): Promise<any> {
    try {
      const [cluster, topics, consumerGroups] = await Promise.all([
        this.describeCluster(),
        this.admin.listTopics(),
        this.listConsumerGroups()
      ]);

      const overview = {
        cluster: {
          clusterId: cluster.clusterId,
          controller: cluster.controller,
          brokerCount: cluster.brokers.length,
          brokers: cluster.brokers
        },
        topics: {
          count: topics.length,
          names: topics
        },
        consumerGroups: {
          count: consumerGroups.length,
          groups: consumerGroups
        }
      };

      logger.info('Cluster overview generated', {
        brokerCount: overview.cluster.brokerCount,
        topicCount: overview.topics.count,
        consumerGroupCount: overview.consumerGroups.count
      });

      return overview;
    } catch (error) {
      logger.error('Failed to get cluster overview', { error });
      throw error;
    }
  }

  /**
   * Monitor topic and consumer health
   */
  async getHealthStatus(): Promise<any> {
    try {
      const cluster = await this.describeCluster();
      const topics = await this.admin.listTopics();
      const consumerGroups = await this.listConsumerGroups();

      const health = {
        status: 'healthy',
        issues: [] as string[],
        cluster: {
          brokersOnline: cluster.brokers.length,
          controllerPresent: cluster.controller !== null
        },
        topics: {
          total: topics.length,
          accessible: 0
        },
        consumerGroups: {
          total: consumerGroups.length,
          active: 0
        }
      };

      // Check controller
      if (cluster.controller === null) {
        health.status = 'degraded';
        health.issues.push('No cluster controller');
      }

      // Sample check a few topics for accessibility
      const sampleTopics = topics.slice(0, Math.min(5, topics.length));
      for (const topic of sampleTopics) {
        try {
          await this.admin.fetchTopicMetadata({ topics: [topic] });
          health.topics.accessible++;
        } catch (error) {
          health.issues.push(`Topic ${topic} not accessible`);
        }
      }

      if (health.issues.length > 0) {
        health.status = health.issues.length > 3 ? 'unhealthy' : 'degraded';
      }

      logger.info('Health status checked', {
        status: health.status,
        issueCount: health.issues.length
      });

      return health;
    } catch (error) {
      logger.error('Failed to get health status', { error });
      return {
        status: 'unhealthy',
        issues: ['Failed to connect to cluster'],
        error: error instanceof Error ? error.message : String(error)
      };
    }
  }
}

export default DescribeAdmin;