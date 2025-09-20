import { Producer } from 'kafkajs';
import KafkaConnection from '../../config/kafka.config';
import { getTopicName } from '../../core/topics';
import logger from '../../utils/logger';

/**
 * Transaction Producer
 * Demonstrates transactional message sending for exactly-once semantics
 */
export class TransactionProducer {
  private producer: Producer;
  private isConnected: boolean = false;
  private transactionalId: string;

  constructor(transactionalId?: string) {
    this.transactionalId = transactionalId || `transaction-producer-${Date.now()}`;
    this.producer = KafkaConnection.createTransactionalProducer(this.transactionalId);
  }

  /**
   * Connect to Kafka
   */
  async connect(): Promise<void> {
    try {
      await this.producer.connect();
      this.isConnected = true;
      logger.info('Transaction producer connected successfully', {
        transactionalId: this.transactionalId
      });
    } catch (error) {
      logger.error('Failed to connect transaction producer', { error });
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
      logger.info('Transaction producer disconnected');
    } catch (error) {
      logger.error('Failed to disconnect transaction producer', { error });
      throw error;
    }
  }

  /**
   * Send messages within a transaction
   */
  async sendTransactional(
    messages: Array<{
      topic: string;
      key?: string;
      value: any;
    }>
  ): Promise<void> {
    if (!this.isConnected) {
      throw new Error('Producer is not connected');
    }

    const transaction = await this.producer.transaction();

    try {
      logger.info('Starting transaction', { transactionalId: this.transactionalId });

      // Prepare all messages for sending
      const sendPromises = messages.map(async ({ topic, key, value }) => {
        return transaction.send({
          topic,
          messages: [
            {
              key: key || null,
              value: JSON.stringify(value),
              timestamp: Date.now().toString()
            }
          ]
        });
      });

      // Send all messages within the transaction
      const results = await Promise.all(sendPromises);

      // Commit the transaction
      await transaction.commit();

      logger.info('Transaction committed successfully', {
        transactionalId: this.transactionalId,
        messageCount: messages.length,
        results: results.map(r => r.map(res => ({
          partition: res.partition,
          offset: res.baseOffset
        })))
      });
    } catch (error) {
      logger.error('Transaction failed, aborting', { 
        error, 
        transactionalId: this.transactionalId 
      });
      
      try {
        await transaction.abort();
        logger.info('Transaction aborted successfully');
      } catch (abortError) {
        logger.error('Failed to abort transaction', { abortError });
      }
      
      throw error;
    }
  }

  /**
   * Transfer money between accounts (atomic operation)
   */
  async transferMoney(
    fromAccount: string,
    toAccount: string,
    amount: number,
    transferId: string
  ): Promise<void> {
    const timestamp = new Date().toISOString();

    const messages = [
      {
        topic: getTopicName('TRANSACTION'),
        key: fromAccount,
        value: {
          id: transferId,
          type: 'DEBIT',
          account: fromAccount,
          amount: -amount,
          timestamp,
          description: `Transfer to ${toAccount}`
        }
      },
      {
        topic: getTopicName('TRANSACTION'),
        key: toAccount,
        value: {
          id: transferId,
          type: 'CREDIT',
          account: toAccount,
          amount: amount,
          timestamp,
          description: `Transfer from ${fromAccount}`
        }
      },
      {
        topic: getTopicName('TRANSACTION'),
        key: transferId,
        value: {
          id: transferId,
          type: 'TRANSFER_COMPLETED',
          fromAccount,
          toAccount,
          amount,
          timestamp,
          status: 'SUCCESS'
        }
      }
    ];

    await this.sendTransactional(messages);
  }

  /**
   * Process order with inventory update (atomic operation)
   */
  async processOrder(
    orderId: string,
    customerId: string,
    items: Array<{ productId: string; quantity: number; price: number }>
  ): Promise<void> {
    const timestamp = new Date().toISOString();
    const totalAmount = items.reduce((sum, item) => sum + (item.quantity * item.price), 0);

    const messages = [
      // Order created event
      {
        topic: getTopicName('ORDER_EVENTS'),
        key: orderId,
        value: {
          orderId,
          customerId,
          items,
          totalAmount,
          status: 'CREATED',
          timestamp,
          type: 'ORDER_CREATED'
        }
      },
      // Inventory updates for each item
      ...items.map(item => ({
        topic: getTopicName('ORDER_EVENTS'),
        key: item.productId,
        value: {
          orderId,
          productId: item.productId,
          quantityReserved: item.quantity,
          timestamp,
          type: 'INVENTORY_RESERVED'
        }
      })),
      // Payment processed
      {
        topic: getTopicName('TRANSACTION'),
        key: customerId,
        value: {
          orderId,
          customerId,
          amount: totalAmount,
          timestamp,
          type: 'PAYMENT_PROCESSED'
        }
      }
    ];

    await this.sendTransactional(messages);
  }

  /**
   * Simulate a failed transaction
   */
  async simulateFailedTransaction(): Promise<void> {
    const messages = [
      {
        topic: getTopicName('DEMO'),
        key: 'test-1',
        value: { message: 'This should not be committed', timestamp: new Date().toISOString() }
      },
      {
        topic: getTopicName('DEMO'),
        key: 'test-2',
        value: { message: 'This should also not be committed', timestamp: new Date().toISOString() }
      }
    ];

    const transaction = await this.producer.transaction();

    try {
      logger.info('Starting transaction that will fail');

      await transaction.send({
        topic: messages[0].topic,
        messages: [
          {
            key: messages[0].key,
            value: JSON.stringify(messages[0].value)
          }
        ]
      });

      // Simulate an error
      throw new Error('Simulated transaction failure');

    } catch (error) {
      logger.info('Expected error occurred, aborting transaction', { 
        error: error instanceof Error ? error.message : String(error) 
      });
      await transaction.abort();
      logger.info('Transaction aborted - no messages should be committed');
    }
  }

  /**
   * Get transactional ID
   */
  getTransactionalId(): string {
    return this.transactionalId;
  }
}

export default TransactionProducer;