#!/usr/bin/env node

import { config } from 'dotenv';
import KafkaDemoApp from './app';
import logger from './utils/logger';

// Load environment variables
config();

/**
 * Main entry point for the Kafka TypeScript Demo
 */
async function main() {
  const app = new KafkaDemoApp();
  
  // Handle graceful shutdown
  const shutdown = async (signal: string) => {
    logger.info(`Received ${signal}, shutting down gracefully`);
    try {
      await app.shutdown();
      process.exit(0);
    } catch (error) {
      logger.error('Error during shutdown', { error });
      process.exit(1);
    }
  };

  // Register signal handlers
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));
  
  // Handle uncaught exceptions
  process.on('uncaughtException', (error) => {
    logger.error('Uncaught exception', { error });
    shutdown('uncaughtException');
  });

  process.on('unhandledRejection', (reason, promise) => {
    logger.error('Unhandled rejection', { reason, promise });
    shutdown('unhandledRejection');
  });

  try {
    logger.info('Starting Kafka TypeScript Demo Application');
    logger.info('Environment', { 
      nodeEnv: process.env.NODE_ENV,
      kafkaBrokers: process.env.KAFKA_BROKERS,
      clientId: process.env.KAFKA_CLIENT_ID
    });

    // Initialize the application
    await app.initialize();

    // Parse command line arguments
    const args = process.argv.slice(2);
    const command = args[0] || 'all';

    switch (command) {
      case 'producer':
        logger.info('Running Producer demos only');
        await app.runProducerDemos();
        break;

      case 'consumer':
        const duration = parseInt(args[1]) || 30;
        logger.info('Running Consumer demos only', { durationSeconds: duration });
        await app.runConsumerDemos(duration);
        break;

      case 'admin':
        logger.info('Running Admin demos only');
        await app.runAdminDemos();
        break;

      case 'metrics':
        logger.info('Displaying current metrics');
        const metrics = app.getMetrics();
        console.log(JSON.stringify(metrics, null, 2));
        break;

      case 'health':
        logger.info('Running health check');
        const DescribeAdmin = (await import('./modules/admin/describe.admin')).default;
        const admin = new DescribeAdmin();
        await admin.connect();
        const health = await admin.getHealthStatus();
        console.log(JSON.stringify(health, null, 2));
        await admin.disconnect();
        break;

      case 'interactive':
        logger.info('Starting interactive mode');
        await runInteractiveMode(app);
        break;

      case 'all':
      default:
        logger.info('Running complete Kafka demo (all components)');
        
        // Run admin demos first
        await app.runAdminDemos();
        
        // Run producer demos
        await app.runProducerDemos();
        
        // Run consumer demos for 60 seconds
        const consumerDuration = 60;
        logger.info('Starting consumer demos', { durationSeconds: consumerDuration });
        
        // Start consumer demos in background
        const consumerPromise = app.runConsumerDemos(consumerDuration);
        
        // Wait for consumers to finish
        await consumerPromise;
        
        // Show final metrics
        const finalMetrics = app.getMetrics();
        logger.info('Demo completed - Final metrics', { metrics: finalMetrics });
        break;
    }

    logger.info('Kafka Demo completed successfully');
    
    // If not running consumers, shutdown immediately
    if (!['consumer', 'interactive', 'all'].includes(command)) {
      await app.shutdown();
    }

  } catch (error) {
    logger.error('Kafka Demo failed', { error });
    await app.shutdown();
    process.exit(1);
  }
}

/**
 * Interactive mode for manual testing
 */
async function runInteractiveMode(app: KafkaDemoApp): Promise<void> {
  const readline = require('readline');
  
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });

  const question = (prompt: string): Promise<string> => {
    return new Promise((resolve) => {
      rl.question(prompt, resolve);
    });
  };

  try {
    logger.info('Entering interactive mode. Type "help" for available commands.');
    
    while (true) {
      const input = await question('kafka-demo> ');
      const [command, ...args] = input.trim().split(' ');

      switch (command.toLowerCase()) {
        case 'help':
          console.log(`
Available commands:
  producer          - Run producer demos
  consumer [time]   - Run consumer demos (default: 30 seconds)
  admin             - Run admin demos
  metrics           - Show current metrics
  export [format]   - Export metrics (json|prometheus)
  status            - Show active components
  quit|exit         - Exit interactive mode
  help              - Show this help
          `);
          break;

        case 'producer':
          await app.runProducerDemos();
          break;

        case 'consumer':
          const duration = parseInt(args[0]) || 30;
          await app.runConsumerDemos(duration);
          break;

        case 'admin':
          await app.runAdminDemos();
          break;

        case 'metrics':
          const metrics = app.getMetrics();
          console.log(JSON.stringify(metrics, null, 2));
          break;

        case 'export':
          const format = (args[0] as 'json' | 'prometheus') || 'json';
          const exportedMetrics = app.exportMetrics(format);
          console.log(exportedMetrics);
          break;

        case 'status':
          const activeComponents = app.getActiveComponents();
          console.log('Active components:', activeComponents);
          break;

        case 'quit':
        case 'exit':
          logger.info('Exiting interactive mode');
          rl.close();
          return;

        case '':
          // Empty command, do nothing
          break;

        default:
          console.log(`Unknown command: ${command}. Type "help" for available commands.`);
          break;
      }
    }
  } catch (error) {
    logger.error('Error in interactive mode', { error });
  } finally {
    rl.close();
  }
}

/**
 * Display usage information
 */
function displayUsage(): void {
  console.log(`
Kafka TypeScript Demo Application

Usage:
  npm start [command] [options]
  npm run dev [command] [options]

Commands:
  all                 - Run complete demo (default)
  producer            - Run producer demos only
  consumer [seconds]  - Run consumer demos (default: 30 seconds)
  admin               - Run admin demos only
  metrics             - Display current metrics
  health              - Run health check
  interactive         - Enter interactive mode

Examples:
  npm start                    # Run complete demo
  npm start producer           # Run only producer demos
  npm start consumer 60        # Run consumer demos for 60 seconds
  npm start admin              # Run only admin demos
  npm start interactive        # Enter interactive mode

Environment Variables:
  KAFKA_BROKERS               - Kafka broker addresses (default: localhost:9092)
  KAFKA_CLIENT_ID             - Client ID (default: kafka-ts-demo)
  KAFKA_GROUP_ID              - Consumer group ID (default: kafka-ts-demo-group)
  LOG_LEVEL                   - Logging level (default: info)
  NODE_ENV                    - Environment (development/production)

Docker:
  docker-compose up           # Start Kafka cluster
  docker-compose down         # Stop Kafka cluster
  `);
}

// Show usage if help is requested
if (process.argv.includes('--help') || process.argv.includes('-h')) {
  displayUsage();
  process.exit(0);
}

// Start the application
if (require.main === module) {
  main().catch((error) => {
    logger.error('Application startup failed', { error });
    process.exit(1);
  });
}

export default main;