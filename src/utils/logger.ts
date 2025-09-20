import winston from 'winston';
import { config } from 'dotenv';

config();

const logLevel = process.env.LOG_LEVEL || 'info';
const logFile = process.env.LOG_FILE || 'logs/kafka-demo.log';

// Create logs directory if it doesn't exist
import { mkdirSync } from 'fs';
import { dirname } from 'path';

try {
  mkdirSync(dirname(logFile), { recursive: true });
} catch (error) {
  // Directory might already exist
}

const logger = winston.createLogger({
  level: logLevel,
  format: winston.format.combine(
    winston.format.timestamp({
      format: 'YYYY-MM-DD HH:mm:ss'
    }),
    winston.format.errors({ stack: true }),
    winston.format.json()
  ),
  defaultMeta: { service: 'kafka-ts-demo' },
  transports: [
    new winston.transports.File({ 
      filename: logFile.replace('.log', '-error.log'), 
      level: 'error' 
    }),
    new winston.transports.File({ 
      filename: logFile 
    })
  ]
});

// Add console transport for development
if (process.env.NODE_ENV !== 'production') {
  logger.add(new winston.transports.Console({
    format: winston.format.combine(
      winston.format.colorize(),
      winston.format.simple()
    )
  }));
}

export default logger;