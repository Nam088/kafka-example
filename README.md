# Kafka TypeScript Demo

[🇺🇸 English](#english) | [🇻🇳 Tiếng Việt](#vietnamese)

---

## English

A comprehensive Kafka TypeScript demonstration application showcasing various Kafka patterns, producers, consumers, admin operations, and monitoring capabilities.

### 📋 Table of Contents

- [Features](#features)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
- [Usage](#usage)
- [Modules](#modules)
- [Monitoring](#monitoring)
- [Docker](#docker)
- [Development](#development)
- [Contributing](#contributing)
- [License](#license)

### 🚀 Features

- **Complete Kafka Integration**: Producers, Consumers, Admin operations
- **Multiple Producer Patterns**: Basic, Partition-aware, Batch, Transactional
- **Advanced Consumer Patterns**: Basic, Consumer Groups, Manual Offset Management, Stream Processing
- **Admin Operations**: Topic management, Cluster description, ACL management
- **Monitoring & Metrics**: Comprehensive metrics collection and export
- **TypeScript**: Full type safety and modern JavaScript features
- **Docker Support**: Easy Kafka cluster setup with Docker Compose
- **Comprehensive Documentation**: Examples and usage patterns

### 📦 Prerequisites

- Node.js >= 16.0.0
- npm or yarn
- Docker and Docker Compose (for Kafka cluster)

### 🛠 Installation

1. Clone the repository:
```bash
git clone https://github.com/your-username/kafka-example.git
cd kafka-example
```

2. Install dependencies:
```bash
npm install
```

3. Copy environment file:
```bash
cp .env.example .env
```

4. Start Kafka cluster:
```bash
docker-compose up -d
```

### ⚡ Quick Start

1. **Start Kafka cluster**:
```bash
docker-compose up -d
```

2. **Build the application**:
```bash
npm run build
```

3. **Run complete demo**:
```bash
npm start
```

4. **Run specific demos**:
```bash
# Producer demos only
npm start producer

# Consumer demos (30 seconds)
npm start consumer

# Admin demos only
npm start admin

# Interactive mode
npm start interactive
```

### 📁 Project Structure

```
kafka-ts-demo/
├── src/
│   ├── config/
│   │   └── kafka.config.ts         # Kafka connection configuration
│   ├── core/
│   │   └── topics.ts               # Common topic definitions
│   ├── modules/
│   │   ├── producer/               # Producer implementations
│   │   │   ├── basic.producer.ts   # Basic message sending
│   │   │   ├── partition.producer.ts # Partition-specific sending
│   │   │   ├── batch.producer.ts   # Batch message sending
│   │   │   └── transaction.producer.ts # Transactional producer
│   │   ├── consumer/               # Consumer implementations
│   │   │   ├── basic.consumer.ts   # Basic consumer
│   │   │   ├── group.consumer.ts   # Consumer group
│   │   │   ├── offset.consumer.ts  # Manual offset management
│   │   │   └── streaming.consumer.ts # Stream processing
│   │   ├── admin/                  # Admin operations
│   │   │   ├── topic.admin.ts      # Topic management
│   │   │   ├── describe.admin.ts   # Cluster description
│   │   │   └── acl.admin.ts        # Access control
│   │   └── monitoring/             # Monitoring & logging
│   │       └── metrics.ts
│   ├── utils/
│   │   └── logger.ts
│   ├── app.ts                      # Main application
│   └── index.ts                    # Entry point
├── docker-compose.yml
├── .env
├── package.json
└── tsconfig.json
```

### ⚙️ Configuration

Environment variables in `.env`:

```bash
# Kafka Configuration
KAFKA_BROKERS=localhost:9092
KAFKA_CLIENT_ID=kafka-ts-demo
KAFKA_GROUP_ID=kafka-ts-demo-group

# Topic Configuration
DEMO_TOPIC=demo-topic
USER_EVENTS_TOPIC=user-events
ORDER_EVENTS_TOPIC=order-events
TRANSACTION_TOPIC=transaction-topic

# Logging
LOG_LEVEL=info
LOG_FILE=logs/kafka-demo.log

# Environment
NODE_ENV=development
```

### 💻 Usage

#### Command Line Interface

```bash
# Run all demos
npm start

# Run specific component demos
npm start producer
npm start consumer [duration_seconds]
npm start admin

# Show metrics
npm start metrics

# Health check
npm start health

# Interactive mode
npm start interactive
```

#### Interactive Mode Commands

```
kafka-demo> help
Available commands:
  producer          - Run producer demos
  consumer [time]   - Run consumer demos (default: 30 seconds)
  admin             - Run admin demos
  metrics           - Show current metrics
  export [format]   - Export metrics (json|prometheus)
  status            - Show active components
  quit|exit         - Exit interactive mode
  help              - Show this help
```

### 📚 Modules

#### Producers

- **Basic Producer**: Simple message sending
- **Partition Producer**: Key-based and manual partition assignment
- **Batch Producer**: High-throughput batch processing
- **Transaction Producer**: ACID transactions for exactly-once semantics

#### Consumers

- **Basic Consumer**: Simple message consumption
- **Group Consumer**: Load balancing across multiple consumers
- **Offset Consumer**: Manual offset management for reliability
- **Streaming Consumer**: Real-time stream processing with windowing

#### Admin

- **Topic Admin**: Create, delete, configure topics
- **Describe Admin**: Cluster and topic information
- **ACL Admin**: Access control management (requires authorization)

#### Monitoring

- **Metrics Collector**: Performance and health metrics
- **Export Formats**: JSON and Prometheus formats
- **Real-time Monitoring**: Live metrics collection

### 📊 Monitoring

The application provides comprehensive monitoring:

- **Producer Metrics**: Messages sent, throughput, errors
- **Consumer Metrics**: Messages consumed, lag, processing time
- **Topic Metrics**: Partition info, offset ranges
- **Cluster Metrics**: Broker status, controller info
- **System Metrics**: Memory, CPU, uptime

Export metrics in JSON or Prometheus format:

```bash
npm start metrics
npm start export prometheus
```

### 🐳 Docker

Start the complete Kafka infrastructure:

```bash
# Start Kafka cluster
docker-compose up -d

# View logs
docker-compose logs -f kafka

# Stop cluster
docker-compose down
```

The Docker setup includes:
- Zookeeper
- Kafka broker
- Kafka UI (accessible at http://localhost:8080)

### 🔧 Development

#### Build and Run

```bash
# Development mode with hot reload
npm run dev

# Build for production
npm run build

# Run built application
npm start

# Run tests
npm test

# Lint code
npm run lint
npm run lint:fix
```

#### Project Commands

```bash
# Clean build artifacts
npm run clean

# Type check
npx tsc --noEmit

# Format code
npx prettier --write src/**/*.ts
```

### 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Run linting and tests
6. Submit a pull request

### 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Vietnamese

Ứng dụng demo Kafka TypeScript toàn diện, trình bày các mẫu Kafka khác nhau, producers, consumers, operations admin và khả năng monitoring.

### 📋 Mục lục

- [Tính năng](#tính-năng)
- [Yêu cầu hệ thống](#yêu-cầu-hệ-thống)
- [Cài đặt](#cài-đặt)
- [Bắt đầu nhanh](#bắt-đầu-nhanh)
- [Cấu trúc dự án](#cấu-trúc-dự-án)
- [Cấu hình](#cấu-hình)
- [Sử dụng](#sử-dụng)
- [Các module](#các-module)
- [Giám sát](#giám-sát)
- [Docker](#docker-vi)
- [Phát triển](#phát-triển)

### 🚀 Tính năng

- **Tích hợp Kafka hoàn chình**: Producers, Consumers, Operations admin
- **Nhiều mẫu Producer**: Cơ bản, Partition-aware, Batch, Transactional
- **Mẫu Consumer nâng cao**: Cơ bản, Consumer Groups, Quản lý Offset thủ công, Stream Processing
- **Operations Admin**: Quản lý topic, Mô tả cluster, Quản lý ACL
- **Giám sát & Metrics**: Thu thập và xuất metrics toàn diện
- **TypeScript**: Type safety đầy đủ và tính năng JavaScript hiện đại
- **Hỗ trợ Docker**: Thiết lập Kafka cluster dễ dàng với Docker Compose
- **Tài liệu toàn diện**: Ví dụ và mẫu sử dụng

### 📦 Yêu cầu hệ thống

- Node.js >= 16.0.0
- npm hoặc yarn
- Docker và Docker Compose (cho Kafka cluster)

### 🛠 Cài đặt

1. Clone repository:
```bash
git clone https://github.com/your-username/kafka-example.git
cd kafka-example
```

2. Cài đặt dependencies:
```bash
npm install
```

3. Copy file môi trường:
```bash
cp .env.example .env
```

4. Khởi động Kafka cluster:
```bash
docker-compose up -d
```

### ⚡ Bắt đầu nhanh

1. **Khởi động Kafka cluster**:
```bash
docker-compose up -d
```

2. **Build ứng dụng**:
```bash
npm run build
```

3. **Chạy demo hoàn chỉnh**:
```bash
npm start
```

4. **Chạy demo cụ thể**:
```bash
# Chỉ demo Producer
npm start producer

# Demo Consumer (30 giây)
npm start consumer

# Chỉ demo Admin
npm start admin

# Chế độ tương tác
npm start interactive
```

### 📁 Cấu trúc dự án

```
kafka-ts-demo/
├── src/
│   ├── config/
│   │   └── kafka.config.ts         # Cấu hình kết nối Kafka
│   ├── core/
│   │   └── topics.ts               # Định nghĩa topic chung
│   ├── modules/
│   │   ├── producer/               # Triển khai Producer
│   │   │   ├── basic.producer.ts   # Gửi message cơ bản
│   │   │   ├── partition.producer.ts # Gửi theo partition cụ thể
│   │   │   ├── batch.producer.ts   # Gửi message theo batch
│   │   │   └── transaction.producer.ts # Producer giao dịch
│   │   ├── consumer/               # Triển khai Consumer
│   │   │   ├── basic.consumer.ts   # Consumer cơ bản
│   │   │   ├── group.consumer.ts   # Consumer group
│   │   │   ├── offset.consumer.ts  # Quản lý offset thủ công
│   │   │   └── streaming.consumer.ts # Stream processing
│   │   ├── admin/                  # Operations admin
│   │   │   ├── topic.admin.ts      # Quản lý topic
│   │   │   ├── describe.admin.ts   # Mô tả cluster
│   │   │   └── acl.admin.ts        # Kiểm soát truy cập
│   │   └── monitoring/             # Giám sát & logging
│   │       └── metrics.ts
│   ├── utils/
│   │   └── logger.ts
│   ├── app.ts                      # Ứng dụng chính
│   └── index.ts                    # Điểm vào
├── docker-compose.yml
├── .env
├── package.json
└── tsconfig.json
```

### ⚙️ Cấu hình

Biến môi trường trong `.env`:

```bash
# Cấu hình Kafka
KAFKA_BROKERS=localhost:9092
KAFKA_CLIENT_ID=kafka-ts-demo
KAFKA_GROUP_ID=kafka-ts-demo-group

# Cấu hình Topic
DEMO_TOPIC=demo-topic
USER_EVENTS_TOPIC=user-events
ORDER_EVENTS_TOPIC=order-events
TRANSACTION_TOPIC=transaction-topic

# Logging
LOG_LEVEL=info
LOG_FILE=logs/kafka-demo.log

# Môi trường
NODE_ENV=development
```

### 💻 Sử dụng

#### Giao diện dòng lệnh

```bash
# Chạy tất cả demo
npm start

# Chạy demo component cụ thể
npm start producer
npm start consumer [thời_gian_giây]
npm start admin

# Hiển thị metrics
npm start metrics

# Kiểm tra sức khỏe
npm start health

# Chế độ tương tác
npm start interactive
```

### 📚 Các module

#### Producers

- **Basic Producer**: Gửi message đơn giản
- **Partition Producer**: Gán partition dựa trên key và thủ công
- **Batch Producer**: Xử lý batch thông lượng cao
- **Transaction Producer**: Giao dịch ACID cho exactly-once semantics

#### Consumers

- **Basic Consumer**: Tiêu thụ message đơn giản
- **Group Consumer**: Cân bằng tải giữa nhiều consumer
- **Offset Consumer**: Quản lý offset thủ công cho độ tin cậy
- **Streaming Consumer**: Xử lý stream thời gian thực với windowing

### 📊 Giám sát

Ứng dụng cung cấp giám sát toàn diện:

- **Producer Metrics**: Messages đã gửi, thông lượng, lỗi
- **Consumer Metrics**: Messages tiêu thụ, lag, thời gian xử lý
- **Topic Metrics**: Thông tin partition, offset ranges
- **Cluster Metrics**: Trạng thái broker, thông tin controller
- **System Metrics**: Memory, CPU, uptime

### 🐳 Docker

Khởi động hạ tầng Kafka hoàn chỉnh:

```bash
# Khởi động Kafka cluster
docker-compose up -d

# Xem logs
docker-compose logs -f kafka

# Dừng cluster
docker-compose down
```

### 🔧 Phát triển

#### Build và Run

```bash
# Chế độ phát triển với hot reload
npm run dev

# Build cho production
npm run build

# Chạy ứng dụng đã build
npm start

# Chạy tests
npm test

# Lint code
npm run lint
npm run lint:fix
```

---

## 📞 Support

For support, please contact:
- Email: support@example.com
- GitHub Issues: [Create an issue](https://github.com/your-username/kafka-example/issues)

Để được hỗ trợ, vui lòng liên hệ:
- Email: support@example.com
- GitHub Issues: [Tạo issue](https://github.com/your-username/kafka-example/issues)