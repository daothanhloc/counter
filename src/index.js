import mongoose from 'mongoose';
import logger from '../utils/logger.js';
import config from '../config/config.js';

class CounterChangeStream {
  constructor() {
    this.changeStreams = new Map();
    this.targetColl = null;
  }

  async connect() {
    try {
      // Connect to source MongoDB
      await mongoose.connect(config.sourceMongoUri, {
        useNewUrlParser: true,
        useUnifiedTopology: true,
        dbName: config.database,
      });
      logger.info('Connected to source MongoDB');

      // Connect to target MongoDB
      const targetMongoose = mongoose.createConnection(config.targetMongoUri, {
        useNewUrlParser: true,
        useUnifiedTopology: true,
        dbName: 'counter_stream_events',
      });
      this.targetColl = targetMongoose.collection('counter_stream_events');
      
      logger.info('Connected to target MongoDB');
    } catch (error) {
      logger.error({ err: error }, 'Failed to connect to services');
      throw error;
    }
  }

  async watchCollection(collection) {
    const db = mongoose.connection.db;
    const coll = db.collection(collection);
    
    const changeStream = coll.watch([], {
      fullDocument: 'updateLookup',
    });

    logger.info(`Watching collection: ${collection}`);

    changeStream.on('change', (change) => {
      logger.info('changestream------' + collection)
      switch (change.operationType) {
        case 'insert':
        case 'update':
          this.targetColl.insertOne({
            collection: collection,
            operationType: change.operationType,
            timestamp: new Date(),
          });
          break;

        case 'delete':
          this.targetColl.insertOne({
            collection: collection,
            operationType: change.operationType,
            timestamp: new Date(),
          });
          break;

        default:
          return;
      }

    });

    changeStream.on('error', (error) => {
      logger.error({ err: error, collection }, 'Change stream error');
    });

    this.changeStreams.set(collection, changeStream);
  }

  async start() {
    try {
      await this.connect();
      
      // Watch all configured collections
      for (const collection of config.collections.split(',')) {
        await this.watchCollection(collection);
      }
      
      this.setupGracefulShutdown();
      logger.info('CDC producer started successfully');
    } catch (error) {
      logger.error({ err: error }, 'Failed to start CDC producer');
      process.exit(1);
    }
  }

  setupGracefulShutdown() {
    const shutdown = async () => {
      logger.info('Shutting down CDC producer...');
      
      try {
        // Close all change streams
        for (const [collection, stream] of this.changeStreams) {
          await stream.close();
          logger.info(`Closed change stream for collection: ${collection}`);
        }

        await mongoose.connection.close();
        logger.info('CDC producer shutdown complete');
        process.exit(0);
      } catch (error) {
        logger.error({ err: error }, 'Error during shutdown');
        process.exit(1);
      }
    };

    process.on('SIGTERM', shutdown);
    process.on('SIGINT', shutdown);
  }
}

// Start the producer
const counterChangeStream = new CounterChangeStream();
counterChangeStream.start().catch((error) => {
  logger.error({ err: error }, 'Fatal error in CDC producer');
  process.exit(1);
});     
