import dotenv from 'dotenv';
dotenv.config();

const config = {
  // MongoDB Configuration
  sourceMongoUri: process.env.SOURCE_MONGO_URI || '',
  targetMongoUri: process.env.TARGET_MONGO_URI || '',
  database: process.env.MONGODB_DATABASE || 'test',
  
  collections: process.env.MONGODB_COLLECTIONS || '',

  logLevel: process.env.LOG_LEVEL || 'info',
};

// Validate required configuration
const requiredFields = ['sourceMongoUri', 'targetMongoUri'];
const missingFields = requiredFields.filter(field => !config[field]);

if (missingFields.length > 0) {
  throw new Error(`Missing required configuration fields: ${missingFields.join(', ')}`);
}

export default config; 