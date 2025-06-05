import mongoose from "mongoose";
import dotenv from "dotenv";
import moment from "moment";
dotenv.config();
class SyncData {
  constructor() {
    this.sourceDb = null;
    this.targetDb = null;
  }

  async connect() {
    this.sourceDb = mongoose.createConnection(process.env.SOURCE_DB_URL, {
      useNewUrlParser: true,
      useUnifiedTopology: true,
      dbName: process.env.DB_NAME,
    });
    this.sourceDb.on("error", (error) => {
      console.error("Source database connection error:", error);
    });
    this.sourceDb.on("open", () => {
      console.log("Source database connection opened");
    });
    this.targetDb = mongoose.createConnection(process.env.TARGET_DB_URL, {
      useNewUrlParser: true,
      useUnifiedTopology: true,
      dbName: process.env.TARGET_DB_NAME,
    });
    this.targetDb.on("error", (error) => {
      console.error("Target database connection error:", error);
    });
    this.targetDb.on("open", () => {
      console.log("Target database connection opened");
    });
  }

  async syncCollection(collectionName) {
    const sourceCollection = this.sourceDb.collection(collectionName);
    const targetCollection = this.targetDb.collection(collectionName);

    const syncTimeCollection = this.targetDb.collection("sync_times");

    // Find last sync time
    const lastSyncTime = await syncTimeCollection.findOne(
      {
        collName: collectionName,
      },
      {
        sort: {
          _id: -1,
        },
      }
    );

    const lastSyncTimeValue =
      lastSyncTime?.syncTime || moment().subtract(1, "hour").toDate();
    const now = moment().toDate();

    const query = {
      updatedAt: {
        $gte: lastSyncTimeValue,
        $lte: now,
      },
    };

    await new Promise((resolve) => setTimeout(resolve, 1000));
    const sourceData = sourceCollection.collection.find(query);

    let count = 0;
    let batchUpsert = [];
    const batchUpsertSize = 100;

    while (await sourceData.hasNext()) {
      const doc = await sourceData.next();
      batchUpsert.push({
        updateOne: {
          filter: { _id: new mongoose.Types.ObjectId(doc._id) },
          update: { $set: doc },
          upsert: true,
        },
      });
      count++;
      if (batchUpsert.length === batchUpsertSize) {
        console.log("sync document: " + collectionName + " index: " + count);
        await targetCollection.bulkWrite(batchUpsert);
        batchUpsert = [];
      }
    }

    if (batchUpsert.length > 0) {
      console.log("sync document: " + collectionName + " index: " + count);
      await targetCollection.bulkWrite(batchUpsert);
      batchUpsert = [];
    }

    const updatedCount = await targetCollection.countDocuments({
      updatedAt: {
        $gte: lastSyncTimeValue,
        $lte: now,
      },
    });

    await syncTimeCollection.insertOne({
      collName: collectionName,
      syncTime: moment(now).subtract(1, "minute").toDate(),
      foundUpdateDocCount: count,
      updatedDocCount: updatedCount,
    });
  }

  async closeConnection() {
    if (this.sourceDb) {
      await this.sourceDb.close();
    }
    if (this.targetDb) {
      await this.targetDb.close();
    }
  }
}

export default SyncData;
