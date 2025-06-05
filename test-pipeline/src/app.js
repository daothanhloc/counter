import nodeCron from "node-cron";
import SyncData from "./service.js";

(async () => {
  const syncData = new SyncData();

  await syncData.connect();
  await syncData.syncCollection("memberships");
  await syncData.syncCollection("transactions");
  await syncData.closeConnection();
})();

nodeCron.schedule("*/15 * * * *", async () => {
  console.log("Running 15 minutes");
  const syncData = new SyncData();

  await syncData.connect();
  await syncData.syncCollection("memberships");
  await syncData.syncCollection("transactions");
  await syncData.closeConnection();
});
