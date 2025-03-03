const cron = require("node-cron");
const runDailyJob = require("./src/jobs/dailyJob");

cron.schedule("* * * * *", () => {
  console.log("⏳ Kör daglig rapportuppdatering...");
  runDailyJob();
});


