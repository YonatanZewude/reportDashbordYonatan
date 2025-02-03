const cron = require("node-cron");
const runDailyJob = require("./src/jobs/dailyJob");
const runMonthlyJob = require("./src/jobs/monthlyJob");

cron.schedule("* * * * *", () => {
  console.log("⏳ Kör daglig rapportuppdatering...");
  runDailyJob();
});

// cron.schedule("*/2 * * * *", () => {
//   console.log("⏳ Kör månatlig rapportuppdatering...");
//   runMonthlyJob();
// });

// console.log("✅ Cron-jobb aktiverade!");
