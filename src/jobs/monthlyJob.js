// const pool = require("../db/connection");
// const { fetchAndProcessData } = require("../services/dataProcessor");

// const runMonthlyJob = async () => {
//   try {
//     console.log("▶️ Startar månatlig rapportuppdatering...");

//     const { viewResults } = await fetchAndProcessData();

//     const client = await pool.getConnection();

//     for (const row of viewResults) {
//       await client.execute(
//         `INSERT INTO db_practice.dashboard_report (created, campaign_id, views) 
//          VALUES (NOW(), ?, ?)`,
//         [row.campaign_id, row.views]
//       );
//     }

//     console.log("✅ Månatlig rapportuppdatering klar!");
//     client.release();
//   } catch (err) {
//     console.error("❌ Fel vid månatlig uppdatering:", err);
//   }
// };

// module.exports = runMonthlyJob;
