const pool = require("../db/connection");
const { fetchAndProcessData } = require("../services/dataProcessor");

const runDailyJob = async () => {
  try {
    console.log("▶️ Startar daglig rapportuppdatering...");

    //Hämta all data från funktionen "fetchAndProcessData". 
    const {
      viewResults, campaignViewResults, leadResults, paidLeadResults,
      uniqueLeadResults, recuringLeadResults, conversionRateResults, smsPartsResults
    } = await fetchAndProcessData();

    const client = await pool.getConnection();
    
     //  Startar transaktion för ACID-säkerhet
    await client.beginTransaction();

    //Spara visningar per länk (Action #1)
    for (const row of viewResults || []) {
      try {
        await client.execute(
          `INSERT INTO db_practice.dashboard_report (date, campaign_id, link, views) 
           VALUES (CURRENT_DATE, ?, ?, ?)`,
          [row.campaign_id, row.link, row.views]
        );
      } catch (error) {
        console.error("❌ SQL-fel vid INSERT (views per länk):", error);
      }
    }

    //Spara kampanjens totalvisningar (Action #2)
    for (const row of campaignViewResults || []) {
      try {
        await client.execute(
          `INSERT INTO db_practice.dashboard_report (date, campaign_id, views) 
           VALUES (CURRENT_DATE, ?, ?)`,
          [row.campaign_id, row.views]
        );
      } catch (error) {
        console.error("❌ SQL-fel vid INSERT (totalvisningar):", error);
      }
    }

    //Uppdatera leads
    for (const row of leadResults || []) {
      try {
        await client.execute(
          `UPDATE db_practice.dashboard_report SET leads = ? 
           WHERE campaign_id = ? AND date = CURRENT_DATE`,
          [row.leads, row.campaign_id]
        );
      } catch (error) {
        console.error("❌ SQL-fel vid UPDATE (leads):", error);
      }
    }

    //Uppdatera paid_leads
    for (const row of paidLeadResults || []) {
      try {
        await client.execute(
          `UPDATE db_practice.dashboard_report SET paid_leads = ? 
           WHERE campaign_id = ? AND date = CURRENT_DATE`,
          [row.paid_leads, row.campaign_id]
        );
      } catch (error) {
        console.error("❌ SQL-fel vid UPDATE (paid_leads):", error);
      }
    }

    //Uppdatera unique_leads
    for (const row of uniqueLeadResults || []) {
      try {
        await client.execute(
          `UPDATE db_practice.dashboard_report SET unique_leads = ? 
           WHERE campaign_id = ? AND date = CURRENT_DATE`,
          [row.unique_leads, row.campaign_id]
        );
      } catch (error) {
        console.error("❌ SQL-fel vid UPDATE (unique_leads):", error);
      }
    }

    //Uppdatera recuring_leads
    for (const row of recuringLeadResults || []) {
      try {
        await client.execute(
          `UPDATE db_practice.dashboard_report SET recuring_leads = ? 
           WHERE campaign_id = ? AND date = CURRENT_DATE`,
          [row.recuring_leads, row.campaign_id]
        );
      } catch (error) {
        console.error("❌ SQL-fel vid UPDATE (recuring_leads):", error);
      }
    }

    //Uppdatera conversion_rate
    for (const row of conversionRateResults || []) {
      try {
        await client.execute(
          `UPDATE db_practice.dashboard_report SET conversion_rate = ? 
           WHERE campaign_id = ? AND date = CURRENT_DATE`,
          [row.conversion_rate, row.campaign_id]
        );
      } catch (error) {
        console.error("❌ SQL-fel vid UPDATE (conversion_rate):", error);
      }
    }

    //Uppdatera sms_parts
    for (const row of smsPartsResults || []) {
      try {
        await client.execute(
          `UPDATE db_practice.dashboard_report SET sms_parts = ? 
           WHERE campaign_id = ? AND date = CURRENT_DATE`,
          [row.sms_parts, row.campaign_id]
        );
      } catch (error) {
        console.error("❌ SQL-fel vid UPDATE (sms_parts):", error);
      }
    }

    await client.commit(); 
    console.log("✅ Daglig rapportuppdatering klar!");
    client.release();
  } catch (err) {
    console.error("❌ Fel vid daglig uppdatering:", err);
  }
};

module.exports = runDailyJob;
