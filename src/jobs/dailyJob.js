const pool = require("../db/connection");
const { fetchAndProcessData } = require("../services/dataProcessor");

const runDailyJob = async () => {
  try {
    console.log("▶️ Startar daglig rapportuppdatering...");

    //Hämtar data från databasen via fetchAndProcessData()
    const {
      viewResults, campaignViewResults, leadResults, paidLeadResults,
      uniqueLeadResults, recuringLeadResults, conversionRateResults, smsPartsResults,
      giftcardsSentResults, moneyReceivedResults, averagePaymentResults,
      engagementTimeResults, gamesFinishedResults
    } = await fetchAndProcessData();

    const client = await pool.getConnection();
    await client.beginTransaction();

    const allData = [];
    const mergeData = {};

    const processData = (rows, keyName) => {
      rows.forEach(row => {
        const key = row.campaign_id;

        //Skapar en ny post om den inte redan finns
        if (!mergeData[key]) {
          mergeData[key] = {
            date: new Date().toISOString().split("T")[0], 
            campaign_id: row.campaign_id,
            link: row.link || null,
            views: 0, leads: 0, paid_leads: 0, giftcards_sent: 0,
            money_received: 0, conversion_rate: 0, sms_parts: 0,
            avarage_payment: 0, engagement_time: 0, games_finished: 0,
            unique_leads: new Set(), recuring_leads: new Set()
          };
        }

        //Rätt fält fylls beroende på nyckeln (keyName)
        if (keyName === "views") mergeData[key].views = row.views || 0;
        if (keyName === "leads") mergeData[key].leads = row.leads || 0;
        if (keyName === "paid_leads") mergeData[key].paid_leads = row.paid_leads || 0;
        if (keyName === "giftcards_sent") mergeData[key].giftcards_sent = row.giftcards_sent || 0;
        if (keyName === "money_received") mergeData[key].money_received = row.money_received || 0;
        if (keyName === "conversion_rate") mergeData[key].conversion_rate = row.conversion_rate || 0;
        if (keyName === "sms_parts") mergeData[key].sms_parts = row.sms_parts || 0;
        if (keyName === "avarage_payment") mergeData[key].avarage_payment = row.avarage_payment || 0;
        if (keyName === "engagement_time") mergeData[key].engagement_time = row.engagement_time || 0;
        if (keyName === "games_finished") mergeData[key].games_finished = row.games_finished || 0;
        if (keyName === "unique_leads" && row.unique_leads) mergeData[key].unique_leads.add(row.unique_leads);
        if (keyName === "recuring_leads" && row.recuring_leads) mergeData[key].recuring_leads.add(row.recuring_leads);
      });
    };

    //Använder processData för att sammanställa alla datakategorier
    processData(viewResults, "views");
    processData(campaignViewResults, "views");
    processData(leadResults, "leads");
    processData(paidLeadResults, "paid_leads");
    processData(giftcardsSentResults, "giftcards_sent");
    processData(moneyReceivedResults, "money_received");
    processData(conversionRateResults, "conversion_rate");
    processData(smsPartsResults, "sms_parts");
    processData(averagePaymentResults, "avarage_payment");
    processData(engagementTimeResults, "engagement_time");
    processData(gamesFinishedResults, "games_finished");
    processData(uniqueLeadResults, "unique_leads");
    processData(recuringLeadResults, "recuring_leads");

    //Konverterar mergeData till en array för batch-insert
    for (const data of Object.values(mergeData)) {
      allData.push([
        data.date, data.campaign_id, data.link, data.views, data.leads, 
        data.paid_leads, data.giftcards_sent, data.money_received, 
        data.unique_leads.size, data.recuring_leads.size, data.conversion_rate, 
        data.sms_parts, data.avarage_payment, data.engagement_time, data.games_finished
      ]);
    }

    //Utför batch-insert om det finns data att spara
    if (allData.length > 0) {
      await client.query(
        `INSERT INTO db_practice.dashboard_report (
          date, campaign_id, link, views, leads, paid_leads, giftcards_sent, 
          money_received, unique_leads, recuring_leads, conversion_rate, sms_parts, 
          avarage_payment, engagement_time, games_finished
        ) VALUES ?;`,
        [allData]
      );
    }

    await client.commit();
    client.release();
    console.log("✅ Daglig rapport sparad!");
  } catch (err) {
    console.error("❌ Fel vid daglig uppdatering:", err);
  }
};

module.exports = runDailyJob;
