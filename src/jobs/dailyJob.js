const { getDatabaseConnection, getDatabaseNames, databaseNames } = require("../db/connection");
const { fetchAndProcessData } = require("../services/dataProcessor");

const runDailyJob = async () => {
  console.log("▶️ Fetching all databases...");

  const dbList = databaseNames.length ? databaseNames : await getDatabaseNames();
  console.log("📌 Databaser att processa:", dbList);

  for (const dbName of dbList) {
    console.log(`🔄  Running for database: ${dbName}`);
    const pool = await getDatabaseConnection(dbName); 

    try {
      const {
        viewResults, leadResults, paidLeadResults,
        uniqueLeadsResults, recurringLeadsResults,
        smsPartsResults, couponSentResults,
        moneyReceivedResults, engagementTimeResults,
        transactionAbandonedResults, gamesFinishedResults
      } = await fetchAndProcessData(pool); 

      const client = await pool.getConnection();
      await client.beginTransaction();

      const today = new Date().toISOString().split("T")[0];
      await client.query(`DELETE FROM dashboard_report WHERE date = ?;`, [today]);

      const mergeData = {};
      const campaignTotals = {};
      const uniqueCampaignLinks = {};

      const engagementTimeMap = {};
      engagementTimeResults.forEach(({ campaign_id, link, engagement_time }) => {
        const key = `${campaign_id}-${link || "TOTAL"}`;
        engagementTimeMap[key] = engagement_time || "0s";
      });

      const processData = (rows, keyName) => {
        rows.forEach(({ campaign_id, link, [keyName]: value = 0, created_date }) => {
          const key = `${campaign_id}-${link || "TOTAL"}`;

          if (!mergeData[key]) {
            mergeData[key] = { 
              date: created_date, campaign_id, link: link || "TOTAL",
              views: 0, leads: 0, paid_leads: 0,
              unique_leads: 0, recuring_leads: 0, sms_parts: 0,
              conversion_rate: "0%", giftcards_sent: 0,
              money_received: "0.00",
              avarage_payment: "0",
              engagement_time: "0s",
              answers_percentage: "0%", games_finished: "0",
              abandoned_transactions: 0 
            };
          }

          if (keyName === "money_received") {
            mergeData[key].money_received = (parseFloat(mergeData[key].money_received) + parseFloat(value)).toFixed(2);
          } else if (keyName === "engagement_time") {
            mergeData[key].engagement_time = value; 
          } else {
            mergeData[key][keyName] += value;
          }

          mergeData[key].conversion_rate = mergeData[key].views > 0 
            ? ((mergeData[key].leads / mergeData[key].views) * 100).toFixed(2) + "%" 
            : "0%";

          mergeData[key].avarage_payment = mergeData[key].paid_leads > 0 
            ? (parseFloat(mergeData[key].money_received) / mergeData[key].paid_leads).toFixed(2) 
            : "0";

          if (!uniqueCampaignLinks[campaign_id]) {
            uniqueCampaignLinks[campaign_id] = new Set();
          }
          if (link) {
            uniqueCampaignLinks[campaign_id].add(link);
          }

          if (!campaignTotals[campaign_id]) {
            campaignTotals[campaign_id] = { 
              views: 0, leads: 0, paid_leads: 0, unique_leads: 0, 
              recuring_leads: 0, sms_parts: 0, giftcards_sent: 0, 
              money_received: 0.00, games_finished: 0, paid_leads_total: 0,
              total_engagement_time: 0, engagement_time_count: 0,
              abandoned_transactions: 0 
            };
          }
          campaignTotals[campaign_id][keyName] += value;

          if (keyName === "paid_leads") {
            campaignTotals[campaign_id].paid_leads_total += value;
          }

          if (keyName === "engagement_time") {
            const timeParts = value.split(":").map(Number);
            const seconds = (timeParts[0] * 3600) + (timeParts[1] * 60) + (timeParts[2] || 0);
            campaignTotals[campaign_id].total_engagement_time += seconds;
            campaignTotals[campaign_id].engagement_time_count += 1;
          }
        });
      };

      processData(viewResults, "views");
      processData(leadResults, "leads");
      processData(paidLeadResults, "paid_leads");
      processData(uniqueLeadsResults, "unique_leads");
      processData(recurringLeadsResults, "recuring_leads");
      processData(smsPartsResults, "sms_parts");
      processData(couponSentResults, "giftcards_sent");
      processData(moneyReceivedResults, "money_received");
      processData(engagementTimeResults, "engagement_time");
      processData(gamesFinishedResults, "games_finished");
      processData(transactionAbandonedResults, "abandoned_transactions");

      Object.entries(mergeData).forEach(([key, row]) => {
        row.answers_percentage = row.views > 0
          ? ((row.abandoned_transactions / row.views) * 100).toFixed(2) + "%"
          : "0.00%";
      });

      Object.entries(campaignTotals).forEach(([campaign_id, totals]) => {
        if (!uniqueCampaignLinks[campaign_id]) return;

        const totalKey = `${campaign_id}-TOTAL`;
        const totalLinksCount = uniqueCampaignLinks[campaign_id]?.size || 0;

        const moneyReceived = Object.entries(mergeData)
          .filter(([key, row]) => key.startsWith(`${campaign_id}-`) && !key.includes("TOTAL"))
          .reduce((sum, [, row]) => sum + parseFloat(row.money_received || 0), 0)
          .toFixed(2);

        const totalPaidLeads = totals.paid_leads_total;

        const averagePayment = totalPaidLeads > 0 
          ? (moneyReceived / totalPaidLeads).toFixed(2) 
          : "0";

        const averageEngagementTime = totals.engagement_time_count > 0
          ? new Date(totals.total_engagement_time * 1000).toISOString().substr(11, 8)
          : "00:00:00";

       const createdDates = Object.entries(mergeData)
          .filter(([key, row]) => key.startsWith(`${campaign_id}-`) && row.date)
          .map(([, row]) => new Date(row.date));

      const earliestCreatedDate = createdDates.length > 0 
           ? new Date(Math.min(...createdDates)) 
            : new Date(); 

        mergeData[totalKey] = {
          date: earliestCreatedDate,
          campaign_id,
          link: `TOTAL (${totalLinksCount} links)`,
          ...totals,
          paid_leads: totalPaidLeads,
          money_received: moneyReceived,
          avarage_payment: averagePayment,
          engagement_time: averageEngagementTime,
          conversion_rate: totals.views > 0 
            ? ((totals.leads / totals.views) * 100).toFixed(2) + "%" 
            : "0%",
          answers_percentage: totals.views > 0
            ? ((totals.abandoned_transactions / totals.views) * 100).toFixed(2) + "%"
            : "0.00%",
          games_finished: totals.games_finished.toString() || "0"
        };
      });

      const allData = Object.values(mergeData)
        .filter(row => row.date && row.campaign_id && row.link)
        .sort((a, b) => a.campaign_id - b.campaign_id || (a.link.startsWith("TOTAL") ? 1 : -1))
        .map(row => [
          row.date, row.campaign_id, row.link, row.views, row.leads, 
          row.paid_leads, row.unique_leads, row.recuring_leads, 
          row.sms_parts, row.conversion_rate, row.giftcards_sent,
          row.money_received, row.avarage_payment, row.engagement_time, 
          row.answers_percentage, row.games_finished, new Date() // Lägg till created datum
        ]);

      if (allData.length > 0) {
        await client.query(
          `INSERT INTO dashboard_report 
          (date, campaign_id, link, views, leads, paid_leads, 
           unique_leads, recuring_leads, sms_parts, conversion_rate, 
           giftcards_sent, money_received, avarage_payment, engagement_time, 
           answers_percentage, games_finished, created) 
          VALUES ?`,
          [allData]
        );
      }

      await client.commit();
      client.release();
      console.log(`✅ Done with: ${dbName}`);
    } catch (err) {
      console.error(`❌ Error updating ${dbName}:`, err);
    }
  }

  console.log("✅ All databases have been processed!");
};

module.exports = runDailyJob;