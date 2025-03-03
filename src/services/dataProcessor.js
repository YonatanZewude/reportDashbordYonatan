const pool = require("../db/connection");

const fetchAndProcessData = async (pool) => {
  try {
    const connection = await pool.getConnection();

    const fetchData = async (query, name) => {
      try {
        const [result] = await connection.execute(query);
        return result || [];
      } catch (err) {
        console.error(`❌ Fel vid hämtning av ${name}:`, err);
        return [];
      }
    };

    const specificDate = "2025-02-01"; 

    const dateCondition = specificDate 
        ? `v.created >= '${specificDate} 00:00:00' AND v.created < '${specificDate} 23:59:59'`
        : `v.created >= NOW() - INTERVAL 1 DAY`;

    const viewResults = await fetchData(`
     SELECT v.campaign_id, 
       v.view_r AS link, 
       COUNT(*) AS views, 
       MIN(COALESCE(p.created, v.created)) AS created_date
       FROM view v
       LEFT JOIN participant p ON v.participant_id = p.id
       WHERE ${dateCondition}
       GROUP BY v.campaign_id, v.view_r
      ORDER BY v.campaign_id, v.view_r;
      `, "viewResults");

    const leadResults = await fetchData(`
      SELECT v.campaign_id, v.view_r AS link, COUNT(DISTINCT p.id) AS leads, MIN(p.created) AS created_date
      FROM view v
      JOIN participant p ON v.participant_id = p.id
      WHERE (p.telephone IS NOT NULL OR p.name IS NOT NULL OR p.email IS NOT NULL)
      AND  ${dateCondition}
      GROUP BY v.campaign_id, v.view_r
      ORDER BY v.campaign_id, v.view_r;
    `, "leadResults");

    const paidLeadResults = await fetchData(`
      SELECT v.campaign_id, v.view_r AS link, COUNT(DISTINCT p.id) AS paid_leads, MIN(p.created) AS created_date
      FROM view v
      LEFT JOIN participant p ON v.participant_id = p.id
      WHERE p.status = 'PAID'
      AND  ${dateCondition}
      GROUP BY v.campaign_id, v.view_r
      ORDER BY v.campaign_id, v.view_r;
    `, "paidLeadResults");

    const uniqueLeadsResults = await fetchData(`
      SELECT v.campaign_id, v.view_r AS link, COUNT(DISTINCT p.telephone) AS unique_leads, MIN(p.created) AS created_date
      FROM view v
      JOIN participant p ON v.participant_id = p.id
      WHERE p.status = 'PAID' AND p.telephone IS NOT NULL
      AND  ${dateCondition}
      GROUP BY v.campaign_id, v.view_r
      ORDER BY v.campaign_id, v.view_r;
    `, "uniqueLeadsResults");

    const recurringLeadsResults = await fetchData(`
      SELECT v.campaign_id, v.view_r AS link, COUNT(DISTINCT p.telephone) AS recuring_leads, MIN(p.created) AS created_date
      FROM view v
      JOIN participant p ON v.participant_id = p.id
      WHERE p.custom_text4 = 'ACTIVE' AND p.telephone IS NOT NULL
      AND  ${dateCondition}
      GROUP BY v.campaign_id, v.view_r
      ORDER BY v.campaign_id, v.view_r;
    `, "recurringLeadsResults");

    const smsPartsResults = await fetchData(`
      SELECT v.campaign_id, v.view_r AS link, 
      SUM(COALESCE(
        CASE 
          WHEN JSON_VALID(p.report_download_email) 
          THEN JSON_UNQUOTE(JSON_EXTRACT(p.report_download_email, '$.parts')) 
          ELSE 0 
        END, 0) 
      + COALESCE(p.sms_parts, 0)) AS sms_parts, MIN(p.created) AS created_date
      FROM view v
      JOIN participant p ON v.participant_id = p.id
      WHERE (p.report_download_email IS NOT NULL OR p.sms_parts IS NOT NULL)
      AND  ${dateCondition}
      GROUP BY v.campaign_id, v.view_r
      ORDER BY v.campaign_id, v.view_r;
    `, "smsPartsResults");

    const couponSentResults = await fetchData(`
      SELECT v.campaign_id, v.view_r AS link, COUNT(*) AS giftcards_sent, MIN(p.created) AS created_date
      FROM view v
      JOIN participant p ON v.participant_id = p.id
      WHERE p.coupon_send = 1
      AND  ${dateCondition}
      GROUP BY v.campaign_id, v.view_r
      ORDER BY v.campaign_id, v.view_r;
    `, "couponSentResults");

    const moneyReceivedResults = await fetchData(`
      SELECT v.campaign_id, v.view_r AS link, 
      COALESCE(SUM(p.amount), 0) AS money_received, MIN(p.created) AS created_date
      FROM view v
      JOIN participant p ON v.participant_id = p.id
      WHERE p.status = 'PAID' AND p.amount IS NOT NULL
      AND  ${dateCondition}
      GROUP BY v.campaign_id, v.view_r
      ORDER BY v.campaign_id, v.view_r;
    `, "moneyReceivedResults");

    const engagementTimeResults = await fetchData(`
      SELECT v.campaign_id, v.view_r AS link, 
      SEC_TO_TIME(AVG(p.time_spent)) AS engagement_time, MIN(p.created) AS created_date
      FROM view v
      JOIN participant p ON v.participant_id = p.id
      WHERE p.time_spent IS NOT NULL
      AND  ${dateCondition}
      GROUP BY v.campaign_id, v.view_r
      ORDER BY v.campaign_id, v.view_r;
    `, "engagementTimeResults");

    const transactionAbandonedResults = await fetchData(`
      SELECT v.campaign_id, v.view_r AS link, COUNT(*) AS abandoned_transactions, MIN(p.created) AS created_date
      FROM view v
      JOIN participant p ON v.participant_id = p.id
      WHERE p.status IN ('ERROR', 'DECLINED')
      AND  ${dateCondition}
      GROUP BY v.campaign_id, v.view_r
      ORDER BY v.campaign_id, v.view_r;
    `, "transactionAbandonedResults");

    const gamesFinishedResults = await fetchData(`
      SELECT v.campaign_id, v.view_r AS link, COUNT(*) AS games_finished, MIN(p.created) AS created_date
      FROM view v
      JOIN participant p ON v.participant_id = p.id
      WHERE p.points_scored IS NOT NULL 
      AND p.points_scored != ''
      AND  ${dateCondition}
      GROUP BY v.campaign_id, v.view_r
      ORDER BY v.campaign_id, v.view_r;
    `, "gamesFinishedResults");

    connection.release();

    return {
      viewResults,
      leadResults,
      paidLeadResults,
      uniqueLeadsResults,
      recurringLeadsResults,
      smsPartsResults,
      couponSentResults,
      moneyReceivedResults,
      engagementTimeResults,
      transactionAbandonedResults,
      gamesFinishedResults,
    };
  } catch (err) {
    console.error("❌ Fel vid hämtning av data:", err);
    return {
      viewResults: [],
      leadResults: [],
      paidLeadResults: [],
      uniqueLeadsResults: [],
      recurringLeadsResults: [],
      smsPartsResults: [],
      couponSentResults: [],
      moneyReceivedResults: [],
      engagementTimeResults: [],
      transactionAbandonedResults: [],
      gamesFinishedResults: [],
    };
  }
};

module.exports = { fetchAndProcessData };
