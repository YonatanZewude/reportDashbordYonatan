const pool = require("../db/connection");

const fetchAndProcessData = async () => {
  try {
    const connection = await pool.getConnection();

    //Hämta kampanjvisningar per länk (Action #1)
    const [viewResults] = await connection.execute(`
      SELECT v.campaign_id, v.view_r AS link, COUNT(*) AS views
      FROM db_practice.view v
      WHERE v.created >= NOW() - INTERVAL 1 DAY
      GROUP BY v.campaign_id, v.view_r
    `);

    //Hämta totalt antal visningar per kampanj (Action #2, #3)
    const [campaignViewResults] = await connection.execute(`
      SELECT campaign_id, COUNT(*) AS views
      FROM db_practice.view
      WHERE created >= NOW() - INTERVAL 1 DAY
      GROUP BY campaign_id
    `);

    //Hämta antal leads per kampanj (Action #2, #3)
    const [leadResults] = await connection.execute(`
      SELECT campaign_id, COUNT(*) AS leads
      FROM db_practice.participant
      WHERE telephone IS NOT NULL AND name IS NOT NULL AND email IS NOT NULL
      GROUP BY campaign_id
    `);

    //Hämta antal betalda leads per kampanj (Action #2, #3)
    const [paidLeadResults] = await connection.execute(`
      SELECT campaign_id, COUNT(*) AS paid_leads
      FROM db_practice.participant
      WHERE status = 'PAID'
      GROUP BY campaign_id
    `);

    //Hämta unika leads där status = 'PAID' (unique_leads)
    const [uniqueLeadResults] = await connection.execute(`
      SELECT campaign_id, COUNT(DISTINCT telephone) AS unique_leads
      FROM db_practice.participant
      WHERE status = 'PAID'
      GROUP BY campaign_id
    `);

    //Hämta **unika** leads där custom_text4 = 'ACTIVE' (recuring_leads)
    const [recuringLeadResults] = await connection.execute(`
      SELECT campaign_id, COUNT(DISTINCT telephone) AS recuring_leads
      FROM db_practice.participant
      WHERE custom_text4 = 'ACTIVE'
      GROUP BY campaign_id
    `);

    //Räkna konverteringsgrad (conversion_rate) → (leads / views) * 100
    const [conversionRateResults] = await connection.execute(`
      SELECT v.campaign_id, 
             (COUNT(DISTINCT p.id) / NULLIF(COUNT(v.id), 0)) * 100 AS conversion_rate
      FROM db_practice.view v
      LEFT JOIN db_practice.participant p ON v.campaign_id = p.campaign_id
      GROUP BY v.campaign_id
    `);

    //Hämta och summera SMS-delar (sms_parts)
    const [smsPartsResults] = await connection.execute(`
      SELECT campaign_id, 
             SUM(
               COALESCE(
                 CASE 
                   WHEN JSON_VALID(report_download_email) 
                   THEN JSON_UNQUOTE(JSON_EXTRACT(report_download_email, '$.parts')) 
                   ELSE 0 
                 END, 0
               ) + COALESCE(sms_parts, 0)
             ) AS sms_parts
      FROM db_practice.participant
      GROUP BY campaign_id
    `);

    connection.release();

    return {
      viewResults,
      campaignViewResults,
      leadResults,
      paidLeadResults,
      uniqueLeadResults,
      recuringLeadResults,
      conversionRateResults,
      smsPartsResults
    };
  } catch (err) {
    console.error("❌ Fel vid hämtning av data:", err);
  }
};

module.exports = { fetchAndProcessData };
