const db = require("../config/db");

const dbPromise = db.promise();

async function run() {
  const [rows] = await dbPromise.query(
    `SELECT
       vt.name AS vehicle_type,
       r.parameter_name,
       r.trigger_metric,
       r.interval_value
     FROM maintenance_rules r
     LEFT JOIN vehicle_types vt ON vt.id = r.vehicle_type_id
     WHERE r.vehicle_id IS NULL
       AND r.is_active = 1
     ORDER BY vt.name ASC, r.parameter_name ASC`
  );

  console.log(`Total rules: ${rows.length}`);
  let currentType = null;

  for (const row of rows) {
    const vt = (row.vehicle_type || "UNKNOWN").toString();
    if (vt !== currentType) {
      currentType = vt;
      console.log(`\n=== ${currentType} ===`);
    }

    const interval = Number(row.interval_value || 0);
    const unit = row.trigger_metric === "KM" ? "KM" : "Hours";
    console.log(
      `- ${row.parameter_name}: every ${interval} ${unit} (${row.trigger_metric})`
    );
  }
}

run()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error("Failed to generate maintenance rule report:", err);
    process.exit(1);
  });
