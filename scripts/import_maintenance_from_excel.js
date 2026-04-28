const path = require("path");
const ExcelJS = require("exceljs");
const db = require("../config/db");

const dbPromise = db.promise();

const DEFAULT_EXCEL_PATH =
  "C:\\Users\\Aditya Baghel\\Downloads\\vehicle_maintenance_schedule.xlsx";

function normalizeLabel(value) {
  return (value || "")
    .toString()
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeMetric(value) {
  const text = (value || "").toString().trim().toUpperCase();
  if (!text) return null;
  if (text.includes("KM") || text.includes("KILOMETER")) return "KM";
  if (text.includes("PTO")) return "PTO_HOUR_METER";
  if (text.includes("BACK ENGINE") || text.includes("REAR ENGINE")) {
    return "BACK_ENGINE_HOUR_METER";
  }
  if (text.includes("HOUR")) return "HOUR_METER";
  return null;
}

function parseInterval(raw) {
  if (raw === null || raw === undefined) return null;
  const text = raw.toString().trim();
  if (!text) return null;
  const match = text.match(/-?\d+(\.\d+)?/);
  if (!match) return null;
  const value = Number(match[0]);
  return Number.isNaN(value) || value <= 0 ? null : value;
}

function resolveVehicleTypeId(rawName, vehicleTypesByUpper, vehicleTypesByNorm) {
  const upper = (rawName || "").toString().trim().toUpperCase();
  if (!upper) return null;

  const aliases = {
    "HYDRAULI CRANE": "HYDRAULIC MOBILE CRANE",
    "HYDRAULIC CRANE": "HYDRAULIC MOBILE CRANE",
    "D G": "DG SET",
    "D.G": "DG SET",
    "DG": "DG SET",
    "FARANA": "FARANA MOBILE CRANE",
  };

  const aliasTarget = aliases[upper] || aliases[normalizeLabel(upper).toUpperCase()];
  if (aliasTarget && vehicleTypesByUpper[aliasTarget]) {
    return vehicleTypesByUpper[aliasTarget].id;
  }

  if (vehicleTypesByUpper[upper]) return vehicleTypesByUpper[upper].id;

  const norm = normalizeLabel(upper);
  if (vehicleTypesByNorm[norm]) return vehicleTypesByNorm[norm].id;

  const candidates = Object.entries(vehicleTypesByNorm).filter(
    ([k]) => k.includes(norm) || norm.includes(k)
  );
  if (candidates.length === 1) return candidates[0][1].id;

  return null;
}

async function ensureTable() {
  await dbPromise.query(`
    CREATE TABLE IF NOT EXISTS maintenance_rules (
      id INT PRIMARY KEY AUTO_INCREMENT,
      vehicle_id INT NULL,
      vehicle_type_id INT NULL,
      parameter_name VARCHAR(150) NOT NULL,
      trigger_metric ENUM('KM', 'HOUR_METER', 'PTO_HOUR_METER', 'BACK_ENGINE_HOUR_METER') NOT NULL,
      interval_value DECIMAL(12,2) NOT NULL,
      last_service_value DECIMAL(12,2) DEFAULT 0,
      is_active TINYINT(1) DEFAULT 1,
      notes TEXT NULL,
      created_by INT NULL,
      created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      INDEX idx_maintenance_vehicle (vehicle_id),
      INDEX idx_maintenance_vehicle_type (vehicle_type_id),
      INDEX idx_maintenance_metric (trigger_metric),
      INDEX idx_maintenance_active (is_active)
    )
  `);

  const [columns] = await dbPromise.query("SHOW COLUMNS FROM maintenance_rules");
  const existing = new Set(columns.map((c) => c.Field));

  const alterStatements = [];

  if (!existing.has("vehicle_id")) {
    alterStatements.push("ADD COLUMN vehicle_id INT NULL AFTER id");
  }
  if (!existing.has("vehicle_type_id")) {
    alterStatements.push("ADD COLUMN vehicle_type_id INT NULL AFTER vehicle_id");
  }
  if (!existing.has("parameter_name") && existing.has("service_type")) {
    alterStatements.push("ADD COLUMN parameter_name VARCHAR(150) NULL AFTER vehicle_type_id");
  }
  if (!existing.has("trigger_metric")) {
    alterStatements.push(
      "ADD COLUMN trigger_metric ENUM('KM', 'HOUR_METER', 'PTO_HOUR_METER', 'BACK_ENGINE_HOUR_METER') NULL AFTER parameter_name"
    );
  }
  if (!existing.has("interval_value")) {
    alterStatements.push("ADD COLUMN interval_value DECIMAL(12,2) NULL AFTER trigger_metric");
  }
  if (!existing.has("last_service_value")) {
    alterStatements.push("ADD COLUMN last_service_value DECIMAL(12,2) DEFAULT 0 AFTER interval_value");
  }
  if (!existing.has("is_active")) {
    alterStatements.push("ADD COLUMN is_active TINYINT(1) DEFAULT 1 AFTER last_service_value");
  }
  if (!existing.has("notes")) {
    alterStatements.push("ADD COLUMN notes TEXT NULL AFTER is_active");
  }
  if (!existing.has("created_by")) {
    alterStatements.push("ADD COLUMN created_by INT NULL AFTER notes");
  }
  if (!existing.has("created_at")) {
    alterStatements.push("ADD COLUMN created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP AFTER created_by");
  }
  if (!existing.has("updated_at")) {
    alterStatements.push(
      "ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP AFTER created_at"
    );
  }

  if (alterStatements.length > 0) {
    await dbPromise.query(
      `ALTER TABLE maintenance_rules ${alterStatements.join(", ")}`
    );
  }

  // Backfill / normalize old-schema data if present.
  if (existing.has("service_type")) {
    await dbPromise.query(
      `UPDATE maintenance_rules
       SET parameter_name = COALESCE(parameter_name, service_type, 'General Maintenance')
       WHERE parameter_name IS NULL`
    );
  } else {
    await dbPromise.query(
      `UPDATE maintenance_rules
       SET parameter_name = COALESCE(parameter_name, 'General Maintenance')
       WHERE parameter_name IS NULL`
    );
  }

  await dbPromise.query(
    `UPDATE maintenance_rules
     SET trigger_metric = COALESCE(trigger_metric, 'HOUR_METER')
     WHERE trigger_metric IS NULL`
  );

  if (existing.has("threshold_value")) {
    await dbPromise.query(
      `UPDATE maintenance_rules
       SET interval_value = COALESCE(interval_value, threshold_value, 0)
       WHERE interval_value IS NULL`
    );
  } else {
    await dbPromise.query(
      `UPDATE maintenance_rules
       SET interval_value = COALESCE(interval_value, 0)
       WHERE interval_value IS NULL`
    );
  }

  await dbPromise.query(
    `UPDATE maintenance_rules
     SET last_service_value = COALESCE(last_service_value, 0),
         is_active = COALESCE(is_active, 1)
     WHERE last_service_value IS NULL OR is_active IS NULL`
  );
}

async function run() {
  const excelPath = process.argv[2] || DEFAULT_EXCEL_PATH;
  const workbook = new ExcelJS.Workbook();

  console.log(`Reading Excel: ${excelPath}`);
  await workbook.xlsx.readFile(excelPath);

  const worksheet = workbook.worksheets[0];
  if (!worksheet) {
    throw new Error("No worksheet found");
  }

  await ensureTable();

  const [vehicleTypes] = await dbPromise.query("SELECT id, name FROM vehicle_types");
  const vehicleTypesByUpper = {};
  const vehicleTypesByNorm = {};
  for (const vt of vehicleTypes) {
    const upper = (vt.name || "").toString().trim().toUpperCase();
    vehicleTypesByUpper[upper] = vt;
    vehicleTypesByNorm[normalizeLabel(upper)] = vt;
  }

  const headerRow = worksheet.getRow(1);
  const colConfigs = [];

  for (let col = 2; col <= headerRow.cellCount; col += 1) {
    const headerText = (headerRow.getCell(col).value || "").toString().trim();
    if (!headerText) continue;

    const metricMatch = headerText.match(/\(([^)]+)\)/);
    const metric = normalizeMetric(metricMatch ? metricMatch[1] : "");
    const vehicleTypeLabel = headerText.replace(/\(.*?\)/g, "").trim();
    const vehicleTypeId = resolveVehicleTypeId(
      vehicleTypeLabel,
      vehicleTypesByUpper,
      vehicleTypesByNorm
    );

    if (!vehicleTypeId) {
      console.log(`SKIP COLUMN ${col}: vehicle type not found for "${headerText}"`);
      continue;
    }

    if (!metric) {
      console.log(`SKIP COLUMN ${col}: metric not found in "${headerText}"`);
      continue;
    }

    colConfigs.push({
      col,
      headerText,
      vehicleTypeId,
      metric,
    });
  }

  if (colConfigs.length === 0) {
    throw new Error("No valid vehicle type columns found");
  }

  let inserted = 0;
  let updated = 0;
  let skipped = 0;
  const importedKeys = new Set();
  const importedVehicleTypeIds = new Set();

  await dbPromise.query("START TRANSACTION");
  try {
    for (let rowNum = 2; rowNum <= worksheet.rowCount; rowNum += 1) {
      const row = worksheet.getRow(rowNum);
      const parameterName = (row.getCell(1).value || "").toString().trim();
      if (!parameterName) continue;

      for (const cfg of colConfigs) {
        const raw = row.getCell(cfg.col).value;
        const intervalValue = parseInterval(raw);
        if (!intervalValue) {
          skipped += 1;
          continue;
        }

        importedVehicleTypeIds.add(cfg.vehicleTypeId);
        const key = `${cfg.vehicleTypeId}||${parameterName.toUpperCase()}||${cfg.metric}`;
        importedKeys.add(key);

        const [existing] = await dbPromise.query(
          `SELECT id
           FROM maintenance_rules
           WHERE vehicle_id IS NULL
             AND vehicle_type_id = ?
             AND parameter_name = ?
             AND trigger_metric = ?
           LIMIT 1`,
          [cfg.vehicleTypeId, parameterName, cfg.metric]
        );

        if (existing.length > 0) {
          await dbPromise.query(
            `UPDATE maintenance_rules
             SET interval_value = ?, is_active = 1
             WHERE id = ?`,
            [intervalValue, existing[0].id]
          );
          updated += 1;
        } else {
          await dbPromise.query(
            `INSERT INTO maintenance_rules
             (vehicle_id, vehicle_type_id, parameter_name, trigger_metric, interval_value, last_service_value, is_active)
             VALUES (NULL, ?, ?, ?, ?, 0, 1)`,
            [cfg.vehicleTypeId, parameterName, cfg.metric, intervalValue]
          );
          inserted += 1;
        }
      }
    }

    if (importedVehicleTypeIds.size > 0) {
      const ids = [...importedVehicleTypeIds];
      const [existingRows] = await dbPromise.query(
        `SELECT id, vehicle_type_id, parameter_name, trigger_metric
         FROM maintenance_rules
         WHERE vehicle_id IS NULL
           AND vehicle_type_id IN (?)`,
        [ids]
      );

      const deleteIds = [];
      for (const row of existingRows) {
        const key = `${row.vehicle_type_id}||${(row.parameter_name || "")
          .toString()
          .toUpperCase()}||${row.trigger_metric}`;
        if (!importedKeys.has(key)) {
          deleteIds.push(row.id);
        }
      }

      if (deleteIds.length > 0) {
        await dbPromise.query(
          "DELETE FROM maintenance_rules WHERE id IN (?)",
          [deleteIds]
        );
      }
    }

    await dbPromise.query("COMMIT");
  } catch (err) {
    await dbPromise.query("ROLLBACK");
    throw err;
  }

  console.log("Import complete");
  console.log(`Inserted: ${inserted}`);
  console.log(`Updated: ${updated}`);
  console.log(`Skipped blanks/invalid: ${skipped}`);
}

run()
  .then(() => process.exit(0))
  .catch((err) => {
    console.error("Import failed:", err.message || err);
    process.exit(1);
  });
