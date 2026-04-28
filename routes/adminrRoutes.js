const express = require("express");
const router = express.Router();
const db = require("../config/db");
const authMiddleware = require("../middleware/authMiddleware");
const ExcelJS = require("exceljs");
const PDFDocument = require("pdfkit");
const multer = require("multer");

const dbPromise = db.promise();
const upload = multer({ storage: multer.memoryStorage() });

const ensureServiceSchedulesTableQuery = `
CREATE TABLE IF NOT EXISTS service_schedules (
  id INT PRIMARY KEY AUTO_INCREMENT,
  vehicle_id INT NOT NULL,
  service_type VARCHAR(120) NOT NULL,
  scheduled_date DATE NOT NULL,
  status ENUM('SCHEDULED', 'COMPLETED', 'CANCELLED', 'OVERDUE') DEFAULT 'SCHEDULED',
  notes TEXT NULL,
  created_by INT NULL,
  completed_at DATETIME NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  INDEX idx_service_vehicle (vehicle_id),
  INDEX idx_service_date (scheduled_date),
  INDEX idx_service_status (status)
)
`;

db.query(ensureServiceSchedulesTableQuery, (err) => {
  if (err) {
    console.error("Failed to ensure service_schedules table:", err);
  }
});

const ensureMaintenanceRulesTableQuery = `
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
`;

const ensureMaintenanceRecordsTableQuery = `
CREATE TABLE IF NOT EXISTS maintenance_records (
  id INT PRIMARY KEY AUTO_INCREMENT,
  maintenance_rule_id INT NOT NULL,
  vehicle_id INT NOT NULL,
  performed_value DECIMAL(12,2) NOT NULL,
  notes TEXT NULL,
  created_by INT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_maintenance_record_rule (maintenance_rule_id),
  INDEX idx_maintenance_record_vehicle (vehicle_id)
)
`;

db.query(ensureMaintenanceRulesTableQuery, (err) => {
  if (err) {
    console.error("Failed to ensure maintenance_rules table:", err);
  }
});

db.query(ensureMaintenanceRecordsTableQuery, (err) => {
  if (err) {
    console.error("Failed to ensure maintenance_records table:", err);
  }
});

const METRIC_PATTERNS = {
  KM: ["km", "kilometer", "odometer", "distance"],
  HOUR_METER: ["hour meter", "hourmeter", "engine hour", "engine hours"],
  PTO_HOUR_METER: ["pto", "p.t.o", "pto hour"],
  BACK_ENGINE_HOUR_METER: ["back engine", "back-engine", "rear engine"],
};

function normalizeMetric(metric) {
  const value = (metric || "").toString().trim().toUpperCase();

  if (value === "KM" || value === "KMS" || value === "ODOMETER") return "KM";
  if (value === "HOUR" || value === "HOUR_METER" || value === "HOURMETER") {
    return "HOUR_METER";
  }
  if (
    value === "PTO_HOUR_METER" ||
    value === "PTO" ||
    value === "P.T.O" ||
    value === "PTO_HOUR"
  ) {
    return "PTO_HOUR_METER";
  }
  if (
    value === "BACK_ENGINE_HOUR_METER" ||
    value === "BACK_ENGINE" ||
    value === "BACK_ENGINE_HOUR" ||
    value === "REAR_ENGINE_HOUR_METER"
  ) {
    return "BACK_ENGINE_HOUR_METER";
  }

  return null;
}

function inferMetricFromReadingName(name) {
  const value = (name || "").toString().toLowerCase();

  if (METRIC_PATTERNS.PTO_HOUR_METER.some((p) => value.includes(p))) {
    return "PTO_HOUR_METER";
  }

  if (METRIC_PATTERNS.BACK_ENGINE_HOUR_METER.some((p) => value.includes(p))) {
    return "BACK_ENGINE_HOUR_METER";
  }

  if (METRIC_PATTERNS.HOUR_METER.some((p) => value.includes(p))) {
    return "HOUR_METER";
  }

  if (METRIC_PATTERNS.KM.some((p) => value.includes(p))) {
    return "KM";
  }

  return null;
}

function normalizeLabel(value) {
  return (value || "")
    .toString()
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function inferMetricFromText(value) {
  const text = (value || "").toString().toLowerCase();
  if (text.includes("km") || text.includes("kilometer")) return "KM";
  if (text.includes("pto")) return "PTO_HOUR_METER";
  if (text.includes("back engine") || text.includes("rear engine")) {
    return "BACK_ENGINE_HOUR_METER";
  }
  if (text.includes("hour")) return "HOUR_METER";
  return null;
}

function resolveVehicleTypeId(vehicleTypeRaw, vehicleTypeByName, vehicleTypeByNormalizedName) {
  const upperRaw = (vehicleTypeRaw || "").toString().trim().toUpperCase();
  if (!upperRaw) return null;

  if (vehicleTypeByName[upperRaw]) {
    return vehicleTypeByName[upperRaw].id;
  }

  const normalized = normalizeLabel(upperRaw);
  if (vehicleTypeByNormalizedName[normalized]) {
    return vehicleTypeByNormalizedName[normalized].id;
  }

  // Fuzzy fallback to handle typos like "Hydrauli crane" vs "Hydraulic Crane".
  const candidates = Object.entries(vehicleTypeByNormalizedName).filter(
    ([key]) => key.includes(normalized) || normalized.includes(key)
  );

  if (candidates.length === 1) {
    return candidates[0][1].id;
  }

  return null;
}

async function getLatestMetersForVehicles(vehicleIds) {
  const meterMap = {};
  if (!vehicleIds || vehicleIds.length === 0) return meterMap;

  const [rows] = await dbPromise.query(
    `SELECT
       t.vehicle_id,
       rt.name AS reading_name,
       COALESCE(tr.end_value, tr.start_value) AS reading_value,
       t.created_at,
       tr.id AS trip_reading_id
     FROM trips t
     JOIN trip_readings tr ON tr.trip_id = t.id
     JOIN reading_types rt ON rt.id = tr.reading_type_id
     WHERE t.vehicle_id IN (?)
       AND COALESCE(tr.end_value, tr.start_value) IS NOT NULL
     ORDER BY t.created_at DESC, tr.id DESC`,
    [vehicleIds]
  );

  for (const row of rows) {
    const vehicleId = Number(row.vehicle_id);
    const metric = inferMetricFromReadingName(row.reading_name);
    const readingValue = Number(row.reading_value);

    if (!metric || Number.isNaN(readingValue)) continue;
    if (!meterMap[vehicleId]) meterMap[vehicleId] = {};
    if (meterMap[vehicleId][metric] !== undefined) continue;

    meterMap[vehicleId][metric] = readingValue;
  }

  return meterMap;
}

async function getResolvedMaintenanceRows({ vehicleId = null, dueOnly = false } = {}) {
  const where = ["r.is_active = 1", "v.status = 'ACTIVE'"];
  const values = [];

  if (vehicleId) {
    where.push("v.id = ?");
    values.push(vehicleId);
  }

  const [rules] = await dbPromise.query(
    `SELECT
       r.id,
       r.vehicle_id AS rule_vehicle_id,
       r.vehicle_type_id AS rule_vehicle_type_id,
       r.parameter_name,
       r.trigger_metric,
       r.interval_value,
       r.last_service_value,
       r.notes,
       v.id AS vehicle_id,
       v.vehicle_number,
       v.equipment_name,
       vt.name AS vehicle_type_name
     FROM maintenance_rules r
     JOIN vehicles v
       ON (
            (r.vehicle_id IS NOT NULL AND r.vehicle_id = v.id)
            OR
            (r.vehicle_id IS NULL AND r.vehicle_type_id IS NOT NULL AND r.vehicle_type_id = v.vehicle_type_id)
          )
     LEFT JOIN vehicle_types vt ON vt.id = v.vehicle_type_id
     WHERE ${where.join(" AND ")}
     ORDER BY v.vehicle_number ASC, r.parameter_name ASC`,
    values
  );

  if (rules.length === 0) return [];

  const vehicleIds = [...new Set(rules.map((r) => Number(r.vehicle_id)))];
  const meterMap = await getLatestMetersForVehicles(vehicleIds);

  const resolved = rules.map((row) => {
    const currentValue =
      meterMap[Number(row.vehicle_id)]?.[row.trigger_metric] ?? null;
    const baseValue = Number(row.last_service_value || 0);
    const intervalValue = Number(row.interval_value || 0);
    const dueValue = intervalValue > 0 ? baseValue + intervalValue : null;
    const isDue =
      dueValue !== null && currentValue !== null ? currentValue >= dueValue : false;
    const remainingValue =
      dueValue !== null && currentValue !== null ? dueValue - currentValue : null;

    return {
      ...row,
      interval_value: intervalValue,
      last_service_value: baseValue,
      current_value: currentValue,
      due_value: dueValue,
      remaining_value: remainingValue,
      due_status:
        dueValue === null || currentValue === null
          ? "UNKNOWN"
          : isDue
          ? "DUE"
          : "UPCOMING",
    };
  });

  if (!dueOnly) return resolved;
  return resolved.filter((row) => row.due_status === "DUE");
}


// =============================
// DASHBOARD STATS
// =============================
router.get("/dashboard-stats", authMiddleware, (req, res) => {

  if (req.user.role !== "ADMIN") {
    return res.status(403).json({ message: "Unauthorized" });
  }

  const query = `
    SELECT
      (SELECT COUNT(*) FROM vehicles) AS totalVehicles,
      (SELECT COUNT(*) FROM trips WHERE trip_status = 'STARTED') AS activeTrips,
      (SELECT COUNT(*) FROM vehicles WHERE status = 'SERVICE_DUE') AS serviceDue,
      (
        SELECT COUNT(*)
        FROM trip_refuels
        WHERE created_at >= CURDATE()
          AND created_at < CURDATE() + INTERVAL 1 DAY
      ) AS refuelToday
  `;

  db.query(query, (err, rows) => {
    if (err) {
      return res.status(500).json({ message: "Unable to load dashboard stats" });
    }

    const stats = rows[0] || {};
    return res.json({
      totalVehicles: Number(stats.totalVehicles || 0),
      activeTrips: Number(stats.activeTrips || 0),
      serviceDue: Number(stats.serviceDue || 0),
      refuelToday: Number(stats.refuelToday || 0),
    });
  });

});


// =============================
// VEHICLE MONITORING
// =============================
router.get("/vehicle-types", authMiddleware, (req, res) => {
  if (req.user.role !== "ADMIN") {
    return res.status(403).json({ message: "Unauthorized" });
  }

  db.query(
    "SELECT id, name FROM vehicle_types ORDER BY name ASC",
    (err, results) => {
      if (err) return res.status(500).json(err);
      return res.json({ vehicle_types: results });
    }
  );
});

router.post("/vehicles", authMiddleware, (req, res) => {
  if (req.user.role !== "ADMIN") {
    return res.status(403).json({ message: "Unauthorized" });
  }

  const vehicleNumber = (req.body?.vehicle_number || "").trim();
  const vehicleTypeId = Number(req.body?.vehicle_type_id);

  if (!vehicleNumber || Number.isNaN(vehicleTypeId) || vehicleTypeId <= 0) {
    return res.status(400).json({
      message: "vehicle_number and valid vehicle_type_id are required",
    });
  }

      db.query(
        "SELECT id, name FROM vehicle_types WHERE id = ? LIMIT 1",
        [vehicleTypeId],
        (typeErr, typeRows) => {
          if (typeErr) return res.status(500).json(typeErr);
          if (typeRows.length === 0) {
            return res.status(404).json({ message: "Vehicle type not found" });
          }
          const vehicleTypeName = (typeRows[0].name || "").toString().trim();

      db.query(
        "SELECT id FROM vehicles WHERE vehicle_number = ? LIMIT 1",
        [vehicleNumber],
        (checkErr, existingRows) => {
          if (checkErr) return res.status(500).json(checkErr);
          if (existingRows.length > 0) {
            return res
              .status(409)
              .json({ message: "Vehicle number already exists" });
          }

          db.query(
            `INSERT INTO vehicles (vehicle_number, equipment_name, vehicle_type_id, status)
             VALUES (?, ?, ?, 'ACTIVE')`,
            [vehicleNumber, vehicleTypeName, vehicleTypeId],
            (insertErr, insertResult) => {
              if (insertErr) return res.status(500).json(insertErr);
              return res.status(201).json({
                message: "Vehicle created successfully",
                vehicle_id: insertResult.insertId,
              });
            }
          );
        }
      );
    }
  );
});

router.delete("/vehicles/:id", authMiddleware, (req, res) => {
  if (req.user.role !== "ADMIN") {
    return res.status(403).json({ message: "Unauthorized" });
  }

  const vehicleId = Number(req.params.id);
  if (Number.isNaN(vehicleId) || vehicleId <= 0) {
    return res.status(400).json({ message: "Invalid vehicle id" });
  }

  db.query(
    "SELECT id FROM vehicles WHERE id = ? LIMIT 1",
    [vehicleId],
    (findErr, rows) => {
      if (findErr) return res.status(500).json(findErr);
      if (rows.length === 0) {
        return res.status(404).json({ message: "Vehicle not found" });
      }

      db.query(
        "SELECT id FROM trips WHERE vehicle_id = ? LIMIT 1",
        [vehicleId],
        (tripErr, tripRows) => {
          if (tripErr) return res.status(500).json(tripErr);
          if (tripRows.length > 0) {
            return res.status(409).json({
              message:
                "Vehicle has trip history and cannot be deleted. Mark it inactive instead.",
            });
          }

          db.query(
            "DELETE FROM vehicles WHERE id = ?",
            [vehicleId],
            (deleteErr) => {
              if (deleteErr) return res.status(500).json(deleteErr);
              return res.json({ message: "Vehicle deleted successfully" });
            }
          );
        }
      );
    }
  );
});

router.get("/vehicles", authMiddleware, (req, res) => {

  if (req.user.role !== "ADMIN") {
    return res.status(403).json({ message: "Unauthorized" });
  }

  const query = `
  SELECT 
    v.id,
    v.vehicle_number,
    v.equipment_name,
    vt.name AS vehicle_type,
    v.status,

    t.trip_status,
    d.name AS driver_name

  FROM vehicles v

  LEFT JOIN vehicle_types vt 
    ON v.vehicle_type_id = vt.id

  LEFT JOIN trips t 
    ON t.vehicle_id = v.id AND t.trip_status = 'STARTED'

  LEFT JOIN drivers d
    ON d.id = t.driver_id

  ORDER BY v.vehicle_number ASC
  `;

  db.query(query, (err, results) => {

    if (err) {
      return res.status(500).json(err);
    }

    res.json({
      vehicles: results
    });

  });

});
router.get("/vehicle/:id", authMiddleware, (req, res) => {

  const vehicleId = req.params.id;

  const query = `
  SELECT 
    v.vehicle_number,
    v.equipment_name,
    vt.name AS vehicle_type,
    v.status
  FROM vehicles v
  LEFT JOIN vehicle_types vt
    ON v.vehicle_type_id = vt.id
  WHERE v.id = ?
  `;

  db.query(query, [vehicleId], (err, results) => {

    if (err) return res.status(500).json(err);

    res.json(results[0]);

  });

});
// =============================
// ACTIVE TRIPS MONITOR
// =============================
router.get("/active-trips", authMiddleware, (req, res) => {

  if (req.user.role !== "ADMIN") {
    return res.status(403).json({ message: "Unauthorized" });
  }

  const query = `
  SELECT 
    t.id AS trip_id,
    v.vehicle_number,
    v.equipment_name,
    d.name AS driver_name,
    t.trip_date,
    t.trip_status,
    t.created_at
  FROM trips t

  LEFT JOIN vehicles v
    ON t.vehicle_id = v.id

  LEFT JOIN drivers d
    ON t.driver_id = d.id

  WHERE t.trip_status = 'STARTED'

  ORDER BY t.created_at DESC
  `;

  db.query(query, (err, results) => {

    if (err) {
      return res.status(500).json(err);
    }

    res.json({
      trips: results
    });

  });

});

function toReadingNumber(value) {
  if (value === null || value === undefined) return null;
  const parsed = Number(value);
  return Number.isNaN(parsed) ? null : parsed;
}

function fetchCompletedTripsWithReadings(filters, callback) {
  const { date_from, date_to, vehicle } = filters || {};
  const where = ["t.trip_status = 'COMPLETED'"];
  const values = [];

  if (date_from) {
    where.push("DATE(COALESCE(t.completed_at, t.created_at)) >= ?");
    values.push(date_from);
  }
  if (date_to) {
    where.push("DATE(COALESCE(t.completed_at, t.created_at)) <= ?");
    values.push(date_to);
  }
  if (vehicle) {
    where.push("v.vehicle_number LIKE ?");
    values.push(`%${vehicle}%`);
  }

  const whereClause = `WHERE ${where.join(" AND ")}`;
  const tripQuery = `
  SELECT
    t.id AS trip_id,
    t.vehicle_id,
    v.vehicle_number,
    v.equipment_name,
    d.name AS driver_name,
    t.trip_date,
    t.trip_status,
    t.created_at,
    t.completed_at
  FROM trips t
  LEFT JOIN vehicles v
    ON t.vehicle_id = v.id
  LEFT JOIN drivers d
    ON t.driver_id = d.id
  ${whereClause}
  ORDER BY COALESCE(t.completed_at, t.created_at) DESC
  `;

  db.query(tripQuery, values, (err, trips) => {
    if (err) return callback(err);

    if (!trips || trips.length === 0) {
      return callback(null, []);
    }

    const tripIds = trips.map((trip) => trip.trip_id);

    db.query(
      `SELECT
         tr.trip_id,
         tr.reading_type_id,
         rt.name,
         rt.unit,
         tr.start_value,
         tr.end_value
       FROM trip_readings tr
       JOIN reading_types rt
         ON rt.id = tr.reading_type_id
       WHERE tr.trip_id IN (?)
       ORDER BY tr.trip_id DESC, rt.name ASC`,
      [tripIds],
      (readingErr, readingRows) => {
        if (readingErr) return callback(readingErr);

        const readingMap = {};

        for (const row of readingRows) {
          const startValue = toReadingNumber(row.start_value);
          const endValue = toReadingNumber(row.end_value);
          const differenceValue =
            startValue !== null && endValue !== null
              ? endValue - startValue
              : null;

          if (!readingMap[row.trip_id]) {
            readingMap[row.trip_id] = [];
          }

          readingMap[row.trip_id].push({
            reading_type_id: row.reading_type_id,
            name: row.name,
            unit: row.unit,
            start_value: startValue,
            end_value: endValue,
            difference_value: differenceValue
          });
        }

        const completedTrips = trips.map((trip) => ({
          ...trip,
          readings: readingMap[trip.trip_id] || []
        }));

        return callback(null, completedTrips);
      }
    );
  });
}

router.get("/completed-trips", authMiddleware, (req, res) => {
  if (req.user.role !== "ADMIN") {
    return res.status(403).json({ message: "Unauthorized" });
  }

  fetchCompletedTripsWithReadings(req.query, (err, trips) => {
    if (err) {
      return res.status(500).json(err);
    }

    return res.json({ trips });
  });
});

// =============================
// LIVE VEHICLE LOCATIONS
// =============================
router.get("/vehicle-locations", authMiddleware, (req, res) => {

  if (req.user.role !== "ADMIN") {
    return res.status(403).json({ message: "Unauthorized" });
  }

  const query = `
  SELECT
    v.id AS vehicle_id,
    v.vehicle_number,
    v.equipment_name,
    d.name AS driver_name,
    l.trip_id,
    l.latitude,
    l.longitude,
    l.created_at
  FROM vehicles v

  LEFT JOIN (
    SELECT l1.vehicle_id, l1.trip_id, l1.latitude, l1.longitude, l1.created_at
    FROM vehicle_locations l1
    INNER JOIN (
      SELECT vehicle_id, MAX(created_at) AS max_created_at
      FROM vehicle_locations
      GROUP BY vehicle_id
    ) latest
      ON latest.vehicle_id = l1.vehicle_id
     AND latest.max_created_at = l1.created_at
  ) l
    ON l.vehicle_id = v.id

  LEFT JOIN trips t
    ON t.id = l.trip_id

  LEFT JOIN drivers d
    ON d.id = t.driver_id

  WHERE l.latitude IS NOT NULL
    AND l.longitude IS NOT NULL

  ORDER BY l.created_at DESC
  `;

  db.query(query, (err, results) => {
    if (err) {
      if (err.code === "ER_NO_SUCH_TABLE") {
        return res.status(500).json({
          message: "Table vehicle_locations not found. Please create it before using live map."
        });
      }

      return res.status(500).json(err);
    }

    res.json({
      vehicle_locations: results
    });
  });
});

// =============================
// REFUEL LOGS
// =============================
router.get("/refuel-logs", authMiddleware, (req, res) => {

  if (req.user.role !== "ADMIN") {
    return res.status(403).json({ message: "Unauthorized" });
  }

  const { date_from, date_to, vehicle, driver } = req.query;
  const where = [];
  const values = [];

  if (date_from) {
    where.push("DATE(r.created_at) >= ?");
    values.push(date_from);
  }

  if (date_to) {
    where.push("DATE(r.created_at) <= ?");
    values.push(date_to);
  }

  if (vehicle) {
    where.push("v.vehicle_number LIKE ?");
    values.push(`%${vehicle}%`);
  }

  if (driver) {
    where.push("d.name LIKE ?");
    values.push(`%${driver}%`);
  }

  const whereClause = where.length > 0
    ? `WHERE ${where.join(" AND ")}`
    : "";

  const logsQuery = `
  SELECT
    r.id,
    r.trip_id,
    r.litre,
    r.photo,
    r.created_at,
    v.vehicle_number,
    d.name AS driver_name
  FROM trip_refuels r
  LEFT JOIN trips t
    ON t.id = r.trip_id
  LEFT JOIN vehicles v
    ON v.id = t.vehicle_id
  LEFT JOIN drivers d
    ON d.id = t.driver_id
  ${whereClause}
  ORDER BY r.created_at DESC
  `;

  const summaryQuery = `
  SELECT
    COALESCE(SUM(CAST(r.litre AS DECIMAL(10,2))), 0) AS total_litres,
    COUNT(*) AS total_logs
  FROM trip_refuels r
  LEFT JOIN trips t
    ON t.id = r.trip_id
  LEFT JOIN vehicles v
    ON v.id = t.vehicle_id
  LEFT JOIN drivers d
    ON d.id = t.driver_id
  ${whereClause}
  `;

  db.query(logsQuery, values, (err, results) => {
    if (err) {
      return res.status(500).json(err);
    }

    db.query(summaryQuery, values, (err2, summaryResults) => {
      if (err2) {
        return res.status(500).json(err2);
      }

      const summary = summaryResults[0] || {};

      res.json({
        refuels: results,
        summary: {
          totalLitres: Number(summary.total_litres || 0),
          totalLogs: Number(summary.total_logs || 0)
        }
      });
    });
  });
});

// =============================
// SERVICE SCHEDULES
// =============================
router.get("/service-schedules", authMiddleware, (req, res) => {

  if (req.user.role !== "ADMIN") {
    return res.status(403).json({ message: "Unauthorized" });
  }

  const { date_from, date_to, vehicle, status } = req.query;
  const where = [];
  const values = [];

  if (date_from) {
    where.push("s.scheduled_date >= ?");
    values.push(date_from);
  }

  if (date_to) {
    where.push("s.scheduled_date <= ?");
    values.push(date_to);
  }

  if (vehicle) {
    where.push("v.vehicle_number LIKE ?");
    values.push(`%${vehicle}%`);
  }

  if (status) {
    where.push("s.status = ?");
    values.push(status);
  }

  const whereClause = where.length > 0
    ? `WHERE ${where.join(" AND ")}`
    : "";

  const schedulesQuery = `
  SELECT
    s.id,
    s.vehicle_id,
    s.service_type,
    s.scheduled_date,
    s.status,
    s.notes,
    s.completed_at,
    s.created_at,
    v.vehicle_number,
    v.equipment_name
  FROM service_schedules s
  LEFT JOIN vehicles v
    ON v.id = s.vehicle_id
  ${whereClause}
  ORDER BY s.scheduled_date ASC, s.created_at DESC
  `;

  const summaryQuery = `
  SELECT
    COUNT(*) AS total_schedules,
    SUM(CASE WHEN s.status = 'SCHEDULED' THEN 1 ELSE 0 END) AS scheduled_count,
    SUM(CASE WHEN s.status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed_count,
    SUM(CASE WHEN s.status = 'OVERDUE' THEN 1 ELSE 0 END) AS overdue_count
  FROM service_schedules s
  LEFT JOIN vehicles v
    ON v.id = s.vehicle_id
  ${whereClause}
  `;

  db.query(schedulesQuery, values, (err, schedules) => {
    if (err) {
      return res.status(500).json(err);
    }

    db.query(summaryQuery, values, (err2, summaryRows) => {
      if (err2) {
        return res.status(500).json(err2);
      }

      const summary = summaryRows[0] || {};

      return res.json({
        schedules,
        summary: {
          totalSchedules: Number(summary.total_schedules || 0),
          scheduledCount: Number(summary.scheduled_count || 0),
          completedCount: Number(summary.completed_count || 0),
          overdueCount: Number(summary.overdue_count || 0)
        }
      });
    });
  });
});

router.post("/service-schedules", authMiddleware, (req, res) => {
  if (req.user.role !== "ADMIN") {
    return res.status(403).json({ message: "Unauthorized" });
  }

  const { vehicle_id, service_type, scheduled_date, notes } = req.body;

  if (!vehicle_id || !service_type || !scheduled_date) {
    return res.status(400).json({
      message: "vehicle_id, service_type and scheduled_date are required"
    });
  }

  db.query(
    `INSERT INTO service_schedules
     (vehicle_id, service_type, scheduled_date, status, notes, created_by)
     VALUES (?, ?, ?, 'SCHEDULED', ?, ?)`,
    [vehicle_id, service_type, scheduled_date, notes || null, req.user.id],
    (err, result) => {
      if (err) {
        return res.status(500).json(err);
      }

      return res.json({
        message: "Service schedule created",
        schedule_id: result.insertId
      });
    }
  );
});

router.patch("/service-schedules/:id/status", authMiddleware, (req, res) => {
  if (req.user.role !== "ADMIN") {
    return res.status(403).json({ message: "Unauthorized" });
  }

  const scheduleId = req.params.id;
  const { status } = req.body;
  const allowed = ["SCHEDULED", "COMPLETED", "CANCELLED", "OVERDUE"];

  if (!allowed.includes(status)) {
    return res.status(400).json({ message: "Invalid status" });
  }

  const completedAt = status === "COMPLETED" ? "NOW()" : "NULL";

  db.query(
    `UPDATE service_schedules
     SET status = ?, completed_at = ${completedAt}
     WHERE id = ?`,
    [status, scheduleId],
    (err, result) => {
      if (err) {
        return res.status(500).json(err);
      }

      if (result.affectedRows === 0) {
        return res.status(404).json({ message: "Schedule not found" });
      }

      return res.json({ message: "Status updated" });
    }
  );
});

// =============================
// DYNAMIC MAINTENANCE RULES
// =============================
router.get("/maintenance-rules", authMiddleware, async (req, res) => {
  if (req.user.role !== "ADMIN") {
    return res.status(403).json({ message: "Unauthorized" });
  }

  try {
    const vehicleId = req.query?.vehicle_id ? Number(req.query.vehicle_id) : null;
    const where = ["r.is_active = 1"];
    const values = [];

    if (vehicleId && !Number.isNaN(vehicleId)) {
      where.push(
        "(r.vehicle_id = ? OR (r.vehicle_id IS NULL AND r.vehicle_type_id = (SELECT vehicle_type_id FROM vehicles WHERE id = ? LIMIT 1)))"
      );
      values.push(vehicleId, vehicleId);
    }

    const [rows] = await dbPromise.query(
      `SELECT
         r.id,
         r.vehicle_id,
         r.vehicle_type_id,
         r.parameter_name,
         r.trigger_metric,
         r.interval_value,
         r.last_service_value,
         r.notes,
         r.is_active,
         v.vehicle_number,
         vt.name AS vehicle_type_name
       FROM maintenance_rules r
       LEFT JOIN vehicles v ON v.id = r.vehicle_id
       LEFT JOIN vehicle_types vt ON vt.id = r.vehicle_type_id
       WHERE ${where.join(" AND ")}
       ORDER BY r.parameter_name ASC, r.id DESC`,
      values
    );

    return res.json({ maintenance_rules: rows });
  } catch (err) {
    return res.status(500).json(err);
  }
});

router.post("/maintenance-rules", authMiddleware, async (req, res) => {
  if (req.user.role !== "ADMIN") {
    return res.status(403).json({ message: "Unauthorized" });
  }

  try {
    const vehicleIdRaw = req.body?.vehicle_id;
    const vehicleTypeIdRaw = req.body?.vehicle_type_id;
    const parameterName = (req.body?.parameter_name || "").toString().trim();
    const triggerMetric = normalizeMetric(req.body?.trigger_metric);
    const intervalValue = Number(req.body?.interval_value);
    const lastServiceValue = Number(req.body?.last_service_value ?? 0);
    const notes = (req.body?.notes || "").toString().trim();

    const vehicleId = vehicleIdRaw ? Number(vehicleIdRaw) : null;
    const vehicleTypeId = vehicleTypeIdRaw ? Number(vehicleTypeIdRaw) : null;

    if (!parameterName || !triggerMetric || Number.isNaN(intervalValue) || intervalValue <= 0) {
      return res.status(400).json({
        message:
          "parameter_name, trigger_metric and positive interval_value are required",
      });
    }

    if ((!vehicleId || Number.isNaN(vehicleId)) && (!vehicleTypeId || Number.isNaN(vehicleTypeId))) {
      return res.status(400).json({
        message: "vehicle_id or vehicle_type_id is required",
      });
    }

    await dbPromise.query(
      `INSERT INTO maintenance_rules
       (vehicle_id, vehicle_type_id, parameter_name, trigger_metric, interval_value, last_service_value, notes, created_by)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
      [
        vehicleId && !Number.isNaN(vehicleId) ? vehicleId : null,
        vehicleTypeId && !Number.isNaN(vehicleTypeId) ? vehicleTypeId : null,
        parameterName,
        triggerMetric,
        intervalValue,
        Number.isNaN(lastServiceValue) ? 0 : lastServiceValue,
        notes || null,
        req.user.id,
      ]
    );

    return res.status(201).json({ message: "Maintenance rule created" });
  } catch (err) {
    return res.status(500).json(err);
  }
});

router.post(
  "/maintenance-rules/import",
  authMiddleware,
  upload.single("file"),
  async (req, res) => {
    if (req.user.role !== "ADMIN") {
      return res.status(403).json({ message: "Unauthorized" });
    }

    if (!req.file?.buffer) {
      return res.status(400).json({ message: "Excel file is required" });
    }

    try {
      const workbook = new ExcelJS.Workbook();
      await workbook.xlsx.load(req.file.buffer);

      const worksheet = workbook.worksheets[0];
      if (!worksheet) {
        return res.status(400).json({ message: "No worksheet found in file" });
      }

      const headerRow = worksheet.getRow(1);
      const headers = {};
      headerRow.eachCell((cell, colNumber) => {
        headers[colNumber] = (cell.value || "").toString().trim().toLowerCase();
      });

      const [vehicles] = await dbPromise.query(
        "SELECT id, vehicle_number, vehicle_type_id FROM vehicles"
      );
      const [vehicleTypes] = await dbPromise.query(
        "SELECT id, name FROM vehicle_types"
      );

      const vehicleByNumber = {};
      for (const v of vehicles) {
        vehicleByNumber[(v.vehicle_number || "").toString().trim().toUpperCase()] = v;
      }

      const vehicleTypeByName = {};
      const vehicleTypeByNormalizedName = {};
      for (const vt of vehicleTypes) {
        const upper = (vt.name || "").toString().trim().toUpperCase();
        vehicleTypeByName[upper] = vt;
        vehicleTypeByNormalizedName[normalizeLabel(upper)] = vt;
      }

      const inserted = [];
      const failed = [];
      const firstHeader = (headers[1] || "").toString().toLowerCase();
      const isMatrixFormat = firstHeader.includes("parameter");

      if (isMatrixFormat) {
        const columnConfigs = [];

        for (let col = 2; col <= headerRow.cellCount; col += 1) {
          const headerText = (headers[col] || "").toString().trim();
          if (!headerText) continue;

          const metricFromHeaderMatch = headerText.match(/\(([^)]+)\)/);
          const metricFromHeader = normalizeMetric(
            metricFromHeaderMatch?.[1] || ""
          );
          const rawVehicleTypeName = headerText.replace(/\(.*?\)/g, "").trim();

          const vehicleTypeId = resolveVehicleTypeId(
            rawVehicleTypeName,
            vehicleTypeByName,
            vehicleTypeByNormalizedName
          );

          if (!vehicleTypeId) {
            failed.push({
              row: 1,
              column: col,
              reason: `Vehicle type not found for header: ${headerText}`,
            });
            continue;
          }

          columnConfigs.push({
            col,
            headerText,
            vehicleTypeId,
            metricFromHeader,
          });
        }

        for (let rowNum = 2; rowNum <= worksheet.rowCount; rowNum += 1) {
          const row = worksheet.getRow(rowNum);
          const parameterName = (row.getCell(1).value || "").toString().trim();

          if (!parameterName) continue;

          for (const config of columnConfigs) {
            const cellRaw = (row.getCell(config.col).value || "").toString().trim();
            if (!cellRaw) continue;

            const numberMatch = cellRaw.match(/-?\d+(\.\d+)?/);
            const intervalValue = numberMatch ? Number(numberMatch[0]) : NaN;
            const triggerMetric =
              config.metricFromHeader || inferMetricFromText(cellRaw);

            if (!triggerMetric || Number.isNaN(intervalValue) || intervalValue <= 0) {
              failed.push({
                row: rowNum,
                column: config.col,
                reason: `Invalid interval or metric for ${parameterName} in ${config.headerText}`,
              });
              continue;
            }

            await dbPromise.query(
              `INSERT INTO maintenance_rules
               (vehicle_id, vehicle_type_id, parameter_name, trigger_metric, interval_value, last_service_value, created_by)
               VALUES (?, ?, ?, ?, ?, ?, ?)`,
              [
                null,
                config.vehicleTypeId,
                parameterName,
                triggerMetric,
                intervalValue,
                0,
                req.user.id,
              ]
            );

            inserted.push({
              row: rowNum,
              column: config.col,
              parameter_name: parameterName,
              trigger_metric: triggerMetric,
              interval_value: intervalValue,
              vehicle_id: null,
              vehicle_type_id: config.vehicleTypeId,
            });
          }
        }

        return res.json({
          message: "Maintenance rules import completed (matrix format)",
          inserted_count: inserted.length,
          failed_count: failed.length,
          inserted,
          failed,
        });
      }

      for (let rowNum = 2; rowNum <= worksheet.rowCount; rowNum += 1) {
        const row = worksheet.getRow(rowNum);
        const values = {};

        for (let col = 1; col <= headerRow.cellCount; col += 1) {
          const key = headers[col];
          if (!key) continue;
          values[key] = (row.getCell(col).value || "").toString().trim();
        }

        const vehicleNumber = (values["vehicle_number"] || values["vehicle"] || "")
          .toString()
          .trim()
          .toUpperCase();
        const vehicleTypeName = (values["vehicle_type"] || values["equipment_name"] || "")
          .toString()
          .trim()
          .toUpperCase();
        const parameterName = (
          values["parameter_name"] ||
          values["service_type"] ||
          values["maintenance_parameter"] ||
          ""
        )
          .toString()
          .trim();
        const triggerMetric = normalizeMetric(
          values["trigger_metric"] || values["metric"] || values["basis"]
        );
        const intervalValue = Number(values["interval_value"] || values["interval"] || values["scheduled"]);
        const lastServiceValue = Number(values["last_service_value"] || values["last_done_value"] || 0);

        if (!parameterName || !triggerMetric || Number.isNaN(intervalValue) || intervalValue <= 0) {
          failed.push({
            row: rowNum,
            reason: "parameter_name, trigger_metric and interval_value are required",
          });
          continue;
        }

        let vehicleId = null;
        let vehicleTypeId = null;

        if (vehicleNumber) {
          const matchedVehicle = vehicleByNumber[vehicleNumber];
          if (!matchedVehicle) {
            failed.push({ row: rowNum, reason: `Vehicle not found: ${vehicleNumber}` });
            continue;
          }
          vehicleId = matchedVehicle.id;
          vehicleTypeId = matchedVehicle.vehicle_type_id || null;
        } else if (vehicleTypeName) {
          const matchedTypeId = resolveVehicleTypeId(
            vehicleTypeName,
            vehicleTypeByName,
            vehicleTypeByNormalizedName
          );
          if (!matchedTypeId) {
            failed.push({ row: rowNum, reason: `Vehicle type not found: ${vehicleTypeName}` });
            continue;
          }
          vehicleTypeId = matchedTypeId;
        } else {
          failed.push({
            row: rowNum,
            reason: "vehicle_number or vehicle_type is required",
          });
          continue;
        }

        await dbPromise.query(
          `INSERT INTO maintenance_rules
           (vehicle_id, vehicle_type_id, parameter_name, trigger_metric, interval_value, last_service_value, created_by)
           VALUES (?, ?, ?, ?, ?, ?, ?)`,
          [
            vehicleId,
            vehicleTypeId,
            parameterName,
            triggerMetric,
            intervalValue,
            Number.isNaN(lastServiceValue) ? 0 : lastServiceValue,
            req.user.id,
          ]
        );

        inserted.push({
          row: rowNum,
          parameter_name: parameterName,
          trigger_metric: triggerMetric,
          interval_value: intervalValue,
          vehicle_id: vehicleId,
          vehicle_type_id: vehicleTypeId,
        });
      }

      return res.json({
        message: "Maintenance rules import completed",
        inserted_count: inserted.length,
        failed_count: failed.length,
        inserted,
        failed,
      });
    } catch (err) {
      return res.status(500).json({
        message: "Failed to import maintenance rules from Excel",
        error: err.message,
      });
    }
  }
);

router.get("/maintenance-due", authMiddleware, async (req, res) => {
  if (req.user.role !== "ADMIN") {
    return res.status(403).json({ message: "Unauthorized" });
  }

  try {
    const vehicleId = req.query?.vehicle_id ? Number(req.query.vehicle_id) : null;
    const dueOnly = (req.query?.due_only || "").toString().toLowerCase() === "true";

    const rows = await getResolvedMaintenanceRows({
      vehicleId: vehicleId && !Number.isNaN(vehicleId) ? vehicleId : null,
      dueOnly,
    });

    return res.json({
      maintenance_due: rows,
      summary: {
        total: rows.length,
        due: rows.filter((r) => r.due_status === "DUE").length,
        upcoming: rows.filter((r) => r.due_status === "UPCOMING").length,
        unknown: rows.filter((r) => r.due_status === "UNKNOWN").length,
      },
    });
  } catch (err) {
    return res.status(500).json(err);
  }
});

router.post("/maintenance-rules/:id/complete", authMiddleware, async (req, res) => {
  if (req.user.role !== "ADMIN") {
    return res.status(403).json({ message: "Unauthorized" });
  }

  try {
    const ruleId = Number(req.params.id);
    const vehicleIdFromBody = req.body?.vehicle_id ? Number(req.body.vehicle_id) : null;
    const notes = (req.body?.notes || "").toString().trim();

    if (Number.isNaN(ruleId) || ruleId <= 0) {
      return res.status(400).json({ message: "Invalid maintenance rule id" });
    }

    const [ruleRows] = await dbPromise.query(
      `SELECT id, vehicle_id, vehicle_type_id, trigger_metric
       FROM maintenance_rules
       WHERE id = ? LIMIT 1`,
      [ruleId]
    );

    if (ruleRows.length === 0) {
      return res.status(404).json({ message: "Maintenance rule not found" });
    }

    const rule = ruleRows[0];
    let resolvedVehicleId = rule.vehicle_id || vehicleIdFromBody;

    if (!resolvedVehicleId && rule.vehicle_type_id) {
      return res.status(400).json({
        message: "vehicle_id is required for vehicle-type level rule completion",
      });
    }

    if (!resolvedVehicleId) {
      return res.status(400).json({ message: "vehicle_id is required" });
    }

    resolvedVehicleId = Number(resolvedVehicleId);
    if (Number.isNaN(resolvedVehicleId) || resolvedVehicleId <= 0) {
      return res.status(400).json({ message: "Invalid vehicle_id" });
    }

    let performedValue =
      req.body?.performed_value !== undefined
        ? Number(req.body.performed_value)
        : null;

    if (performedValue === null || Number.isNaN(performedValue)) {
      const meterMap = await getLatestMetersForVehicles([resolvedVehicleId]);
      const latest = meterMap[resolvedVehicleId]?.[rule.trigger_metric];
      if (latest === undefined) {
        return res.status(400).json({
          message:
            "performed_value missing and latest meter value could not be resolved from trip readings",
        });
      }
      performedValue = Number(latest);
    }

    await dbPromise.query(
      `INSERT INTO maintenance_records
       (maintenance_rule_id, vehicle_id, performed_value, notes, created_by)
       VALUES (?, ?, ?, ?, ?)`,
      [ruleId, resolvedVehicleId, performedValue, notes || null, req.user.id]
    );

    if (rule.vehicle_id) {
      await dbPromise.query(
        "UPDATE maintenance_rules SET last_service_value = ? WHERE id = ?",
        [performedValue, ruleId]
      );
    } else {
      const [specificRuleRows] = await dbPromise.query(
        `SELECT id FROM maintenance_rules
         WHERE vehicle_id = ?
           AND parameter_name = (SELECT parameter_name FROM maintenance_rules WHERE id = ?)
           AND trigger_metric = (SELECT trigger_metric FROM maintenance_rules WHERE id = ?)
         LIMIT 1`,
        [resolvedVehicleId, ruleId, ruleId]
      );

      if (specificRuleRows.length > 0) {
        await dbPromise.query(
          "UPDATE maintenance_rules SET last_service_value = ? WHERE id = ?",
          [performedValue, specificRuleRows[0].id]
        );
      } else {
        await dbPromise.query(
          `INSERT INTO maintenance_rules
           (vehicle_id, vehicle_type_id, parameter_name, trigger_metric, interval_value, last_service_value, is_active, created_by)
           SELECT ?, vehicle_type_id, parameter_name, trigger_metric, interval_value, ?, 1, ?
           FROM maintenance_rules
           WHERE id = ?`,
          [resolvedVehicleId, performedValue, req.user.id, ruleId]
        );
      }
    }

    return res.json({
      message: "Maintenance marked as completed",
      maintenance_rule_id: ruleId,
      vehicle_id: resolvedVehicleId,
      performed_value: performedValue,
    });
  } catch (err) {
    return res.status(500).json(err);
  }
});

// =============================
// REPORTS
// =============================
function getReportPayload(filters, callback) {
  const { date_from, date_to, vehicle } = filters;

  const tripWhere = [];
  const tripValues = [];

  if (date_from) {
    tripWhere.push("DATE(t.created_at) >= ?");
    tripValues.push(date_from);
  }
  if (date_to) {
    tripWhere.push("DATE(t.created_at) <= ?");
    tripValues.push(date_to);
  }
  if (vehicle) {
    tripWhere.push("v.vehicle_number LIKE ?");
    tripValues.push(`%${vehicle}%`);
  }

  const tripWhereClause = tripWhere.length > 0 ? `WHERE ${tripWhere.join(" AND ")}` : "";

  const refuelWhere = [];
  const refuelValues = [];

  if (date_from) {
    refuelWhere.push("DATE(r.created_at) >= ?");
    refuelValues.push(date_from);
  }
  if (date_to) {
    refuelWhere.push("DATE(r.created_at) <= ?");
    refuelValues.push(date_to);
  }
  if (vehicle) {
    refuelWhere.push("v.vehicle_number LIKE ?");
    refuelValues.push(`%${vehicle}%`);
  }

  const refuelWhereClause = refuelWhere.length > 0 ? `WHERE ${refuelWhere.join(" AND ")}` : "";

  const serviceWhere = [];
  const serviceValues = [];

  if (date_from) {
    serviceWhere.push("s.scheduled_date >= ?");
    serviceValues.push(date_from);
  }
  if (date_to) {
    serviceWhere.push("s.scheduled_date <= ?");
    serviceValues.push(date_to);
  }
  if (vehicle) {
    serviceWhere.push("v.vehicle_number LIKE ?");
    serviceValues.push(`%${vehicle}%`);
  }

  const serviceWhereClause = serviceWhere.length > 0 ? `WHERE ${serviceWhere.join(" AND ")}` : "";

  const tripSummaryQuery = `
  SELECT
    COUNT(*) AS total_trips,
    SUM(CASE WHEN t.trip_status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed_trips,
    SUM(CASE WHEN t.trip_status = 'STARTED' THEN 1 ELSE 0 END) AS active_trips
  FROM trips t
  LEFT JOIN vehicles v
    ON v.id = t.vehicle_id
  ${tripWhereClause}
  `;

  const refuelSummaryQuery = `
  SELECT
    COUNT(*) AS total_refuels,
    COALESCE(SUM(CAST(r.litre AS DECIMAL(10,2))), 0) AS total_refuel_litres
  FROM trip_refuels r
  LEFT JOIN trips t
    ON t.id = r.trip_id
  LEFT JOIN vehicles v
    ON v.id = t.vehicle_id
  ${refuelWhereClause}
  `;

  const serviceSummaryQuery = `
  SELECT
    COUNT(*) AS total_services,
    SUM(CASE WHEN s.status = 'SCHEDULED' THEN 1 ELSE 0 END) AS scheduled_services,
    SUM(CASE WHEN s.status = 'COMPLETED' THEN 1 ELSE 0 END) AS completed_services,
    SUM(CASE WHEN s.status = 'OVERDUE' THEN 1 ELSE 0 END) AS overdue_services
  FROM service_schedules s
  LEFT JOIN vehicles v
    ON v.id = s.vehicle_id
  ${serviceWhereClause}
  `;

  const topRefuelVehiclesQuery = `
  SELECT
    v.vehicle_number,
    COALESCE(SUM(CAST(r.litre AS DECIMAL(10,2))), 0) AS total_litres,
    COUNT(*) AS total_refuels
  FROM trip_refuels r
  LEFT JOIN trips t
    ON t.id = r.trip_id
  LEFT JOIN vehicles v
    ON v.id = t.vehicle_id
  ${refuelWhereClause}
  GROUP BY v.vehicle_number
  ORDER BY total_litres DESC
  LIMIT 5
  `;

  const recentTripsQuery = `
  SELECT
    t.id AS trip_id,
    v.vehicle_number,
    d.name AS driver_name,
    t.trip_status,
    t.created_at
  FROM trips t
  LEFT JOIN vehicles v
    ON v.id = t.vehicle_id
  LEFT JOIN drivers d
    ON d.id = t.driver_id
  ${tripWhereClause}
  ORDER BY t.created_at DESC
  LIMIT 10
  `;

  db.query(tripSummaryQuery, tripValues, (err, tripRows) => {
    if (err) return callback(err);

    db.query(refuelSummaryQuery, refuelValues, (err2, refuelRows) => {
      if (err2) return callback(err2);

      db.query(serviceSummaryQuery, serviceValues, (err3, serviceRows) => {
        if (err3) return callback(err3);

        db.query(topRefuelVehiclesQuery, refuelValues, (err4, topRefuelVehicles) => {
          if (err4) return callback(err4);

          db.query(recentTripsQuery, tripValues, (err5, recentTrips) => {
            if (err5) return callback(err5);

            fetchCompletedTripsWithReadings(filters, (err6, completedTrips) => {
              if (err6) return callback(err6);

              const tripSummary = tripRows[0] || {};
              const refuelSummary = refuelRows[0] || {};
              const serviceSummary = serviceRows[0] || {};

              return callback(null, {
                summary: {
                  totalTrips: Number(tripSummary.total_trips || 0),
                  completedTrips: Number(tripSummary.completed_trips || 0),
                  activeTrips: Number(tripSummary.active_trips || 0),
                  totalRefuels: Number(refuelSummary.total_refuels || 0),
                  totalRefuelLitres: Number(refuelSummary.total_refuel_litres || 0),
                  totalServices: Number(serviceSummary.total_services || 0),
                  scheduledServices: Number(serviceSummary.scheduled_services || 0),
                  completedServices: Number(serviceSummary.completed_services || 0),
                  overdueServices: Number(serviceSummary.overdue_services || 0)
                },
                topRefuelVehicles,
                recentTrips,
                completedTrips
              });
            });
          });
        });
      });
    });
  });
}

router.get("/reports", authMiddleware, (req, res) => {
  if (req.user.role !== "ADMIN") {
    return res.status(403).json({ message: "Unauthorized" });
  }

  getReportPayload(req.query, (err, payload) => {
    if (err) return res.status(500).json(err);
    return res.json(payload);
  });
});

router.get("/reports/export/excel", authMiddleware, (req, res) => {
  if (req.user.role !== "ADMIN") {
    return res.status(403).json({ message: "Unauthorized" });
  }

  getReportPayload(req.query, async (err, payload) => {
    if (err) return res.status(500).json(err);

    try {
      const workbook = new ExcelJS.Workbook();
      const summarySheet = workbook.addWorksheet("Summary");

      summarySheet.addRow(["Fleet Report"]);
      summarySheet.addRow([]);
      summarySheet.addRow(["Metric", "Value"]);
      Object.entries(payload.summary).forEach(([key, value]) => {
        summarySheet.addRow([key, value]);
      });

      const refuelSheet = workbook.addWorksheet("Top Refuel Vehicles");
      refuelSheet.addRow(["Vehicle", "Total Litres", "Refuels"]);
      payload.topRefuelVehicles.forEach((row) => {
        refuelSheet.addRow([
          row.vehicle_number || "-",
          Number(row.total_litres || 0),
          Number(row.total_refuels || 0)
        ]);
      });

      const tripsSheet = workbook.addWorksheet("Recent Trips");
      tripsSheet.addRow(["Trip ID", "Vehicle", "Driver", "Status", "Created At"]);
      payload.recentTrips.forEach((row) => {
        tripsSheet.addRow([
          row.trip_id || "-",
          row.vehicle_number || "-",
          row.driver_name || "-",
          row.trip_status || "-",
          row.created_at || "-"
        ]);
      });

      const completedTripsSheet = workbook.addWorksheet("Completed Trips");
      completedTripsSheet.addRow([
        "Trip ID",
        "Vehicle",
        "Driver",
        "Status",
        "Completed At",
        "Reading",
        "Unit",
        "Start",
        "End",
        "Difference"
      ]);
      payload.completedTrips.forEach((trip) => {
        const readings = Array.isArray(trip.readings) ? trip.readings : [];

        if (readings.length == 0) {
          completedTripsSheet.addRow([
            trip.trip_id || "-",
            trip.vehicle_number || "-",
            trip.driver_name || "-",
            trip.trip_status || "-",
            trip.completed_at || trip.created_at || "-",
            "-",
            "-",
            "-",
            "-",
            "-"
          ]);
          return;
        }

        readings.forEach((reading) => {
          completedTripsSheet.addRow([
            trip.trip_id || "-",
            trip.vehicle_number || "-",
            trip.driver_name || "-",
            trip.trip_status || "-",
            trip.completed_at || trip.created_at || "-",
            reading.name || "-",
            reading.unit || "-",
            reading.start_value ?? "-",
            reading.end_value ?? "-",
            reading.difference_value ?? "-"
          ]);
        });
      });

      const buffer = await workbook.xlsx.writeBuffer();
      const timestamp = new Date().toISOString().slice(0, 19).replace(/[T:]/g, "-");
      const fileName = `fleet-report-${timestamp}.xlsx`;

      res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
      res.setHeader("Content-Disposition", `attachment; filename=\"${fileName}\"`);
      return res.send(Buffer.from(buffer));
    } catch (exportErr) {
      return res.status(500).json({
        message: "Failed to generate Excel report",
        error: exportErr.message
      });
    }
  });
});

router.get("/reports/export/pdf", authMiddleware, (req, res) => {
  if (req.user.role !== "ADMIN") {
    return res.status(403).json({ message: "Unauthorized" });
  }

  getReportPayload(req.query, (err, payload) => {
    if (err) return res.status(500).json(err);

    const timestamp = new Date().toISOString().slice(0, 19).replace(/[T:]/g, "-");
    const fileName = `fleet-report-${timestamp}.pdf`;

    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `attachment; filename=\"${fileName}\"`);

    const doc = new PDFDocument({ margin: 40, size: "A4" });
    doc.pipe(res);

    doc.fontSize(18).text("Fleet Report", { underline: true });
    doc.moveDown();

    doc.fontSize(13).text("Summary");
    Object.entries(payload.summary).forEach(([key, value]) => {
      doc.fontSize(11).text(`${key}: ${value}`);
    });

    doc.moveDown();
    doc.fontSize(13).text("Top Refuel Vehicles");
    if (payload.topRefuelVehicles.length === 0) {
      doc.fontSize(11).text("No data");
    } else {
      payload.topRefuelVehicles.forEach((row) => {
        doc
          .fontSize(11)
          .text(
            `${row.vehicle_number || "-"}  |  ${Number(row.total_litres || 0)} L  |  ${Number(row.total_refuels || 0)} refuels`
          );
      });
    }

    doc.moveDown();
    doc.fontSize(13).text("Recent Trips");
    if (payload.recentTrips.length === 0) {
      doc.fontSize(11).text("No trips found");
    } else {
      payload.recentTrips.forEach((row) => {
        doc
          .fontSize(10)
          .text(
            `Trip #${row.trip_id || "-"} | ${row.vehicle_number || "-"} | ${row.driver_name || "-"} | ${row.trip_status || "-"} | ${row.created_at || "-"}`
          );
      });
    }

    doc.moveDown();
    doc.fontSize(13).text("Completed Trips");
    if (payload.completedTrips.length === 0) {
      doc.fontSize(11).text("No completed trips found");
    } else {
      payload.completedTrips.forEach((trip) => {
        doc
          .fontSize(11)
          .text(
            `Trip #${trip.trip_id || "-"} | ${trip.vehicle_number || "-"} | ${trip.driver_name || "-"} | ${trip.completed_at || trip.created_at || "-"}`
          );

        if (!Array.isArray(trip.readings) || trip.readings.length === 0) {
          doc.fontSize(10).text("No reading details");
          doc.moveDown(0.5);
          return;
        }

        trip.readings.forEach((reading) => {
          doc
            .fontSize(10)
            .text(
              `${reading.name || "-"} (${reading.unit || "-"}) | Start: ${reading.start_value ?? "-"} | End: ${reading.end_value ?? "-"} | Diff: ${reading.difference_value ?? "-"}`
            );
        });
        doc.moveDown(0.5);
      });
    }

    doc.end();
    return null;
  });
});

module.exports = router;
