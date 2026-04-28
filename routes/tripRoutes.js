const express = require("express");
const router = express.Router();
const db = require("../config/db");
const authMiddleware = require("../middleware/authMiddleware");
const multer = require("multer");
const dbPromise = db.promise();

const storage = multer.diskStorage({
  destination: (req, file, cb) => {
    cb(null, "uploads/");
  },
  filename: (req, file, cb) => {
    cb(null, Date.now() + "-" + file.originalname);
  },
});

const upload = multer({ storage: storage });

async function ensureTripsCompletedAtColumn() {
  try {
    const [rows] = await dbPromise.query(
      `SELECT 1
       FROM information_schema.columns
       WHERE table_schema = DATABASE()
         AND table_name = 'trips'
         AND column_name = 'completed_at'
       LIMIT 1`
    );

    if (rows.length === 0) {
      await dbPromise.query(
        "ALTER TABLE trips ADD COLUMN completed_at DATETIME NULL"
      );
    }
  } catch (err) {
    console.error("Failed to ensure trips.completed_at column:", err);
  }
}

ensureTripsCompletedAtColumn();

function inferMetricFromReadingName(name) {
  const value = (name || "").toString().toLowerCase();

  if (value.includes("pto")) return "PTO_HOUR_METER";
  if (value.includes("back engine") || value.includes("rear engine")) {
    return "BACK_ENGINE_HOUR_METER";
  }
  if (value.includes("hour")) return "HOUR_METER";
  if (value.includes("km") || value.includes("kilometer") || value.includes("odometer")) {
    return "KM";
  }

  return null;
}

async function syncVehicleMaintenanceDue(vehicleId, createdBy) {
  const [vehicleRows] = await dbPromise.query(
    "SELECT id, vehicle_type_id, status FROM vehicles WHERE id = ? LIMIT 1",
    [vehicleId]
  );

  if (vehicleRows.length === 0) return;
  const vehicle = vehicleRows[0];

  const [ruleRows] = await dbPromise.query(
    `SELECT id, parameter_name, trigger_metric, interval_value, last_service_value
     FROM maintenance_rules
     WHERE is_active = 1
       AND (
         vehicle_id = ?
         OR (vehicle_id IS NULL AND vehicle_type_id = ?)
       )`,
    [vehicleId, vehicle.vehicle_type_id]
  );

  if (ruleRows.length === 0) {
    if (vehicle.status === "SERVICE_DUE") {
      await dbPromise.query(
        "UPDATE vehicles SET status = 'ACTIVE' WHERE id = ?",
        [vehicleId]
      );
    }
    return;
  }

  const [readingRows] = await dbPromise.query(
    `SELECT rt.name AS reading_name, COALESCE(tr.end_value, tr.start_value) AS reading_value
     FROM trips t
     JOIN trip_readings tr ON tr.trip_id = t.id
     JOIN reading_types rt ON rt.id = tr.reading_type_id
     WHERE t.vehicle_id = ?
       AND COALESCE(tr.end_value, tr.start_value) IS NOT NULL
     ORDER BY t.created_at DESC, tr.id DESC`,
    [vehicleId]
  );

  const latestMetricValues = {};
  for (const row of readingRows) {
    const metric = inferMetricFromReadingName(row.reading_name);
    const value = Number(row.reading_value);
    if (!metric || Number.isNaN(value)) continue;
    if (latestMetricValues[metric] !== undefined) continue;
    latestMetricValues[metric] = value;
  }

  const [openScheduleRows] = await dbPromise.query(
    `SELECT service_type
     FROM service_schedules
     WHERE vehicle_id = ?
       AND status IN ('SCHEDULED', 'OVERDUE')`,
    [vehicleId]
  );

  const openByServiceType = new Set(
    openScheduleRows.map((r) => (r.service_type || "").toString().trim().toUpperCase())
  );

  let dueCount = 0;

  for (const rule of ruleRows) {
    const currentValue = latestMetricValues[rule.trigger_metric];
    if (currentValue === undefined) continue;

    const dueValue = Number(rule.last_service_value || 0) + Number(rule.interval_value || 0);
    if (Number.isNaN(dueValue)) continue;

    if (currentValue >= dueValue) {
      dueCount += 1;

      const serviceType = (rule.parameter_name || "").toString().trim();
      const serviceKey = serviceType.toUpperCase();

      if (!openByServiceType.has(serviceKey)) {
        const metricLabel = rule.trigger_metric.replaceAll("_", " ");
        const notes =
          `Auto-generated: due at ${dueValue} ${metricLabel}, current ${currentValue} ${metricLabel}`;

        await dbPromise.query(
          `INSERT INTO service_schedules
           (vehicle_id, service_type, scheduled_date, status, notes, created_by)
           VALUES (?, ?, CURDATE(), 'OVERDUE', ?, ?)`,
          [vehicleId, serviceType, notes, createdBy || null]
        );

        openByServiceType.add(serviceKey);
      }
    }
  }

  if (vehicle.status === "ACTIVE" && dueCount > 0) {
    await dbPromise.query(
      "UPDATE vehicles SET status = 'SERVICE_DUE' WHERE id = ?",
      [vehicleId]
    );
  }

  if (vehicle.status === "SERVICE_DUE" && dueCount === 0) {
    await dbPromise.query(
      "UPDATE vehicles SET status = 'ACTIVE' WHERE id = ?",
      [vehicleId]
    );
  }
}

async function findActiveTripByDriver(driverId) {
  const [rows] = await dbPromise.query(
    `SELECT t.id,
            t.driver_id,
            t.vehicle_id,
            t.trip_date,
            t.trip_status,
            t.created_at,
            v.vehicle_number
     FROM trips t
     JOIN vehicles v ON v.id = t.vehicle_id
     WHERE t.driver_id = ?
       AND t.trip_status = 'STARTED'
     ORDER BY t.created_at DESC, t.id DESC
     LIMIT 1`,
    [driverId]
  );

  return rows[0] || null;
}

async function findActiveTripByVehicle(vehicleId) {
  const [rows] = await dbPromise.query(
    `SELECT t.id,
            t.driver_id,
            t.vehicle_id,
            t.trip_date,
            t.trip_status,
            t.created_at,
            v.vehicle_number,
            d.name AS driver_name
     FROM trips t
     JOIN vehicles v ON v.id = t.vehicle_id
     LEFT JOIN drivers d ON d.id = t.driver_id
     WHERE t.vehicle_id = ?
       AND t.trip_status = 'STARTED'
     ORDER BY t.created_at DESC, t.id DESC
     LIMIT 1`,
    [vehicleId]
  );

  return rows[0] || null;
}


// ============================
// START TRIP
// ============================
// ============================
// START TRIP (FINAL VERSION)
// ============================
router.post(
  "/start",
  authMiddleware,
  upload.single("start_photo"),
  async (req, res) => {
    const driverId = req.user.id;
    const vehicleId = Number(req.body?.vehicle_id);

    let readings;

    try {
      readings = JSON.parse(req.body.readings);
    } catch (err) {
      return res.status(400).json({
        message: "Invalid readings format"
      });
    }

    if (Number.isNaN(vehicleId) || vehicleId <= 0 || !readings || readings.length === 0) {
      return res.status(400).json({
        message: "Vehicle and readings required"
      });
    }

    const photoPath = req.file ? req.file.path : null;

    if (!photoPath) {
      return res.status(400).json({
        message: "Speedometer photo required"
      });
    }

    const hasInvalidReading = readings.some(
      (reading) =>
        !reading ||
        Number.isNaN(Number(reading.reading_type_id)) ||
        Number.isNaN(Number(reading.start_value))
    );

    if (hasInvalidReading) {
      return res.status(400).json({
        message: "Each reading must include a valid reading type and start value"
      });
    }

    try {
      const existingDriverTrip = await findActiveTripByDriver(driverId);
      if (existingDriverTrip) {
        return res.json({
          message: "Trip already started for this driver",
          already_started: true,
          trip_id: existingDriverTrip.id,
          trip: existingDriverTrip
        });
      }

      const existingVehicleTrip = await findActiveTripByVehicle(vehicleId);
      if (existingVehicleTrip) {
        return res.status(409).json({
          message: `Vehicle already has an active trip${existingVehicleTrip.driver_name ? ` with ${existingVehicleTrip.driver_name}` : ""}`,
          trip_id: existingVehicleTrip.id,
          trip: existingVehicleTrip
        });
      }

      const connection = await dbPromise.getConnection();

      try {
        await connection.beginTransaction();

        const [tripResult] = await connection.query(
          `INSERT INTO trips
           (driver_id, vehicle_id, trip_date, trip_status, start_photo)
           VALUES (?, ?, CURDATE(), 'STARTED', ?)`,
          [driverId, vehicleId, photoPath]
        );

        const tripId = tripResult.insertId;
        const readingValues = readings.map((reading) => [
          tripId,
          Number(reading.reading_type_id),
          Number(reading.start_value),
          null
        ]);

        await connection.query(
          `INSERT INTO trip_readings
           (trip_id, reading_type_id, start_value, end_value)
           VALUES ?`,
          [readingValues]
        );

        await connection.commit();

        return res.json({
          message: "Trip started successfully",
          trip_id: tripId
        });
      } catch (error) {
        await connection.rollback();
        console.error("Failed to start trip:", error);
        return res.status(500).json({ message: "Unable to start trip" });
      } finally {
        connection.release();
      }
    } catch (error) {
      console.error("Failed to prepare trip start:", error);
      return res.status(500).json({ message: "Unable to start trip" });
    }
  }
);


// ============================
// END TRIP
// ============================
router.post(
  "/end",
  authMiddleware,
  upload.single("end_photo"),
  (req, res) => {
    const tripId = Number(req.body?.trip_id);

    let readings;

    try {
      readings = JSON.parse(req.body.readings);
    } catch (err) {
      return res.status(400).json({
        message: "Invalid readings format"
      });
    }

    if (Number.isNaN(tripId) || tripId <= 0 || !readings || readings.length === 0) {
      return res.status(400).json({
        message: "Trip ID and readings required"
      });
    }

    const photoPath = req.file ? req.file.path : null;

    if (!photoPath) {
      return res.status(400).json({
        message: "End speedometer photo required"
      });
    }

    // 1️⃣ Update end readings
    readings.forEach((r) => {
      db.query(
        `UPDATE trip_readings
         SET end_value = ?
         WHERE trip_id = ? AND reading_type_id = ?`,
        [r.end_value, tripId, r.reading_type_id]
      );
    });

    // Resolve vehicle for post-completion maintenance sync.
    db.query(
      "SELECT vehicle_id, driver_id, trip_status FROM trips WHERE id = ? LIMIT 1",
      [tripId],
      (tripErr, tripRows) => {
        if (tripErr) {
          return res.status(500).json({ message: "Unable to complete trip" });
        }
        if (tripRows.length === 0) {
          return res.status(404).json({ message: "Trip not found" });
        }

        const trip = tripRows[0];
        if (trip.driver_id !== req.user.id) {
          return res.status(403).json({ message: "Unauthorized for this trip" });
        }
        if (trip.trip_status !== "STARTED") {
          return res.status(400).json({ message: "Trip is not active" });
        }

        // 2️⃣ Update trip status & photo
    db.query(
      `UPDATE trips 
       SET trip_status = 'COMPLETED',
           end_photo = ?,
           completed_at = NOW()
       WHERE id = ?`,
      [photoPath, tripId],
          async (err) => {
            if (err) {
              return res.status(500).json({ message: "Unable to complete trip" });
            }

            res.json({
              message: "Trip completed successfully"
            });

            // Maintenance due generation should not block trip completion response.
            try {
              await syncVehicleMaintenanceDue(trip.vehicle_id, req.user.id);
            } catch (syncErr) {
              console.error("Failed to sync maintenance after trip end:", syncErr);
            }
          }
        );
      }
    );
  }
);


// ============================
// GET DRIVER TRIP HISTORY
// ============================
router.get("/history", authMiddleware, (req, res) => {
  const driver_id = req.user.id;

  db.query(
    `SELECT t.id, 
            v.vehicle_number, 
            t.trip_date,
            t.trip_status,
            t.created_at
     FROM trips t
     JOIN vehicles v ON t.vehicle_id = v.id
     WHERE t.driver_id = ?
     ORDER BY t.created_at DESC`,
    [driver_id],
    (err, tripRows) => {
      if (err) return res.status(500).json(err);

      if (tripRows.length === 0) {
        return res.json({
          message: "Trip history fetched",
          trips: []
        });
      }

      const tripIds = tripRows.map((row) => row.id);

      db.query(
        `SELECT tr.trip_id,
                tr.reading_type_id,
                rt.name,
                rt.unit,
                tr.start_value,
                tr.end_value
         FROM trip_readings tr
         JOIN reading_types rt ON rt.id = tr.reading_type_id
         WHERE tr.trip_id IN (?)
         ORDER BY tr.trip_id DESC, rt.name ASC`,
        [tripIds],
        (readingErr, readingRows) => {
          if (readingErr) return res.status(500).json(readingErr);

          const readingMap = {};

          const toNumber = (value) => {
            if (value === null || value === undefined) return null;
            const parsed = Number(value);
            return Number.isNaN(parsed) ? null : parsed;
          };

          for (const row of readingRows) {
            const startValue = toNumber(row.start_value);
            const endValue = toNumber(row.end_value);
            const usedValue =
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
              used_value: usedValue
            });
          }

          const enrichedTrips = tripRows.map((trip) => {
            const tripReadings = readingMap[trip.id] || [];
            let totalKm = null;

            for (const reading of tripReadings) {
              const readingName = (reading.name || "").toString().toLowerCase();
              if (
                reading.used_value !== null &&
                (readingName.includes("km") ||
                  readingName.includes("kilometer") ||
                  readingName.includes("odometer"))
              ) {
                totalKm = reading.used_value;
                break;
              }
            }

            return {
              ...trip,
              total_km: totalKm,
              readings: tripReadings
            };
          });

          return res.json({
            message: "Trip history fetched",
            trips: enrichedTrips
          });
        }
      );
    }
  );
});

router.get("/active/current", authMiddleware, async (req, res) => {
  try {
    const activeTrip = await findActiveTripByDriver(req.user.id);

    if (!activeTrip) {
      return res.status(404).json({ message: "No active trip found" });
    }

    return res.json({
      trip: activeTrip
    });
  } catch (error) {
    console.error("Failed to fetch active trip:", error);
    return res.status(500).json({ message: "Unable to fetch active trip" });
  }
});
// ============================
// GET REQUIRED READINGS FOR TRIP
// ============================
router.get("/:tripId/readings", authMiddleware, (req, res) => {
  const { tripId } = req.params;

  // 1️⃣ Get vehicle from trip
  db.query(
    "SELECT vehicle_id FROM trips WHERE id = ?",
    [tripId],
    (err, tripResult) => {
      if (err) return res.status(500).json(err);

      if (tripResult.length === 0) {
        return res.status(404).json({ message: "Trip not found" });
      }

      const vehicleId = tripResult[0].vehicle_id;

      // 2️⃣ Get vehicle_type_id
      db.query(
        "SELECT vehicle_type_id FROM vehicles WHERE id = ?",
        [vehicleId],
        (err2, vehicleResult) => {
          if (err2) return res.status(500).json(err2);

          if (vehicleResult.length === 0) {
            return res.status(404).json({ message: "Vehicle not found" });
          }

          const vehicleTypeId = vehicleResult[0].vehicle_type_id;

          // 3️⃣ Get reading types
          db.query(
            `
            SELECT rt.id AS reading_type_id,
                   rt.name,
                   rt.unit
            FROM vehicle_type_readings vtr
            JOIN reading_types rt 
              ON vtr.reading_type_id = rt.id
            WHERE vtr.vehicle_type_id = ?
            `,
            [vehicleTypeId],
            (err3, readingResults) => {
              if (err3) return res.status(500).json(err3);

              res.json({
                readings: readingResults
              });
            }
          );
        }
      );
    }
  );
});
router.post(
  "/refuel",
  authMiddleware,
  upload.single("refuel_photo"),
  (req, res) => {
    const tripId = Number(req.body?.trip_id);
    const litre = Number(req.body?.litre);
    const photoPath = req.file ? req.file.path : null;

    if (Number.isNaN(tripId) || tripId <= 0 || Number.isNaN(litre) || litre <= 0) {
      return res.status(400).json({ message: "Trip and litre required" });
    }

    db.query(
      `SELECT id, driver_id, trip_status
       FROM trips
       WHERE id = ? LIMIT 1`,
      [tripId],
      (tripErr, tripRows) => {
        if (tripErr) {
          return res.status(500).json({ message: "Unable to record refuel" });
        }
        if (tripRows.length === 0) {
          return res.status(404).json({ message: "Trip not found" });
        }

        const trip = tripRows[0];
        if (trip.driver_id !== req.user.id) {
          return res.status(403).json({ message: "Unauthorized for this trip" });
        }
        if (trip.trip_status !== "STARTED") {
          return res.status(400).json({ message: "Trip is not active" });
        }

        db.query(
          `INSERT INTO trip_refuels (trip_id, litre, photo)
           VALUES (?, ?, ?)`,
          [tripId, litre, photoPath],
          (err) => {
            if (err) {
              return res.status(500).json({ message: "Unable to record refuel" });
            }

            res.json({ message: "Refuel recorded successfully" });
          }
        );
      }
    );
  }
);

// ============================
// UPDATE LIVE LOCATION
// ============================
router.post("/location", authMiddleware, (req, res) => {
  const driver_id = req.user.id;
  const { trip_id, latitude, longitude } = req.body;

  const lat = Number(latitude);
  const lng = Number(longitude);

  if (!trip_id || Number.isNaN(lat) || Number.isNaN(lng)) {
    return res.status(400).json({
      message: "trip_id, latitude and longitude are required"
    });
  }

  if (lat < -90 || lat > 90 || lng < -180 || lng > 180) {
    return res.status(400).json({
      message: "Invalid latitude/longitude range"
    });
  }

  db.query(
    `SELECT id, vehicle_id, driver_id, trip_status
     FROM trips
     WHERE id = ?`,
    [trip_id],
    (err, tripResults) => {
      if (err) return res.status(500).json(err);

      if (tripResults.length === 0) {
        return res.status(404).json({ message: "Trip not found" });
      }

      const trip = tripResults[0];

      if (trip.driver_id !== driver_id) {
        return res.status(403).json({ message: "Unauthorized for this trip" });
      }

      if (trip.trip_status !== "STARTED") {
        return res.status(400).json({ message: "Trip is not active" });
      }

      db.query(
        `INSERT INTO vehicle_locations (trip_id, vehicle_id, latitude, longitude)
         VALUES (?, ?, ?, ?)`,
        [trip.id, trip.vehicle_id, lat, lng],
        (err2, result) => {
          if (err2) {
            if (err2.code === "ER_NO_SUCH_TABLE") {
              return res.status(500).json({
                message: "Table vehicle_locations not found. Please create it before updating live location."
              });
            }

            return res.status(500).json(err2);
          }

          return res.json({
            message: "Location updated",
            location_id: result.insertId
          });
        }
      );
    }
  );
});

module.exports = router;
