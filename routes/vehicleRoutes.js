const express = require("express");
const router = express.Router();
const db = require("../config/db");
const authMiddleware = require("../middleware/authMiddleware");
router.post("/by-qr", authMiddleware, (req, res) => {
  const rawQrValue = (req.body?.qr_value || "").toString();
  const qrValue = rawQrValue.trim().toUpperCase().replace(/[^A-Z0-9]/g, "");

  if (!qrValue) {
    return res.status(400).json({ message: "QR value required" });
  }

  db.query(
    `SELECT v.id, v.vehicle_number, v.vehicle_type_id, vt.name AS vehicle_type
     FROM vehicles v
     LEFT JOIN vehicle_types vt ON v.vehicle_type_id = vt.id
     WHERE REPLACE(
             REPLACE(
               REPLACE(
                 REPLACE(
                   REPLACE(UPPER(TRIM(v.vehicle_number)), ' ', ''),
                   '-',
                   ''
                 ),
                 '/',
                 ''
               ),
               '.',
               ''
             ),
             '_',
             ''
           ) = ?
       AND UPPER(TRIM(COALESCE(v.status, 'ACTIVE'))) = 'ACTIVE'`,
    [qrValue],
    (err, vehicleResults) => {
      if (err) return res.status(500).json(err);

      if (vehicleResults.length === 0) {
        return res.status(404).json({ message: "Vehicle not found" });
      }

      const vehicle = vehicleResults[0];
      if (!vehicle.vehicle_type_id) {
        return res.json({
          vehicle: {
            id: vehicle.id,
            vehicle_number: vehicle.vehicle_number,
            vehicle_type: vehicle.vehicle_type || "",
            readings_required: []
          }
        });
      }

      db.query(
        `SELECT rt.id, rt.name, rt.unit
         FROM vehicle_type_readings vtr
         JOIN reading_types rt ON vtr.reading_type_id = rt.id
         WHERE vtr.vehicle_type_id = ?`,
        [vehicle.vehicle_type_id],
        (err2, readingResults) => {
          if (err2) return res.status(500).json(err2);

          res.json({
            vehicle: {
              id: vehicle.id,
              vehicle_number: vehicle.vehicle_number,
              vehicle_type: vehicle.vehicle_type,
              readings_required: readingResults
            }
          });
        }
      );
    }
  );
});

module.exports = router;
