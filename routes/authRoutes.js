const express = require("express");
const bcrypt = require("bcrypt");
const jwt = require("jsonwebtoken");

const db = require("../config/db");

const router = express.Router();
const jwtSecret = process.env.JWT_SECRET || "SECRET_KEY";

router.post("/register", async (req, res) => {
  const name = (req.body?.name || "").trim();
  const mobile = (req.body?.mobile || "").trim();
  const username = (req.body?.username || req.body?.user_id || "").trim();
  const password = req.body?.password || "";

  if (!name || !mobile || !username || !password) {
    return res.status(400).json({
      message: "Name, mobile, username and password are required",
    });
  }

  if (password.length < 4) {
    return res.status(400).json({
      message: "Password must be at least 4 characters",
    });
  }

  db.query(
    "SELECT id FROM drivers WHERE username = ? OR mobile = ? LIMIT 1",
    [username, mobile],
    async (err, existing) => {
      if (err) {
        return res.status(500).json({ message: "Unable to register driver" });
      }

      if (existing.length > 0) {
        return res.status(409).json({
          message: "Driver already exists with this username or mobile",
        });
      }

      try {
        const hashedPassword = await bcrypt.hash(password, 10);

        db.query(
          `INSERT INTO drivers (name, mobile, username, password, role, status)
           VALUES (?, ?, ?, ?, 'DRIVER', 'ACTIVE')`,
          [name, mobile, username, hashedPassword],
          (insertErr, result) => {
            if (insertErr) {
              return res.status(500).json({
                message: "Unable to register driver",
              });
            }

            return res.status(201).json({
              message: "Driver registered successfully",
              driver: {
                id: result.insertId,
                name,
                username,
                mobile,
                role: "DRIVER",
                status: "ACTIVE",
              },
            });
          }
        );
      } catch (_) {
        return res.status(500).json({
          message: "Unable to register driver",
        });
      }
    }
  );
});

router.post("/login", (req, res) => {
  const { username, password } = req.body || {};

  if (!username || !password) {
    return res.status(400).json({
      message:
        "Username & Password required. Send JSON body like {\"username\":\"...\",\"password\":\"...\"}.",
    });
  }

  db.query(
    "SELECT * FROM drivers WHERE username = ? AND status = 'ACTIVE'",
    [username],
    async (err, results) => {
      if (err) {
        return res.status(500).json({ message: "Unable to process login" });
      }

      if (results.length === 0) {
        return res.status(401).json({ message: "Invalid credentials" });
      }

      try {
        const user = results[0];
        const isMatch = await bcrypt.compare(password, user.password);

        if (!isMatch) {
          return res.status(401).json({ message: "Invalid credentials" });
        }

        const token = jwt.sign(
          {
            id: user.id,
            role: user.role,
            name: user.name,
          },
          jwtSecret,
          { expiresIn: "7d" }
        );

        return res.json({
          token,
          user: {
            id: user.id,
            name: user.name,
            role: user.role,
          },
          driver: {
            id: user.id,
            name: user.name,
            role: user.role,
          },
        });
      } catch (_) {
        return res.status(500).json({ message: "Unable to process login" });
      }
    }
  );
});

module.exports = router;
