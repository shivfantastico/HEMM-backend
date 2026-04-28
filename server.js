require("dotenv").config();

const express = require("express");
const cors = require("cors");

const db = require("./config/db");

const app = express();
const port = Number(process.env.PORT || 5005);
const apkRoutes = require("./routes/apk.routes");

app.use(cors());
app.use(express.json({ limit: "10mb" }));
app.use(express.urlencoded({ extended: true, limit: "10mb" }));
app.use("/uploads", express.static("uploads"));

app.use("/api", apkRoutes);
app.use("/api/auth", require("./routes/authRoutes"));
app.use("/api/vehicle", require("./routes/vehicleRoutes"));
app.use("/api/trip", require("./routes/tripRoutes"));
app.use("/api/admin", require("./routes/adminrRoutes"));

db.getConnection((err, connection) => {
  if (err) {
    console.log("Database connection failed:", err.message);
    return;
  }

  console.log("Database Connected Successfully");
  connection.release();
});

app.get("/", (req, res) => {
  res.send("Fleet Backend Running");
});

app.use((req, res) => {
  res.status(404).json({ message: "Route not found" });
});

app.use((err, req, res, next) => {
  console.error("Unhandled server error:", err);

  if (res.headersSent) {
    return next(err);
  }

  return res.status(500).json({
    message: "Internal server error",
  });
});

app.listen(port, () => {
  console.log(`Server running on port ${port}`);
});
