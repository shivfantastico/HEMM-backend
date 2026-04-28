const express = require("express");
const fs = require("fs");
const multer = require("multer");
const path = require("path");

const router = express.Router();
const apkDirectory = path.join(process.cwd(), "uploads", "apk");
const apkFileName = "fleet-app.apk";
const apkFilePath = path.join(apkDirectory, apkFileName);
const metadataFilePath = path.join(apkDirectory, "latest.json");

fs.mkdirSync(apkDirectory, { recursive: true });

const storage = multer.diskStorage({
  destination(req, file, cb) {
    cb(null, apkDirectory);
  },
  filename(req, file, cb) {
    cb(null, apkFileName);
  },
});

const upload = multer({ storage });

function buildDownloadUrl(req) {
  return `${req.protocol}://${req.get("host")}/api/apk/download`;
}

function readMetadata() {
  if (!fs.existsSync(metadataFilePath)) {
    return null;
  }

  try {
    return JSON.parse(fs.readFileSync(metadataFilePath, "utf8"));
  } catch (error) {
    console.error("Failed to parse APK metadata:", error);
    return null;
  }
}

function writeMetadata(req, metadata) {
  const payload = {
    version_name: metadata.version_name,
    version_code: Number(metadata.version_code),
    changelog: metadata.changelog || "",
    force_update: Boolean(metadata.force_update),
    file_name: apkFileName,
    uploaded_at: new Date().toISOString(),
    download_url: buildDownloadUrl(req),
  };

  fs.writeFileSync(metadataFilePath, JSON.stringify(payload, null, 2), "utf8");
  return payload;
}

router.get("/apk/latest", (req, res) => {
  const metadata = readMetadata();

  if (!metadata || !fs.existsSync(apkFilePath)) {
    return res.status(404).json({
      message: "No APK release is available yet",
    });
  }

  return res.json({
    ...metadata,
    download_url: buildDownloadUrl(req),
  });
});

router.get("/apk/download", (req, res) => {
  if (!fs.existsSync(apkFilePath)) {
    return res.status(404).json({ message: "APK file not found" });
  }

  return res.download(apkFilePath, apkFileName);
});

router.post("/upload-apk", upload.single("file"), (req, res) => {
  const versionName = (req.body?.version_name || "").toString().trim();
  const versionCode = Number(req.body?.version_code);
  const changelog = (req.body?.changelog || "").toString().trim();
  const forceUpdateRaw = (req.body?.force_update || "")
    .toString()
    .trim()
    .toLowerCase();
  const forceUpdate = forceUpdateRaw === "true" || forceUpdateRaw === "1";

  if (!req.file) {
    return res.status(400).json({ message: "APK file is required" });
  }

  if (!versionName || Number.isNaN(versionCode) || versionCode <= 0) {
    return res.status(400).json({
      message: "version_name and numeric version_code are required",
    });
  }

  const metadata = writeMetadata(req, {
    version_name: versionName,
    version_code: versionCode,
    changelog,
    force_update: forceUpdate,
  });

  return res.json({
    message: "APK uploaded successfully",
    metadata,
  });
});

module.exports = router;
