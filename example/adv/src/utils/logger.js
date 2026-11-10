const fs = require("fs");
const path = require("path");

const logDir = path.resolve(__dirname, "../../logs");
const logFile = path.join(logDir, "integration-test.log");

fs.mkdirSync(logDir, { recursive: true });

function write(level, message, details = {}) {
  const line = JSON.stringify({
    timestamp: new Date().toISOString(),
    level,
    message,
    details
  });

  fs.appendFileSync(logFile, line + "\n");
  console.log(line);
}

function clearLog() {
  if (fs.existsSync(logFile)) {
    fs.truncateSync(logFile, 0);
  }
}

module.exports = {
  info: (message, details) => write("INFO", message, details),
  warn: (message, details) => write("WARN", message, details),
  error: (message, details) => write("ERROR", message, details),
  clearLog,
  logFile
};
