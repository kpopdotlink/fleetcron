# FleetCron V3.0.0

<div align="center">
  <h3>Distributed Cron Job Management System</h3>
  <p>A robust, MongoDB-backed distributed scheduler for multi-machine environments</p>
</div>

---

## 🚀 Features

- **Distributed Execution** - User-defined order values with automatic minute-level coordination
- **Smart Failover** - Staggered execution with configurable offsets to ensure job completion
- **Dynamic Job Management** - Hot reload jobs without restarting agents
- **HTTP Action Chains** - Sequential execution of multiple HTTP requests with conditional logic
- **Template System** - Secure secrets management with `{{KEY}}` variable interpolation
- **Timezone Support** - Configurable timezone with automatic DST handling
- **Retry Logic** - Configurable retry policies with exponential backoff
- **Custom Headers** - Full HTTP header customization including User-Agent
- **Telegram Alerts** - Send silent routine updates and push-based failure notifications

## 🧭 Agent Ordering

- Each agent stores an `order` value in the `machines` collection (mirrored to the legacy `serial` field for compatibility).
- Update the order at runtime with `db.machines.updateOne({ machine_id: "..." }, { $set: { order: 20 } })`.
- The next scheduling cycle reloads the latest order automatically—no restart required.

## 📋 Prerequisites

- Python 3.7+
- MongoDB 3.6+
- NTP synchronization (recommended for multi-machine setups)

## 🔧 Installation

```bash
# Required dependencies
pip install pymongo requests pytz certifi

# Optional but recommended
pip install rich  # Beautiful console output
pip install cloudscraper  # Cloudflare bypass support

# For building standalone executable
pip install pyinstaller
```

## ⚙️ Configuration

1. Copy the example configuration:
```bash
cp fleetcron.config.example.json fleetcron.config.json
```

2. Edit `fleetcron.config.json`:
```json
{
  "mongodb_uri": "mongodb+srv://user:pass@cluster.mongodb.net/",
  "db_name": "fleetcron",
  "tz": "Asia/Seoul",
  "max_order": 10,
  "default_order": 9999,
  "order_field": "order",
  "secrets": {
    "BASE_URL": "https://api.example.com",
    "API_SECRET": "your-secret-key"
  }
}
```

## 🏃 Running the Agent

### Development
```bash
python agent.py
```

### Production (Standalone Binary)

⚠️ **Important**: Must include certifi CA bundle for SSL to work properly!

```bash
# Build with SSL certificates (REQUIRED)
pyinstaller --onefile --name fleetcron-agent \
  --add-data "$(python -c 'import certifi; print(certifi.where())'):certifi" \
  agent.py

# Windows build
pyinstaller --onefile --name fleetcron-agent ^
  --add-data "path\to\certifi\cacert.pem;certifi" ^
  agent.py

# More complete build (includes timezone data)
pyinstaller --onefile --name fleetcron-agent \
  --add-data "$(python -c 'import certifi; print(certifi.where())'):certifi" \
  --add-data "$(python -c 'import pytz; import os; print(os.path.dirname(pytz.__file__))'):pytz" \
  --hidden-import pytz \
  --hidden-import certifi \
  agent.py

# Run
./dist/fleetcron-agent
```

#### Alternative: Using spec file (Recommended)

Create `fleetcron.spec`:
```python
import certifi
import pytz
import os

a = Analysis(
    ['agent.py'],
    datas=[
        (certifi.where(), 'certifi'),
        (os.path.dirname(pytz.__file__), 'pytz'),
    ],
    hiddenimports=['pytz', 'certifi'],
)
pyz = PYZ(a.pure)
exe = EXE(
    pyz, a.scripts, a.binaries, a.datas, [],
    name='fleetcron-agent',
    debug=False,
    console=True,
)
```

Then build:
```bash
pyinstaller fleetcron.spec
```

## 🗄️ Database Setup

### Creating Jobs

Insert jobs into the MongoDB `jobs` collection:

```javascript
// Simple job - runs at specific time
db.jobs.insertOne({
  "name": "Daily Report",
  "enabled": true,
  "hour": 9,        // 9 AM in configured timezone
  "minute": 30,
  "method": "GET",
  "url": "{{BASE_URL}}/report",
  "headers": {
    "Authorization": "Bearer {{API_SECRET}}"
  },
  "timeout_sec": 30,
  "retry": {
    "retries": 3,
    "delay_sec": 5
  }
})

// Action chain - sequential execution
db.jobs.insertOne({
  "name": "Data Pipeline",
  "enabled": true,
  "hour": null,     // Run every hour
  "minute": 0,
  "actions": [
    {
      "type": "http",
      "name": "Fetch Data",
      "method": "GET",
      "url": "{{BASE_URL}}/api/data",
      "timeout_sec": 60
    },
    {
      "type": "http", 
      "name": "Process Data",
      "method": "POST",
      "url": "{{BASE_URL}}/api/process",
      "when": {
        "hour_in": [0, 12]  // Only at midnight and noon
      },
      "continue_on_failure": false
    }
  ]
})
```

### Job Schema

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Job identifier |
| `enabled` | boolean | Enable/disable without deletion |
| `hour` | number/null | Hour (0-23) or null for every hour |
| `minute` | number | Minute (0-59) |
| `actions` | array | Sequential HTTP actions to execute |
| `method` | string | HTTP method (GET, POST, etc.) |
| `url` | string | Target URL with template support |
| `headers` | object | HTTP headers with template support |
| `retry` | object | Retry configuration |
| `when` | object | Conditional execution rules |

## 🆕 Recent Fixes (v2.0)

### Major Bug Fixes
1. **Timezone Issues**: Fixed 9-hour time difference on non-development machines
   - Now uses pytz library for reliable timezone handling
   - Manual UTC+9 fallback for Korea timezone

2. **SSL Certificate Errors**: Resolved `CERTIFICATE_VERIFY_FAILED` errors
   - Certifi CA bundle must be included in PyInstaller builds
   - Automatic fallback to system CA certificates

3. **Machine Coordination**: Fixed race conditions in multi-machine setups
   - Serial > 1 machines now wait properly (10-second offset)
   - Improved heartbeat timing to prevent simultaneous execution

## 🔄 System Commands

```bash
# Reload jobs (all machines)
python agent.py reload

# Reload configuration
python agent.py reload-config

# Target specific machine
python agent.py reload <machine_id>
```

## 🖥️ System Service Setup

### macOS (launchd)

Create `~/Library/LaunchAgents/com.fleetcron.agent.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" 
  "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
  <key>Label</key>
  <string>com.fleetcron.agent</string>
  <key>ProgramArguments</key>
  <array>
    <string>/path/to/fleetcron-agent</string>
  </array>
  <key>RunAtLoad</key>
  <true/>
  <key>KeepAlive</key>
  <true/>
  <key>StandardOutPath</key>
  <string>/tmp/fleetcron.log</string>
  <key>StandardErrorPath</key>
  <string>/tmp/fleetcron.error.log</string>
</dict>
</plist>
```

```bash
# Load service
launchctl load ~/Library/LaunchAgents/com.fleetcron.agent.plist

# Check status
launchctl list | grep fleetcron
```

### Linux (systemd)

Create `/etc/systemd/system/fleetcron.service`:

```ini
[Unit]
Description=FleetCron Distributed Scheduler
After=network.target mongodb.service

[Service]
Type=simple
User=fleetcron
ExecStart=/usr/local/bin/fleetcron-agent
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

```bash
# Enable and start
sudo systemctl enable fleetcron
sudo systemctl start fleetcron
```

### Windows (Task Scheduler)

1. Open Task Scheduler
2. Create Basic Task → Name: "FleetCron Agent"
3. Trigger: "When the computer starts"
4. Action: Start a program → `C:\path\to\fleetcron-agent.exe`
5. Check "Run whether user is logged on or not"

## 🏗️ Architecture

### Serial Number Assignment
- Each machine receives a unique serial (1-N)
- Serial determines execution order
- Automatic reassignment on machine failure

### Execution Flow
1. **Scheduling**: Agent wakes at scheduled times only
2. **Coordination**: Updates heartbeat, checks serial order
3. **Leader Election**: Lowest available serial executes
4. **Failover**: Higher serials wait (serial-1)*10 seconds
5. **Execution**: Claim job, execute actions, record results

### Database Collections
- `machines` - Registered agents and heartbeats
- `serials` - Serial number assignments
- `jobs` - Job definitions and schedules
- `job_runs` - Execution history and status
- `commands` - Remote control commands

## 📊 Monitoring

Check agent status:
```javascript
// View active machines
db.machines.find({
  last_seen: { $gte: new Date(Date.now() - 120000) }
})

// Recent job runs
db.job_runs.find().sort({ scheduled_for: -1 }).limit(10)

// Failed jobs
db.job_runs.find({ status: "error" })
```

## 🔒 Security Considerations

- Store sensitive data in `secrets` configuration
- Use MongoDB connection string with TLS
- Implement proper network segmentation
- Rotate API keys regularly
- Monitor job_runs for anomalies

## 🤝 Contributing

Contributions are welcome! Please ensure:
- Code follows existing patterns
- Tests pass (when implemented)
- Documentation is updated
- Commit messages are descriptive

## 📄 License

This project is proprietary software. All rights reserved.

## 🆘 Troubleshooting

### Agent won't start
- Check MongoDB connectivity
- Verify configuration file exists
- Ensure no other instance running (`~/.fleetcron/agent.lock`)

### Jobs not executing
- Verify NTP synchronization
- Check job enabled status
- Review MongoDB connection
- Check serial assignment

### High CPU usage
- Jobs scheduled too frequently
- Reduce retry attempts
- Check for infinite retry loops

---

<div align="center">
  <p>Built with ❤️ for reliable distributed task scheduling</p>
</div>
