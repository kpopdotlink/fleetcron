# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

FleetCron Agent v2 - A distributed cron job management system with MongoDB backend. This is a single-file Python application designed for multi-machine job scheduling and execution.

## Key Commands

### Installation
```bash
# Install required dependencies
pip install pymongo requests
```

### Running the Agent
```bash
# Run the agent (requires fleetcron.config.json)
python agent.py
```

### Configuration
The agent expects a `fleetcron.config.json` file in one of these locations:
1. Same directory as agent.py
2. `~/.fleetcron/fleetcron.config.json`

## Architecture

### Single File Structure
- `agent.py` - Complete application in a single file (416 lines)
  - Configuration loading and management
  - MongoDB connection and database operations
  - Job scheduling and execution engine
  - Distributed coordination with serial number assignment
  - HTTP action execution with retry logic
  - Command polling system for remote control

### Core Components

1. **Configuration System** (`load_config()` - agent.py:29)
   - Loads from `fleetcron.config.json`
   - Supports secret interpolation with `{{KEY}}` template syntax
   - Machine ID stored in `~/.fleetcron/machine.json`

2. **Database Layer**
   - MongoDB collections: `machines`, `serials`, `jobs`, `job_runs`, `commands`
   - Automatic index creation for performance optimization
   - Connection string from config or environment variable

3. **Job Scheduling**
   - Minute-based cron scheduling with timezone support
   - Support for single or multiple schedules per job
   - Leader-only or all-machines execution modes

4. **Action Execution** (`execute_actions()` - agent.py:284)
   - Sequential HTTP request chains
   - Retry logic with configurable attempts and delays
   - Response validation and conditional flow

5. **Distributed Coordination**
   - Serial number assignment (1 to N machines)
   - Process locking via `~/.fleetcron/agent.lock`
   - Heartbeat mechanism for online status
   - Leader election based on lowest serial number

6. **Command System**
   - Remote command execution via MongoDB polling
   - Supports: job reload, config refresh, targeted commands
   - Commands polled every 10 seconds by non-leader machines

### Key Design Patterns

- **Single Instance**: File locking ensures one agent per machine
- **Hot Reload**: Jobs and config can be updated without restart
- **Fault Tolerance**: Comprehensive error handling and retry mechanisms
- **Template System**: Secrets management through variable interpolation
- **Time Handling**: Timezone-aware scheduling (default: Asia/Seoul)

## Development Notes

- All comments and documentation in the code are in Korean
- No test files currently exist in the repository
- External dependencies: `pymongo`, `requests`
- Python 3.7+ required (uses `zoneinfo` with `pytz` fallback)
- Cross-platform file locking (Windows and Unix support)