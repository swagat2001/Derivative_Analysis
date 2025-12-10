#!/bin/bash

#############################################################################
# Derivative Analysis - Deployment Script
# Automated deployment with backup, rollback, and health checks
#############################################################################

set -e  # Exit on error

# Configuration
APP_NAME="derivative-analysis"
DEPLOY_DIR="/opt/${APP_NAME}"
BACKUP_DIR="${DEPLOY_DIR}/backups"
LOG_FILE="${DEPLOY_DIR}/logs/deployment.log"
VENV_DIR="${DEPLOY_DIR}/venv"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "${GREEN}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1" | tee -a "$LOG_FILE"
}

error() {
    echo -e "${RED}[$(date +'%Y-%m-%d %H:%M:%S')] ERROR:${NC} $1" | tee -a "$LOG_FILE"
}

warn() {
    echo -e "${YELLOW}[$(date +'%Y-%m-%d %H:%M:%S')] WARNING:${NC} $1" | tee -a "$LOG_FILE"
}

# Check if running as root
if [ "$EUID" -eq 0 ]; then
    error "Do not run this script as root"
    exit 1
fi

# Environment (dev or prod)
ENV=${1:-prod}
if [ "$ENV" != "dev" ] && [ "$ENV" != "prod" ]; then
    error "Usage: $0 [dev|prod]"
    exit 1
fi

log "Starting deployment to ${ENV} environment..."

# Pre-deployment checks
log "Running pre-deployment checks..."

# Check Python version
PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
log "Python version: ${PYTHON_VERSION}"

# Check PostgreSQL
if ! command -v psql &> /dev/null; then
    error "PostgreSQL client not found"
    exit 1
fi
log "PostgreSQL: OK"

# Check disk space (require at least 5GB free)
AVAILABLE_SPACE=$(df -BG "${DEPLOY_DIR}" | awk 'NR==2 {print $4}' | sed 's/G//')
if [ "$AVAILABLE_SPACE" -lt 5 ]; then
    error "Insufficient disk space. Available: ${AVAILABLE_SPACE}GB, Required: 5GB"
    exit 1
fi
log "Disk space: ${AVAILABLE_SPACE}GB available"

# Backup database
log "Creating database backup..."
python3 "${DEPLOY_DIR}/Database/backup_database_auto.py" || {
    error "Database backup failed"
    exit 1
}
log "Database backup completed"

# Stop application
log "Stopping application..."
if systemctl is-active --quiet ${APP_NAME}; then
    sudo systemctl stop ${APP_NAME}
    log "Application stopped via systemd"
else
    # Try to find and kill by PID
    PID=$(ps aux | grep "[r]un.py" | awk '{print $2}')
    if [ ! -z "$PID" ]; then
        kill $PID
        sleep 2
        log "Application stopped (PID: $PID)"
    fi
fi

# Backup current version
if [ -d "${DEPLOY_DIR}/current" ]; then
    log "Backing up current version..."
    mv "${DEPLOY_DIR}/current" "${DEPLOY_DIR}/previous"
fi

# Extract new version
log "Extracting new version..."
mkdir -p "${DEPLOY_DIR}/current"
tar -xzf derivative-analysis-*.tar.gz -C "${DEPLOY_DIR}/current" || {
    error "Failed to extract deployment package"
    # Rollback
    if [ -d "${DEPLOY_DIR}/previous" ]; then
        log "Rolling back to previous version..."
        rm -rf "${DEPLOY_DIR}/current"
        mv "${DEPLOY_DIR}/previous" "${DEPLOY_DIR}/current"
    fi
    exit 1
}

# Update virtual environment
log "Updating Python dependencies..."
cd "${DEPLOY_DIR}/current"

if [ ! -d "${VENV_DIR}" ]; then
    python3 -m venv "${VENV_DIR}"
fi

source "${VENV_DIR}/bin/activate"
pip install --upgrade pip
pip install -r requirements.txt || {
    error "Failed to install dependencies"
    # Rollback
    deactivate
    if [ -d "${DEPLOY_DIR}/previous" ]; then
        log "Rolling back to previous version..."
        rm -rf "${DEPLOY_DIR}/current"
        mv "${DEPLOY_DIR}/previous" "${DEPLOY_DIR}/current"
    fi
    exit 1
}
deactivate

# Run database migrations (if any)
if [ -d "${DEPLOY_DIR}/current/Database/migrations" ]; then
    log "Running database migrations..."
    # Add migration logic here
fi

# Start application
log "Starting application..."
sudo systemctl start ${APP_NAME} || {
    error "Failed to start application"
    # Rollback
    if [ -d "${DEPLOY_DIR}/previous" ]; then
        log "Rolling back to previous version..."
        sudo systemctl stop ${APP_NAME}
        rm -rf "${DEPLOY_DIR}/current"
        mv "${DEPLOY_DIR}/previous" "${DEPLOY_DIR}/current"
        sudo systemctl start ${APP_NAME}
    fi
    exit 1
}

# Wait for application to start
sleep 5

# Health check
log "Running health checks..."
for i in {1..5}; do
    HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:5000/health)
    if [ "$HTTP_CODE" -eq 200 ]; then
        log "Health check passed (HTTP $HTTP_CODE)"
        break
    else
        warn "Health check failed (attempt $i/5): HTTP $HTTP_CODE"
        if [ $i -eq 5 ]; then
            error "Health check failed after 5 attempts"
            # Rollback
            if [ -d "${DEPLOY_DIR}/previous" ]; then
                log "Rolling back to previous version..."
                sudo systemctl stop ${APP_NAME}
                rm -rf "${DEPLOY_DIR}/current"
                mv "${DEPLOY_DIR}/previous" "${DEPLOY_DIR}/current"
                sudo systemctl start ${APP_NAME}
            fi
            exit 1
        fi
        sleep 3
    fi
done

# Check logs for errors
ERROR_COUNT=$(grep -c "ERROR" "${DEPLOY_DIR}/logs/app.log" | tail -n 100 || echo 0)
if [ "$ERROR_COUNT" -gt 10 ]; then
    warn "Found ${ERROR_COUNT} errors in recent logs"
fi

# Cleanup old backups (keep last 10)
log "Cleaning up old backups..."
cd "${BACKUP_DIR}"
ls -t | tail -n +11 | xargs -r rm -f
log "Cleanup completed"

# Cleanup old deployment packages (keep last 5)
cd "${DEPLOY_DIR}"
ls -t derivative-analysis-*.tar.gz 2>/dev/null | tail -n +6 | xargs -r rm -f

log "Deployment to ${ENV} completed successfully!"
log "Application version: $(cat ${DEPLOY_DIR}/current/VERSION 2>/dev/null || echo 'unknown')"

exit 0
