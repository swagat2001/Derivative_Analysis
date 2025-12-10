# CI/CD Setup Guide
## Derivative Analysis - NSE Options Dashboard

---

## 🎯 Quick Start (5 Minutes)

### For UI Developer:

```bash
# 1. Setup environment (Windows - Git Bash or WSL)
bash setup-dev.sh

# 2. Create feature branch
git checkout -b feature/my-feature

# 3. Start development
python run.py
# Visit: http://localhost:5000

# 4. Test health endpoint
# Visit: http://localhost:5000/health

# 5. Make changes, commit, push
git add .
git commit -m "feat(ui): my new feature"
git push origin feature/my-feature
```

---

## 📂 Files Added to Your Project

```
Derivative_Analysis/
├── .github/
│   └── workflows/
│       └── ci-cd.yml                  ✅ GitHub Actions pipeline
│
├── Analysis_Tools/app/
│   ├── __init__.py                    ✅ UPDATED (added health check)
│   └── health_check.py                ✅ NEW Health monitoring
│
├── .env.template                      ✅ Environment variables template
├── .gitignore                         ✅ UPDATED Enhanced gitignore
├── .pre-commit-config.yaml            ✅ Code quality hooks
├── Dockerfile                         ✅ Container configuration
├── docker-compose.yml                 ✅ Multi-container setup
├── deploy.sh                          ✅ Server deployment script
├── setup-dev.sh                       ✅ Developer setup script
└── derivative-analysis.service        ✅ Systemd service
```

---

## 🚀 What Each File Does

### 1. **`.github/workflows/ci-cd.yml`**
**GitHub Actions Pipeline - Automated CI/CD**

**Runs automatically on:**
- Push to `main`, `develop`, `feature/*`
- Pull requests

**Pipeline stages:**
1. Code Quality (flake8, pylint, black, isort, bandit)
2. Frontend Build (HTML/CSS/JS validation)
3. Backend Tests (pytest with PostgreSQL)
4. Build (creates deployment package)
5. Deploy Dev (auto-deploy on develop branch)
6. Deploy Prod (manual approval on main branch)

**Total time:** ~10-15 minutes

---

### 2. **`Analysis_Tools/app/health_check.py`**
**Health Monitoring Endpoint**

**URL:** `http://localhost:5000/health`

**Returns:**
```json
{
  "status": "healthy",
  "timestamp": "2024-12-10T10:00:00Z",
  "version": "v1.0.0",
  "checks": {
    "database": "connected",
    "cache": "latest: 2024-12-09",
    "filesystem": "writable"
  }
}
```

**Usage:**
- CI/CD pipeline health checks
- Monitoring systems
- Deployment verification
- Load balancer health probes

---

### 3. **`setup-dev.sh`**
**One-Command Developer Setup**

**What it does:**
1. Checks Python, Git, PostgreSQL
2. Creates virtual environment
3. Installs dependencies
4. Installs dev tools (black, flake8, pytest)
5. Sets up pre-commit hooks
6. Creates .env from template
7. Creates necessary directories

**Usage:**
```bash
bash setup-dev.sh
```

---

### 4. **`deploy.sh`**
**Automated Server Deployment**

**What it does:**
1. Pre-deployment checks
2. Database backup
3. Stop application
4. Extract new version
5. Update dependencies
6. Run migrations
7. Start application
8. Health checks
9. **Automatic rollback on failure**

**Usage:**
```bash
./deploy.sh prod  # Production
./deploy.sh dev   # Development
```

---

### 5. **`.env.template`**
**Environment Configuration Template**

**Contains:**
- Flask configuration
- Database credentials
- Server settings
- Security settings
- Feature flags

**Setup:**
```bash
cp .env.template .env
# Edit .env with your actual values
```

---

### 6. **`.gitignore`**
**Enhanced Git Ignore Rules**

**Added:**
- CI/CD artifacts (`deployment/`, `*.tar.gz`)
- Docker overrides
- Test coverage
- Secrets (`.env`, `*.key`)
- Keep templates (`!.env.template`)

---

### 7. **`.pre-commit-config.yaml`**
**Local Code Quality Checks**

**Runs automatically before commit:**
- Code formatting (black, isort)
- Linting (flake8)
- Security (bandit, detect-secrets)
- General checks (trailing whitespace, large files)

**Setup:**
```bash
pip install pre-commit
pre-commit install
```

---

### 8. **`Dockerfile`**
**Container Image Configuration**

**Multi-stage build:**
- Stage 1: Install dependencies
- Stage 2: Runtime image

**Features:**
- Non-root user
- Health check
- Optimized layers
- Small image size

**Usage:**
```bash
docker build -t derivative-analysis .
docker run -p 5000:5000 derivative-analysis
```

---

### 9. **`docker-compose.yml`**
**Multi-Container Orchestration**

**Services:**
- **postgres**: PostgreSQL 16
- **web**: Flask application
- **redis**: Caching layer
- **nginx**: Reverse proxy (production)
- **pgadmin**: Database UI (development)

**Usage:**
```bash
# Development
docker-compose up -d

# Production
docker-compose --profile production up -d

# With pgAdmin
docker-compose --profile development up -d
```

---

### 10. **`derivative-analysis.service`**
**Systemd Service Configuration**

**Features:**
- Auto-restart on failure
- Resource limits (4GB RAM, 200% CPU)
- Security hardening
- Separate log files

**Installation:**
```bash
sudo cp derivative-analysis.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable derivative-analysis
sudo systemctl start derivative-analysis
```

---

## 🔄 Complete Workflow

### Daily Development:

```bash
# 1. Get latest code
git checkout develop
git pull

# 2. Create feature branch
git checkout -b feature/dashboard-redesign

# 3. Make changes
# Edit: Analysis_Tools/app/static/css/dashboard.css
# Edit: Analysis_Tools/app/views/dashboard.html
# Edit: Analysis_Tools/app/static/js/dashboard.js

# 4. Test locally
python run.py
# Visit: http://localhost:5000

# 5. Pre-commit hooks run automatically
git add .
git commit -m "feat(ui): redesign dashboard layout"

# 6. Push (triggers CI/CD pipeline)
git push origin feature/dashboard-redesign

# 7. Create Pull Request on GitHub
# Pipeline runs:
#   - Code quality checks
#   - Frontend validation
#   - Backend tests
#   - Build package

# 8. Code review
# Team reviews your changes

# 9. Merge to develop
# Auto-deploys to development server

# 10. Test on dev server
# http://dev-server:5000

# 11. Merge develop to main
# Manual approval required
# Deploys to production server
```

---

## ⚙️ Configuration

### GitHub Secrets (Required):

Go to: **GitHub → Settings → Secrets → Actions**

```
DB_PASSWORD           = Your production database password
SSH_PRIVATE_KEY       = SSH key for server access
PROD_SERVER_HOST      = Production server IP (e.g., 192.168.1.100)
PROD_SERVER_USER      = SSH username (e.g., appuser)
DEV_SERVER_HOST       = Development server IP (optional)
DEV_SERVER_USER       = SSH username (optional)
```

### GitHub Environments:

Go to: **GitHub → Settings → Environments**

1. Create **"development"** environment
2. Create **"production"** environment
   - Add protection rules:
     - Required reviewers: 1
     - Wait timer: 5 minutes

---

## 📊 Monitoring

### Health Check:
```bash
curl http://localhost:5000/health
```

### Application Logs:
```bash
# Real-time
tail -f logs/app.log

# Errors only
grep "ERROR" logs/app.log

# Last 100 lines
tail -n 100 logs/app.log
```

### Service Status:
```bash
# Check status
sudo systemctl status derivative-analysis

# View logs
sudo journalctl -u derivative-analysis -f

# Restart
sudo systemctl restart derivative-analysis
```

---

## 🐛 Troubleshooting

### Pipeline Fails:

```bash
# Run checks locally
pre-commit run --all-files

# Fix formatting
black Analysis_Tools/ Database/ run.py
isort Analysis_Tools/ Database/ run.py

# Run tests
pytest tests/ -v
```

### Deployment Fails:

```bash
# Check logs
tail -f logs/deployment.log

# Manual rollback
cd /opt/derivative-analysis
sudo systemctl stop derivative-analysis
mv current current-failed
mv previous current
sudo systemctl start derivative-analysis
```

### Health Check Fails:

```bash
# Check application
curl http://localhost:5000/health

# Check database
python Database/final_inspector.py

# Check logs
tail -f logs/app.log
```

---

## ✅ Next Steps

1. **Test Health Endpoint:**
   ```bash
   python run.py
   curl http://localhost:5000/health
   ```

2. **Commit Changes:**
   ```bash
   git add .
   git commit -m "ci: add CI/CD pipeline"
   git push
   ```

3. **Configure GitHub:**
   - Add secrets
   - Create environments
   - Enable Actions

4. **Test Pipeline:**
   - Create test branch
   - Make small change
   - Push and watch Actions tab

---

## 📞 Support

- **Documentation:** See this file
- **Pipeline Status:** GitHub → Actions tab
- **Health Check:** http://localhost:5000/health
- **Logs:** `logs/` directory

---

**Everything is ready! Start developing and push your code.** 🚀
