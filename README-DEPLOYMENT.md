# RINGTS Deployment Guide

## Prerequisites

- Docker installed
- Docker Compose installed
- Port 5000 available

## Quick Start

### Option 1: Using Docker Compose (Recommended)

```bash
# Build and start
docker-compose up -d

# View logs
docker-compose logs -f

# Stop
docker-compose down
```

### Option 2: Using Docker directly

```bash
# Build image
docker build -t ringts-app .

# Run container
docker run -d \
  -p 5000:5000 \
  -v $(pwd)/data:/app/data \
  --name ringts \
  ringts-app

# View logs
docker logs -f ringts

# Stop container
docker stop ringts
docker rm ringts
```

### Option 3: Using deploy script

```bash
# Make script executable
chmod +x deploy.sh

# Run deployment
./deploy.sh
```

## Configuration

### Environment Variables

Copy `.env.example` to `.env` and adjust as needed:

```bash
cp .env.example .env
```

### Data Persistence

The `data/` directory is mounted as a volume to persist CSV files between container restarts.

## Production Deployment

### Using Gunicorn (Included in Dockerfile)

The application runs with Gunicorn by default with:
- 2 workers
- 2 threads per worker
- 120 second timeout

### Using nginx as Reverse Proxy

1. Install nginx on your host

2. Create nginx config (`/etc/nginx/sites-available/ringts`):

```nginx
server {
    listen 80;
    server_name your-domain.com;

    location / {
        proxy_pass http://localhost:5000;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        
        # For Server-Sent Events
        proxy_buffering off;
        proxy_cache off;
        proxy_read_timeout 86400;
    }

    location /static {
        proxy_pass http://localhost:5000/static;
    }
}
```

3. Enable and restart nginx:

```bash
sudo ln -s /etc/nginx/sites-available/ringts /etc/nginx/sites-enabled/
sudo nginx -t
sudo systemctl restart nginx
```

### SSL with Let's Encrypt

```bash
sudo apt install certbot python3-certbot-nginx
sudo certbot --nginx -d your-domain.com
```

## Monitoring

### Check application status

```bash
# Container logs
docker-compose logs -f ringts

# Application health
curl http://localhost:5000/api/scraper-status
```

### Resource usage

```bash
# Docker stats
docker stats ringts-app
```

## Troubleshooting

### Container won't start

```bash
# Check logs
docker-compose logs ringts

# Check if port is in use
sudo netstat -tlnp | grep 5000
```

### Data not persisting

Ensure the `data/` directory exists and has proper permissions:

```bash
mkdir -p data
chmod 755 data
```

### High memory usage

Adjust worker/thread count in Dockerfile CMD or docker-compose.yml:

```yaml
command: gunicorn --bind 0.0.0.0:5000 --workers 1 --threads 2 flask_app:app
```

## Updating

```bash
# Pull latest changes
git pull

# Rebuild and restart
docker-compose up -d --build
```

## Backup

### Backup data files

```bash
# Create backup
tar -czf ringts-backup-$(date +%Y%m%d).tar.gz data/

# Restore backup
tar -xzf ringts-backup-YYYYMMDD.tar.gz
```

## Scaling

To run multiple instances behind a load balancer:

```yaml
services:
  ringts:
    # ... existing config ...
    deploy:
      replicas: 3
```

Note: Background scraper should only run in one instance. Consider separating scraper from web server for multi-instance deployments.

## Support

For issues, check:
1. Container logs: `docker-compose logs -f`
2. Application logs in console tab
3. Health check: `curl http://localhost:5000/api/scraper-status`
