#!/bin/bash

# RINGTS Deployment Script

echo "🚀 Starting RINGTS deployment..."

# Stop and remove existing containers
echo "📦 Stopping existing containers..."
docker-compose down

# Build the Docker image
echo "🔨 Building Docker image..."
docker-compose build

# Start the services
echo "▶️  Starting services..."
docker-compose up -d

# Show logs
echo "📋 Showing logs (Ctrl+C to exit)..."
docker-compose logs -f
