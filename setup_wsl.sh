#!/usr/bin/env bash

set -e

echo "Updating system packages..."
sudo apt update && sudo apt upgrade -y

echo "Installing Python, pip, and venv..."
sudo apt install -y python3 python3-pip python3-venv

echo "Ensuring Git is installed..."
sudo apt install -y git

echo "WSL environment ready."