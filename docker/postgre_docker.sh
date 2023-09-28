#!/bin/bash
INSTANCE_NAME="postgre-server"
PASSWORD="pgpwd"

# Get the directory of the current script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Get the parent directory
PARENT_DIR="$(dirname "$SCRIPT_DIR")"



docker run \
  --name $INSTANCE_NAME \
  -p 5432:5432 \
  -e POSTGRES_PASSWORD=$PASSWORD \
  -d \
  -u 1000 \
  -v $PARENT_DIR/mount/postgres/data:/var/lib/postgresql/data \
  postgres:latest
