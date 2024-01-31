#!/bin/bash
INSTANCE_NAME="neo4j-server"
PASSWORD="neo4jpwd"

# Get the directory of the current script
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Get the parent directory
PARENT_DIR="$(dirname "$SCRIPT_DIR")"


docker run \
--name $INSTANCE_NAME \
-p7474:7474 -p7687:7687 \
--user=$(id -u):$(id -g) \
--env NEO4J_dbms_connector_https_advertised__address="localhost:7473" \
--env NEO4J_dbms_connector_http_advertised__address="localhost:7474" \
--env NEO4J_dbms_connector_bolt_advertised__address="localhost:7687" \
--env NEO4J_AUTH=neo4j/$PASSWORD \
-v $PARENT_DIR/mount/neo4j/data:/data \
-v $PARENT_DIR/mount/neo4j/import:/var/lib/neo4j/import \
-v $PARENT_DIR/mount/neo4j/plugins:/plugins \
-e NEO4J_apoc_export_file_enabled=true \
-e NEO4J_apoc_import_file_enabled=true \
-e NEO4J_apoc_import_file_use__neo4j__config=true \
-e NEO4J_PLUGINS=\[\"apoc\"\] \
neo4j:latest
