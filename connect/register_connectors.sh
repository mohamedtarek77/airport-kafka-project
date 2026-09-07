#!/bin/bash
# تسجيل الـ Connectors على Kafka Connect (لو شغّلت Kafka Connect على بورت 8083)

CONNECT_URL="http://localhost:8083/connectors"

echo "=== Registering flight-db-source ==="
curl -X POST -H "Content-Type: application/json" \
  --data @flight-db-source-connector.json \
  $CONNECT_URL

echo ""
echo "=== Registering baggage-elasticsearch-sink ==="
curl -X POST -H "Content-Type: application/json" \
  --data @baggage-elasticsearch-sink-connector.json \
  $CONNECT_URL
