#!/bin/bash
set -e

BASE_URL="http://localhost:8080"
ORDER_COUNT=50
PAYLOAD='{
  "customerId": "load-test-customer",
  "lineItems": [
    {"productId": "prod-load", "quantity": 1, "unitPrice": 9.99}
  ],
  "shippingAddress": {
    "street": "1 Load Test Ave",
    "city": "Toronto",
    "postalCode": "M1A 1A1",
    "country": "Canada"
  }
}'

echo "=== Order Processing Load Test ==="
echo "Placing $ORDER_COUNT orders..."

START_TIME=$(date +%s%3N)
ORDER_IDS=()

for i in $(seq 1 $ORDER_COUNT); do
  RESPONSE=$(curl -s -X POST "$BASE_URL/orders" \
    -H "Content-Type: application/json" \
    -d "$PAYLOAD")
  ID=$(echo "$RESPONSE" | grep -o '"id":"[^"]*"' | cut -d'"' -f4)
  ORDER_IDS+=("$ID")
done

END_TIME=$(date +%s%3N)
ELAPSED=$((END_TIME - START_TIME))

echo "Placed $ORDER_COUNT orders in ${ELAPSED}ms"
echo "Throughput: $(awk "BEGIN {printf \"%.1f\", $ORDER_COUNT * 1000 / $ELAPSED}") orders/sec"
echo ""
echo "Waiting 15 seconds for event processing..."
sleep 15

echo ""
echo "=== Metrics ==="
METRICS=$(curl -s "$BASE_URL/metrics")
echo "$METRICS" | python3 -m json.tool 2>/dev/null || echo "$METRICS"

echo ""
echo "=== Checking final order statuses ==="
FULFILLED=0
FAILED=0
PLACED=0

for ID in "${ORDER_IDS[@]}"; do
  STATUS=$(curl -s "$BASE_URL/orders/$ID/status" | tr -d '"')
  case $STATUS in
    FULFILLED)                FULFILLED=$((FULFILLED + 1)) ;;
    FAILED)                   FAILED=$((FAILED + 1)) ;;
    PLACED|PAYMENT_PROCESSED) PLACED=$((PLACED + 1)) ;;
  esac
done

echo "FULFILLED:        $FULFILLED / $ORDER_COUNT"
echo "FAILED (DLQ):     $FAILED / $ORDER_COUNT"
echo "STILL_PROCESSING: $PLACED / $ORDER_COUNT"
echo ""
echo "=== Done ==="
