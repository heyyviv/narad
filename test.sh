#!/bin/bash

echo "Sending log to Redis stream..."
docker-compose exec redis redis-cli XADD logiq:logs "*" payload '{"service": "payment-svc", "level": "ERROR", "code": "PAYMENT_DECLINED", "msg": "Card authorization failed at gateway", "dims": {"customer_id": "cust_821", "txn_id": "txn_992abc", "order_id": "ord_443"}}'

echo "Waiting for worker to process..."
sleep 2

echo "Querying logs by customer_id..."
curl -s "http://localhost:8080/v1/logs?dim.customer_id=cust_821"
echo -e "\n"

echo "Querying trace by order_id..."
curl -s "http://localhost:8080/v1/trace/order_id/ord_443"
echo -e "\n"
