curl -X POST http://localhost:8081/raft/put \
  -H "Content-Type: application/json" \
  -d '{"key": "age", "val": "ding"}'

curl -X GET http://localhost:8081/raft/get/username \
  -H "Content-Type: application/json"