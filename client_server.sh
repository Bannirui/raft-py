curl -X POST http://localhost:8081/raft/put \
  -H "Content-Type: application/json" \
  -d '{"key": "username", "val": "ding"}'

curl -X GET http://localhost:8081/raft/get/1 \
  -H "Content-Type: application/json"