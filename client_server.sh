curl -X POST http://localhost:8081/raft/put \
  -H "Content-Type: application/json" \
  -d '{"key": "name", "val": "rui"}'

curl -X GET http://localhost:8081/raft/get/username