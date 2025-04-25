
```
docker volume create pg_malone
docker run -d \
	--name pg_malone_docker \
	-e POSTGRES_USER=postgres \
	-e POSTGRES_PASSWORD=postgres \
	-e POSTGRES_DB=pg_malone \
	-p 15432:5432 \
	-v pg_malone:/var/lib/postgresql/data \
	-v /Users/mac/pg_malone:/data/malone \
	postgres:latest

docker ps
docker exec -it pg_malone_docker psql -U postgres -d postgres
```

Next, connect to posgres on port 15432 uing any PG Client. 

