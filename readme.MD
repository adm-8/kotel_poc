```
docker network create kotel-network
```

```
sudo docker build -t kotel-pyflink ./pyflink && \
docker run --rm --name kotel-pyflink --network=kotel-network kotel-pyflink
```

```
sudo docker compose up  --force-recreate --build
```

```
docker exec -it kotel-kafka /bin/sh

docker exec -it kotel-kafka /usr/bin/kafka-console-consumer --bootstrap-server localhost:9092 --topic customer_offer_state_processed --from-beginning

```