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