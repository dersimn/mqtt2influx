Dumps MQTT messages to InfluxDB using InfluxQL (for InfluxDB < 2.0). Messages will be stored as raw strings. For a script that parses messages, take a look at [dersimn/mqsh2influx](https://github.com/dersimn/mqsh2influx).

## Usage

### Docker

```
docker run -d --restart=always --name=mqtt2influx \
    dersimn/mqtt2influx \
    --mqtt-url mqtt://10.1.1.50 \
    --influxdb-host 10.1.1.50 \
    --influxdb-port 8086 \
    --influxdb-database raw_mqtt
```

Run `docker run --rm dersimn/mqtt2influx -h` for a list of options.

## Development

### Build

Docker development build:

    docker build -t mqtt2influx .
    docker run --rm mqtt2influx -v debug --mqtt-url mqtt://host.docker.internal --influxdb-host host.docker.internal

Docker Hub deploy:

    docker buildx create --name mybuilder
    docker buildx use mybuilder
    docker buildx build --platform linux/amd64,linux/arm/v7 \
        -t dersimn/mqtt2influx \
        -t dersimn/mqtt2influx:2 \
        -t dersimn/mqtt2influx:2.x.x \
        --push .

### Testing

MQTT:

    docker run -d --name=mqtt -p 1883:1883 -p 9001:9001 -v "$(pwd)/contrib/mosquitto.conf":/mosquitto/config/mosquitto.conf:ro eclipse-mosquitto

InfluxDB:

    docker run -d --name=influxdb -p 8086:8086 -e INFLUXDB_DB=mqtt influxdb:1.8

Grafana:

    docker run -d --name=grafana -p 3000:3000 -e "GF_SERVER_ROOT_URL=http://10.1.1.100:3000" -e "GF_USERS_ALLOW_SIGN_UP=false" -e "GF_USERS_DEFAULT_THEME=light" -e "GF_AUTH_ANONYMOUS_ENABLED=true" -e "GF_AUTH_BASIC_ENABLED=false" -e "GF_AUTH_ANONYMOUS_ORG_ROLE=Admin" grafana/grafana
