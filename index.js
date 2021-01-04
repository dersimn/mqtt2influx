#!/usr/bin/env node

const pkg = require('./package.json');
const log = require('yalm');
const config = require('yargs')
    .env('MQTT2INFLUX')
    .usage(pkg.name + ' ' + pkg.version + '\n' + pkg.description + '\n\nUsage: $0 [options]')
    .describe('verbosity', 'possible values: "error", "warn", "info", "debug"')
    .describe('mqtt-url', 'mqtt broker url. See https://github.com/mqttjs/MQTT.js#connect-using-a-url')
    .describe('broker-name')
    .describe('influxdb-host')
    .describe('influxdb-port')
    .describe('influxdb-database')
    .describe('influxdb-measurement')
    .describe('subscription', 'array of topics to subscribe').array('subscription')
    .describe('chunk-size', 'maximum number of points to buffer before writing to InfluxDB')
    .describe('max-interval', 'maximum time to wait if chunk size is not completely filled before writing to InfluxDB anyway')
    .alias({
        h: 'help',
        m: 'mqtt-url',
        i: 'influxdb-host',
        v: 'verbosity'
    })
    .default({
        name: 'mqtt2influx',
        'mqtt-url': 'mqtt://127.0.0.1',
        'influxdb-host': '127.0.0.1',
        'influxdb-port': 8086,
        'influxdb-database': 'raw_mqtt',
        'influxdb-measurement': 'data',
        subscription: [
            '#'
        ],
        'chunk-size': 5000,
        'max-interval': 3
    })
    .version()
    .help('help')
    .argv;
const Mqtt = require('mqtt');
const Influx = require('influx');

log.setLevel(config.verbosity);
log.info(pkg.name + ' ' + pkg.version + ' starting');
log.debug('loaded config: ', config);

const pointBuffer = [];

const mqttUrl = new URL(config.mqttUrl);
log.debug('parsed mqttUrl: ', mqttUrl);

const influx = new Influx.InfluxDB({
    host: config.influxdbHost,
    port: config.influxdbPort,
    database: config.influxdbDatabase,
    schema: [
        {
            measurement: Influx.escape.measurement(config.influxdbMeasurement),
            fields: {
                payload__string: Influx.FieldType.STRING
            },
            tags: [
                'host',
                'port',
                'username',
                'broker_name',
                'topic',
                'retain'
            ]
        }
    ]
});

log.info('mqtt init');
const mqtt = Mqtt.connect(config.mqttUrl);

mqtt.on('connect', () => {
    log.info('mqtt connected');

    config.subscription.forEach(topic => {
        log.info('mqtt subscribe ' + topic);
        mqtt.subscribe(topic);
    });
});

mqtt.on('close', () => {
    log.warn('mqtt closed');
});

mqtt.on('error', err => {
    log.error('mqtt error', err.message);
});

mqtt.on('message', (topic, message, packet) => {
    const point = {
        measurement: Influx.escape.measurement(config.influxdbMeasurement),
        fields: {
            payload__string: String(message)
        },
        tags: {
            ...mqttUrl.host && {host: mqttUrl.host},
            ...mqttUrl.port && {port: mqttUrl.port},
            ...mqttUrl.username && {username: mqttUrl.username},
            ...config.brokerName && {broker_name: config.brokerName},
            topic,
            retain: packet.retain
        }
    };

    pointBuffer.push(point);

    if (pointBuffer.length > config.chunkSize) {
        setImmediate(write);
    }
});

const writeInterval = setInterval(write, config.maxInterval * 1000);
function write() {
    const chunk = pointBuffer.splice(0, config.chunkSize);
    if (chunk.length === 0) {
        return;
    }

    // Write Datapoints
    influx.writePoints(chunk).then(() => {
        log.debug('influx >', chunk.length);
    }).catch(error => {
        log.warn('influx >', chunk.length, error.message);
    });
}

process.on('SIGINT', () => {
    log.info('received SIGINT');
    stop();
});
process.on('SIGTERM', () => {
    log.info('received SIGTERM');
    stop();
});
async function stop() {
    clearInterval(writeInterval);
    mqtt.end();
    try {
        await influx.writePoints(pointBuffer);
        log.debug('influx >', pointBuffer.length);
    } catch (error) {
        log.error('influx >', pointBuffer.length, error.message);
    }

    log.debug('exiting..');
    process.exit(0);
}
