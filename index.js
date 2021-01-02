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
    .alias({
        h: 'help',
        m: 'mqtt-url',
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
        ]
    })
    .version()
    .help('help')
    .argv;
const Mqtt = require('mqtt');
const Influx = require('influx');

log.setLevel(config.verbosity);
log.info(pkg.name + ' ' + pkg.version + ' starting');
log.debug('loaded config: ', config);

const mqttUrl = new URL(config.mqttUrl);
log.debug('parsed mqttUrl: ', mqttUrl);

const influx = new Influx.InfluxDB({
    host: config.influxdbHost,
    port: config.influxdbPort,
    database: config.influxdbDatabase
});

// Workaround: Field type set to boolean if first written string is "true" or "false"
influx.writePoints([{
    measurement: Influx.escape.measurement(config.influxdbMeasurement),
    fields: {
        payload__string: 'dummy string'
    },
    timestamp: 0
}]).then(() => {
    log.debug('influx > dummy string');
}).catch(error => {
    log.warn('influx > dummy string', error.message);
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

    // Write Datapoint
    influx.writePoints([point]).then(() => {
        log.debug('influx >', point.measurement);
    }).catch(error => {
        log.warn('influx >', point.measurement, error.message);
    });
});
