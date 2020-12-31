#!/usr/bin/env node

const pkg = require('./package.json');
const log = require('yalm');
const config = require('yargs')
    .env('MQTT2INFLUX')
    .usage(pkg.name + ' ' + pkg.version + '\n' + pkg.description + '\n\nUsage: $0 [options]')
    .describe('verbosity', 'possible values: "error", "warn", "info", "debug"')
    .describe('mqtt-url', 'mqtt broker url. See https://github.com/mqttjs/MQTT.js#connect-using-a-url')
    .describe('influx-host')
    .describe('influx-port')
    .describe('influx-database')
    .describe('influx-measurement')
    .describe('subscription', 'array of topics to subscribe').array('subscription')
    .alias({
        h: 'help',
        m: 'mqtt-url',
        v: 'verbosity'
    })
    .default({
        name: 'mqtt2influx',
        'mqtt-url': 'mqtt://127.0.0.1',
        'influx-host': '127.0.0.1',
        'influx-port': 8086,
        'influx-database': 'raw_mqtt',
        'influx-measurement': 'data',
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
    host: config.influxHost,
    port: config.influxPort,
    database: config.influxDatabase
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
    if (packet.retain) {
        // Skip retained messages on start
        return;
    }

    const point = {
        measurement: config.influxMeasurement,
        fields: {
            string_value: String(message)
        },
        tags: {
            url: config.mqttUrl,
            broker: mqttUrl.host,
            topic
        }
    };
    if (mqttUrl.port) point.tags.port = mqttUrl.port;
    if (mqttUrl.username) point.tags.username = mqttUrl.username;

    // Write Datapoint
    influx.writePoints([point]).then(() => {
        log.debug('influx >', point.measurement);
    }).catch(error => {
        log.warn('influx >', point.measurement, error.message);
    });
});
