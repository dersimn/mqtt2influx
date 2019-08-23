#!/usr/bin/env node

const pkg = require('./package.json');
const log = require('yalm');
const config = require('yargs')
    .env('MQTT2INFLUX')
    .usage(pkg.name + ' ' + pkg.version + '\n' + pkg.description + '\n\nUsage: $0 [options]')
    .describe('verbosity', 'possible values: "error", "warn", "info", "debug"')
    .describe('name', 'instance name. used as mqtt client id and as prefix for connected topic')
    .describe('mqtt-url', 'mqtt broker url. See https://github.com/mqttjs/MQTT.js#connect-using-a-url')
    .describe('influx-host')
    .describe('influx-port')
    .describe('influx-database')
    .describe('subscription', 'array of topics to subscribe').array('subscription')
    .alias({
        h: 'help',
        m: 'mqtt-url',
        v: 'verbosity'
    })
    .default({
        name: 'influx',
        'mqtt-url': 'mqtt://127.0.0.1',
        'influx-host': '127.0.0.1',
        'influx-port': 8086,
        'influx-database': 'mqtt',
        'subscription': [
            '+/status/#',
            '+/connected',
            '+/maintenance/#'
        ]
    })
    .version()
    .help('help')
    .argv;
const MqttSmarthome = require('mqtt-smarthome-connect');
const Influx = require('influx');

log.setLevel(config.verbosity);
log.info(pkg.name + ' ' + pkg.version + ' starting');
log.debug("loaded config: ", config);

const influx = new Influx.InfluxDB({
    host: config.influxHost,
    port: config.influxPort,
    database: config.influxDatabase
});

log.info('mqtt trying to connect', config.mqttUrl);
const mqtt = new MqttSmarthome(config.mqttUrl, {
    logger: log,
    will: {topic: config.name + '/connected', payload: '0', retain: true}
});
mqtt.connect();

mqtt.on('connect', () => {
    log.info('mqtt connected', config.mqttUrl);
    mqtt.publish(config.name + '/connected', '1', {retain: true});
});

mqtt.subscribe(config.subscription, (topic, message, wildcard, packet) => {
    if (packet.retain) {
        // Skip retained messages on start
        return;
    }

    /* Process topic string
     *     foo/status/bar -> foo//bar
     *     var/status/foo -> $foo
     */
    let tpc = topic.split('/');
    if (tpc[1] == 'status') tpc[1] = null;
    topic = tpc.join('/');
    topic = topic.replace('var//', '$');

    // Build InfluxDB Datapoint
    let point = {};
    point.fields = {};
    if (typeof message === 'object') {
        if (Array.isArray(message)) {
            message.forEach((val, i) => {
                point.fields['_'+i] = val;
            });
        } else {
            if ('val' in message) {
                point.fields.value = message.val;
            }
            Object.keys(message).forEach(key => {
                if (key === 'val') return; // 'val' has already been processed
                if (key === 'ts')  return; // skip mqtt-smarthome specific data
                if (key === 'lc')  return; // ..
                if (key === 'ttl') return; // ..
                if (typeof message[key] === 'object') return;

                point.fields[key] = message[key];
            });
            if ('ts' in message) {
                let ts = new Date(message.ts);
                point.timestamp = ts;
            }
        }
    } else {
        point.fields.value = message;
    }
    if (typeof point.fields.value === 'boolean') {
        // Provide bool transformation until InfluxDB supports type conversions
        point.fields.intValue = (point.fields.value) ? 1 : 0;
    }
    point.measurement = Influx.escape.measurement(topic);

    // Write Datapoint
    influx.writePoints([point]).then(() => {
        log.debug('influx >', point.measurement);
    }).catch((err) => {
        log.warn('influx >', point.measurement, err.message);
    });
});
