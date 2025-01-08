import argparse
import json
import logging
import os
import signal
import time
from typing import Any

import paho.mqtt.client as mqtt


logger = logging.getLogger(__name__)
logger.addHandler(logging.StreamHandler())


class TimestampEnricher(mqtt.Client):

    def __init__(self, *args, **kwargs):
        self._source_topics = kwargs.pop('source_topics')
        self._target_topic = kwargs.pop('target_topic')
        self._topics_by_mid = {}

        super().__init__(*args, **kwargs)

    # def on_message(self):
    def on_message(self, client: mqtt.Client, userdata: Any, message: mqtt.MQTTMessage):
        data = json.loads(str(message.payload, 'utf-8'))
        if isinstance(data, dict):
            data['timestamp'] = int(time.time())
        else:
            data = {
                'timestamp': int(time.time()),
                'value': data,
            }
        logger.debug("Formatted message: %s, topic: %s" % (data, message.topic))

        target_subtopic = message.topic.removeprefix(message.topic[:-2])
        target_topic = self._target_topic + target_subtopic

        client.publish(target_topic, json.dumps(data))
        logger.info(f"Published message to topic: {target_topic}")

    def on_subscribe(self, client: mqtt.Client, userdata: Any, mid: int, reason_code_list, properties):
        logger.info("Subscribed to topic %s" % self._topics_by_mid[mid])

    def _signal_handler(self, signal, frame):
        self.disconnect()
        logger.info("Received signal %s. Disconnected from MQTT broker." % signal)

    def _register_signals(self):
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def run(self, host: str, port: int, username: str, password: str):
        self._register_signals()
        self.username_pw_set(username, password)
        self.connect(host, port)
        logger.info("Connected to Mosquitto server at %s:%d" % (host, port))

        for topic in self._source_topics:
            t = topic.strip()
            result, mid = self.subscribe(t)
            self._topics_by_mid[mid] = t

        rc = 0
        while rc == 0:
            rc = self.loop()
        return rc


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='MQTT client')
    parser.add_argument(
        '--host',
        type=str,
        default=os.environ.get('MQTT_HOST', 'localhost'),
        help='MQTT broker host',
    )
    parser.add_argument(
        '--port',
        type=int,
        default=int(os.environ.get('MQTT_PORT', 1883)),
        help='MQTT broker port',
    )
    parser.add_argument(
        '--source-topics',
        type=str,
        nargs='*',  # Accept multiple strings as a list
        default=os.environ.get('SOURCE_TOPICS', '').split(','),
        help='Additional topics'
    )
    parser.add_argument(
        '--target-topic',
        type=str,
        default=os.environ.get('TARGET_TOPIC', 'timestamp-enriched'),
        help='Target topic (default: timestamp-enriched)'
    )
    parser.add_argument(
        '--username',
        type=str,
        default=os.environ.get('MQTT_USERNAME', 'app'),
        help='MQTT broker username',
    )
    parser.add_argument(
        '--password',
        type=str,
        default=os.environ.get('MQTT_PASSWORD', None),
        help='MQTT broker password',
    )
    parser.add_argument(
        '--debug-level',
        type=str,
        default=os.environ.get('DEBUG_LEVEL', 'INFO').upper(),
        help='Debug level',
    )

    logger.setLevel(parser.parse_args().debug_level)

    args = parser.parse_args()
    if str(args.target_topic).endswith('/'):
        args.target_topic = args.target_topic[:-1]

    client = TimestampEnricher(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id='timestamp-message-enricher',
        source_topics=args.source_topics,
        target_topic=args.target_topic
    )
    client.run(args.host, args.port, args.username, args.password)
