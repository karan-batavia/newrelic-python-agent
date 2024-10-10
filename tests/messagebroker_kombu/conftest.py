# Copyright 2010 New Relic, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import uuid

import kombu
import pytest
from testing_support.db_settings import rabbitmq_settings
from testing_support.fixtures import (  # noqa: F401; pylint: disable=W0611
    collector_agent_registration_fixture,
    collector_available_fixture,
)

from newrelic.api.transaction import current_transaction
from newrelic.common.object_wrapper import transient_function_wrapper

DB_SETTINGS = rabbitmq_settings()[0]
# BOOTSTRAP_SERVER = f"{DB_SETTINGS['host']}:{DB_SETTINGS['port']}"


@pytest.fixture(scope="session")
def connection():
    with kombu.Connection(DB_SETTINGS["host"]) as conn:
        yield conn


_default_settings = {
    "package_reporting.enabled": False,  # Turn off package reporting for testing as it causes slow downs.
    "transaction_tracer.explain_threshold": 0.0,
    "transaction_tracer.transaction_threshold": 0.0,
    "transaction_tracer.stack_trace_threshold": 0.0,
    "debug.log_data_collector_payloads": True,
    "debug.record_transaction_failure": True,
}

collector_agent_registration = collector_agent_registration_fixture(
    app_name="Python Agent Test (messagebroker_kombu)",
    default_settings=_default_settings,
    linked_applications=["Python Agent Test (messagebroker_kombu)"],
)


@pytest.fixture(
    scope="session", params=["no_serializer"]  # , "serializer_function", "callable_object", "serializer_object"]
)
def client_type(request):
    return request.param


@pytest.fixture
def skip_if_not_serializing(client_type):
    if client_type == "no_serializer":
        pytest.skip("Only serializing clients supported.")


@pytest.fixture(scope="function")
def producer(client_type, connection):  # json_serializer, json_callable_serializer, connection):
    if client_type == "no_serializer":
        producer = connection.Producer()
    elif client_type == "serializer_function":
        producer = connection.Producer(serializer="json")
    elif client_type == "callable_object":
        producer = connection.Producer(
            serializer=lambda v: json.dumps(v).encode("utf-8") if v else None,
        )
    elif client_type == "serializer_object":
        producer = connection.Producer(
            serializer=json_serializer,
        )

    yield producer


@pytest.fixture(scope="function")
def consumer(group_id, producer, json_deserializer, json_callable_deserializer, connection, queue, consume):
    if client_type == "no_serializer":
        consumer = connection.Consumer(queue, callbacks=[consume])
    elif client_type == "serializer_function":
        consumer = connection.Consumer(queue, callbacks=[consume])
        #    key_deserializer=lambda v: json.loads(v.decode("utf-8")) if v else None,
    elif client_type == "callable_object":
        consumer = connection.Consumer(queue, callbacks=[consume])
        #    key_deserializer=json_callable_deserializer,
    elif client_type == "serializer_object":
        consumer = connection.Consumer(queue, callbacks=[consume])
        #    key_deserializer=json_deserializer,
    with consumer as con:
        yield con


@pytest.fixture
def consume(body, message):
    message.ack()


@pytest.fixture
def exchange():
    return kombu.Exchange("exchange", "direct", durable=True)


@pytest.fixture
def queue(exchange):
    return kombu.Queue("bar", exchange=exchange, routing_key="bar")


@pytest.fixture(scope="session")
def serialize(client_type):
    if client_type == "no_serializer":
        return lambda v: json.dumps(v).encode("utf-8")
    else:
        return lambda v: v


@pytest.fixture(scope="session")
def deserialize(client_type):
    if client_type == "no_serializer":
        return lambda v: json.loads(v.decode("utf-8"))
    else:
        return lambda v: v


@pytest.fixture(scope="session")
def json_serializer():
    class JSONSerializer(kombu.serializer.Serializer):
        def serialize(self, topic, obj):
            return json.dumps(obj).encode("utf-8") if obj is not None else None

    return JSONSerializer()


@pytest.fixture(scope="session")
def json_deserializer():
    class JSONDeserializer(kombu.serializer.Deserializer):
        def deserialize(self, topic, bytes_):
            return json.loads(bytes_.decode("utf-8")) if bytes_ is not None else None

    return JSONDeserializer()


@pytest.fixture(scope="session")
def json_callable_serializer():
    class JSONCallableSerializer:
        def __call__(self, obj):
            return json.dumps(obj).encode("utf-8") if obj is not None else None

    return JSONCallableSerializer()


@pytest.fixture(scope="session")
def json_callable_deserializer():
    class JSONCallableDeserializer:
        def __call__(self, obj):
            return json.loads(obj.decode("utf-8")) if obj is not None else None

    return JSONCallableDeserializer()


@pytest.fixture(scope="session")
def group_id():
    return str(uuid.uuid4())


@pytest.fixture
def send_producer_message(producer, exchange, queue):
    def _test():
        producer.publish({"foo": 1}, exchange=exchange, routing_key="bar", declare=[queue])

    return _test


@pytest.fixture
def get_consumer_record(send_producer_message, connection, consumer):
    def _test():
        send_producer_message()
        while True:
            events = connection.drain_events()

        assert events

    return _test


# @transient_function_wrapper(kombu.producer.kombu, "KafkaProducer.send.__wrapped__")
## Place transient wrapper underneath instrumentation
# def cache_kombu_producer_headers(wrapped, instance, args, kwargs):
#    transaction = current_transaction()
#
#    if transaction is None:
#        return wrapped(*args, **kwargs)
#
#    ret = wrapped(*args, **kwargs)
#    headers = kwargs.get("headers", [])
#    headers = dict(headers)
#    transaction._test_request_headers = headers
#    return ret


# @transient_function_wrapper(kombu.consumer.group, "KafkaConsumer.__next__")
## Place transient wrapper underneath instrumentation
# def cache_kombu_consumer_headers(wrapped, instance, args, kwargs):
#    record = wrapped(*args, **kwargs)
#    transaction = current_transaction()
#
#    if transaction is None:
#        return record
#
#    headers = record.headers
#    headers = dict(headers)
#    transaction._test_request_headers = headers
#    return record
