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

import sys
import time

from newrelic.api.application import application_instance

from newrelic.api.error_trace import ErrorTrace
from newrelic.api.external_trace import ExternalTrace
from newrelic.api.time_trace import notice_error
from newrelic.api.transaction import current_transaction
from newrelic.api.web_transaction import WebTransaction
from newrelic.common.object_names import callable_name
from newrelic.common.object_wrapper import wrap_function_wrapper


def _bind_finish_handler_with_unary_response(rpc_state, unary_handler, request, servicer_context, response_serializer, loop):
    return unary_handler, servicer_context, unary_handler

def _bind_finish_handler_with_stream_responses(rpc_state, stream_handler, request, servicer_context, loop):
    return stream_handler, servicer_context, stream_handler


def grpc_web_transaction(_bind_finish):
    async def _grpc_web_transaction(wrapped, instance, args, kwargs):
        behavior, context, handler = _bind_finish(*args, **kwargs)
        behavior_name = callable_name(behavior)

        # handler = 

        metadata = (
                getattr(context, 'invocation_metadata', None) or
                getattr(context, 'request_metadata', None))()

        host = port = request_path = None
        try:
            host = context.host().decode("utf-8").split(":")  # Split host into pieces, removing protocol
            port = host[-1]  # Remove port
            host = ":".join(host[:-1])  # Rejoin ipv6 hosts
            request_path = context.method()
        except AttributeError:
            pass

        transaction = WebTransaction(
            application=application_instance(),
            name=behavior_name,
            request_path=request_path,
            host=host,
            port=port,
            headers=metadata)
        transaction.__enter__()

        response = await wrapped(*args, **kwargs)

        status_code, trailing_metadata = 0, ()
        try:
            status_code = context.code()
            trailing_metadata = context.trailing_metadata()
        except AttributeError:
            pass

        transaction.process_response(status_code, trailing_metadata)

        return response

    return _grpc_web_transaction


def _prepare_request(
        transaction,
        guid, 
        request,
        *args,
        timeout=None,
        metadata=None,
        **kwargs
    ):
    metadata = metadata and list(metadata) or []
    dt_metadata = transaction._create_distributed_trace_data_with_guid(guid)
    metadata.extend(
        transaction._generate_distributed_trace_headers(dt_metadata)
    )
    kwargs.update({"timeout": timeout, "metadata": metadata})
    args = (request,) + args
    return args, kwargs


def _prepare_request_stream(
        transaction, guid, request_iterator, *args, **kwargs):
    return _prepare_request(
            transaction, guid, request_iterator, *args, **kwargs)

def _get_uri_method(instance, *args, **kwargs):
    if hasattr(instance._channel, "target"):
        target = instance._channel.target().decode('utf-8')
    else:
        target = "<unknown>"

    method = instance._method.decode('utf-8').lstrip('/')
    uri = 'grpc://%s/%s' % (target, method)
    return (uri, method)


def await_response(trace):
    def _await_response(call):
        """Retrieve status code using an event loop."""
        transaction = current_transaction()
        if transaction:
            import asyncio
            loop = asyncio.get_event_loop()
            code, trailing_metadata = loop.run_until_complete(asyncio.gather(call.code(), call.trailing_metadata()))
            breakpoint()
            trace.process_response(code.value[0], trailing_metadata)

    return _await_response


async def wrap_handle_rpc(wrapped, instance, args, kwargs):
    with ErrorTrace():
        return await wrapped(*args, **kwargs)


async def wrap_handle_exceptions(wrapped, instance, args, kwargs):
    # True transaction start time is now, but WebTransaction attributes are
    # not available from the pure python level until the final handler.
    # We need to preserve the start time and handle transaction exits cleanly
    # so that exception and abort handlers have a transaction to process
    # responses for.

    start_time = time.time()

    try:
        return await wrapped(*args, **kwargs)
    finally:
        transaction = current_transaction()
        if transaction:
            transaction.start_time = start_time
            transaction.__exit__(*sys.exc_info())


def bind_send_error_status_from_server(grpc_call_wrapper,
    code,
    details,
    trailing_metadata,
    send_initial_metadata_op,
    loop):
    return code, trailing_metadata


async def wrap_send_error_status_from_server(wrapped, instance, args, kwargs):
    transaction = current_transaction()
    breakpoint()
    if transaction:
        code, trailing_metadata = bind_send_error_status_from_server(*args, **kwargs)
        transaction.process_response(code, trailing_metadata)

    return await wrapped(*args, **kwargs)


def wrap_call(module, object_path, prepare):

    def _call_wrapper(wrapped, instance, args, kwargs):
        transaction = current_transaction()
        if transaction is None:
            return wrapped(*args, **kwargs)

        uri, method = _get_uri_method(instance)
        with ExternalTrace('gRPC', uri, method) as trace:
            args, kwargs = prepare(transaction, None, *args, **kwargs)
            call = wrapped(*args, **kwargs)
            call.add_done_callback(await_response(trace))

        return call

    wrap_function_wrapper(module, object_path, _call_wrapper)


def instrument_grpc_aio_channel(module):
    wrap_call(module, 'UnaryUnaryMultiCallable.__call__',
            _prepare_request)
    wrap_call(module, 'StreamUnaryMultiCallable.__call__',
            _prepare_request_stream)
    wrap_call(module, 'UnaryStreamMultiCallable.__call__',
            _prepare_request)
    wrap_call(module, 'StreamStreamMultiCallable.__call__',
            _prepare_request_stream)


def instrument_grpc_cygprc(module):
    wrap_function_wrapper(module, "_finish_handler_with_unary_response", grpc_web_transaction(_bind_finish_handler_with_unary_response))
    wrap_function_wrapper(module, "_finish_handler_with_stream_responses", grpc_web_transaction(_bind_finish_handler_with_stream_responses))
    wrap_function_wrapper(module, "_handle_exceptions", wrap_handle_exceptions)
    wrap_function_wrapper(module, "_handle_rpc", wrap_handle_rpc)
    wrap_function_wrapper(module, "_send_error_status_from_server", wrap_send_error_status_from_server)
