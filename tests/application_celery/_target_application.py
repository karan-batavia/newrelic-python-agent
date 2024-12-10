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

from celery import Celery, Task, shared_task
from testing_support.validators.validate_distributed_trace_accepted import (
    validate_distributed_trace_accepted,
)

from newrelic.api.transaction import current_transaction

app = Celery(
    "tasks",
    broker_url="memory://",
    result_backend="cache+memory://",
    worker_hijack_root_logger=False,
    pool="solo",
    broker_heartbeat=0,
)


class CustomBaseTask(Task):
    def __call__(self, *args, **kwargs):
        return self.run(*args, **kwargs)


# Despite Celery's documentation urging users to not
# call super().__call__ in custom tasks (as shown here
# under the tip line:
# https://docs.celeryq.dev/en/latest/userguide/application.html#abstract-tasks)
# This has been accounted for in
# the code base.  This is a test to ensure that our
# instrumentation still works in this case as well
class CustomBaseTaskWithSuper(Task):
    def __call__(self, *args, **kwargs):
        return super().__call__(*args, **kwargs)


class CustomClassBasedTask(Task):
    def run(self, a, b):
        return a + b


@app.task
def add(x, y):
    return x + y


@app.task
def tsum(nums):
    return sum(nums)


@app.task
def nested_add(x, y):
    return add(x, y)


@shared_task
def shared_task_add(x, y):
    return x + y


@app.task(base=CustomBaseTask)
def custom_base_task_add(x, y):
    return x + y


@app.task(base=CustomBaseTaskWithSuper)
def custom_base_task_with_super_add(x, y):
    return x + y


@app.task
@validate_distributed_trace_accepted(transport_type="AMQP")
def assert_dt():
    # Basic checks for DT delegated to task
    txn = current_transaction()
    assert txn, "No transaction active."
    assert txn.name == "_target_application.assert_dt", f"Transaction name does not match: {txn.name}"
    return 1
