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

import celery
import pytest
from testing_support.validators.validate_code_level_metrics import (
    validate_code_level_metrics,
)
from testing_support.validators.validate_transaction_count import (
    validate_transaction_count,
)
from testing_support.validators.validate_transaction_metrics import (
    validate_transaction_metrics,
)

FORGONE_TASK_METRICS = [("Function/_target_application.add", None), ("Function/_target_application.tsum", None)]


@pytest.fixture(scope="module", autouse=True, params=[False, True], ids=["unpatched", "patched"])
def with_worker_optimizations(request, celery_worker_available):
    if request.param:
        celery.app.trace.setup_worker_optimizations(celery_worker_available.app)

    yield request.param
    celery.app.trace.reset_worker_optimizations()


@validate_transaction_metrics(
    name="_target_application.add",
    group="Celery",
    scoped_metrics=FORGONE_TASK_METRICS,
    rollup_metrics=FORGONE_TASK_METRICS,
    background_task=True,
)
@validate_code_level_metrics("_target_application", "add")
@validate_transaction_count(1)
def test_celery_task_call(application):
    """
    Executes task in local process and returns the result directly.
    """
    result = application.add(3, 4)
    assert result == 7


@validate_transaction_metrics(
    name="_target_application.add",
    group="Celery",
    scoped_metrics=FORGONE_TASK_METRICS,
    rollup_metrics=FORGONE_TASK_METRICS,
    background_task=True,
)
@validate_code_level_metrics("_target_application", "add")
@validate_transaction_count(1)
def test_celery_task_apply(application):
    """
    Executes task in local process and returns an EagerResult.
    """
    result = application.add.apply((3, 4))
    result = result.get()
    assert result == 7


@validate_transaction_metrics(
    name="_target_application.add",
    group="Celery",
    scoped_metrics=FORGONE_TASK_METRICS,
    rollup_metrics=FORGONE_TASK_METRICS,
    background_task=True,
)
@validate_code_level_metrics("_target_application", "add")
@validate_transaction_count(1)
def test_celery_task_delay(application):
    """
    Executes task on worker process and returns an AsyncResult.
    """
    result = application.add.delay(3, 4)
    result = result.get()
    assert result == 7


@validate_transaction_metrics(
    name="_target_application.add",
    group="Celery",
    scoped_metrics=FORGONE_TASK_METRICS,
    rollup_metrics=FORGONE_TASK_METRICS,
    background_task=True,
)
@validate_code_level_metrics("_target_application", "add")
@validate_transaction_count(1)
def test_celery_task_apply_async(application):
    """
    Executes task on worker process and returns an AsyncResult.
    """
    result = application.add.apply_async((3, 4))
    result = result.get()
    assert result == 7


@validate_transaction_metrics(
    name="_target_application.add",
    group="Celery",
    scoped_metrics=FORGONE_TASK_METRICS,
    rollup_metrics=FORGONE_TASK_METRICS,
    background_task=True,
)
@validate_code_level_metrics("_target_application", "add")
@validate_transaction_count(1)
def test_celery_app_send_task(celery_session_app):
    """
    Executes task on worker process and returns an AsyncResult.
    """
    result = celery_session_app.send_task("_target_application.add", (3, 4))
    result = result.get()
    assert result == 7


@validate_transaction_metrics(
    name="_target_application.add",
    group="Celery",
    scoped_metrics=FORGONE_TASK_METRICS,
    rollup_metrics=FORGONE_TASK_METRICS,
    background_task=True,
)
@validate_code_level_metrics("_target_application", "add")
@validate_transaction_count(1)
def test_celery_task_signature(application):
    """
    Executes task on worker process and returns an AsyncResult.
    """
    result = application.add.s(3, 4).delay()
    result = result.get()
    assert result == 7


@validate_transaction_metrics(
    name="_target_application.add",
    group="Celery",
    scoped_metrics=FORGONE_TASK_METRICS,
    rollup_metrics=FORGONE_TASK_METRICS,
    background_task=True,
)
@validate_transaction_metrics(
    name="_target_application.add",
    group="Celery",
    scoped_metrics=FORGONE_TASK_METRICS,
    rollup_metrics=FORGONE_TASK_METRICS,
    background_task=True,
    index=-2,
)
@validate_code_level_metrics("_target_application", "add")
@validate_code_level_metrics("_target_application", "add", index=-2)
@validate_transaction_count(2)
def test_celery_task_link(application):
    """
    Executes multiple tasks on worker process and returns an AsyncResult.
    """
    result = application.add.apply_async((3, 4), link=[application.add.s(5)])
    result = result.get()
    assert result == 7  # Linked task result won't be returned


@validate_transaction_metrics(
    name="_target_application.add",
    group="Celery",
    scoped_metrics=FORGONE_TASK_METRICS,
    rollup_metrics=FORGONE_TASK_METRICS,
    background_task=True,
)
@validate_transaction_metrics(
    name="_target_application.add",
    group="Celery",
    scoped_metrics=FORGONE_TASK_METRICS,
    rollup_metrics=FORGONE_TASK_METRICS,
    background_task=True,
    index=-2,
)
@validate_code_level_metrics("_target_application", "add")
@validate_code_level_metrics("_target_application", "add", index=-2)
@validate_transaction_count(2)
def test_celery_chain(application):
    """
    Executes multiple tasks on worker process and returns an AsyncResult.
    """
    result = celery.chain(application.add.s(3, 4), application.add.s(5))()

    result = result.get()
    assert result == 12


@validate_transaction_metrics(
    name="_target_application.add",
    group="Celery",
    scoped_metrics=FORGONE_TASK_METRICS,
    rollup_metrics=FORGONE_TASK_METRICS,
    background_task=True,
)
@validate_transaction_metrics(
    name="_target_application.add",
    group="Celery",
    scoped_metrics=FORGONE_TASK_METRICS,
    rollup_metrics=FORGONE_TASK_METRICS,
    background_task=True,
    index=-2,
)
@validate_code_level_metrics("_target_application", "add")
@validate_code_level_metrics("_target_application", "add", index=-2)
@validate_transaction_count(2)
def test_celery_group(application):
    """
    Executes multiple tasks on worker process and returns an AsyncResult.
    """
    result = celery.group(application.add.s(3, 4), application.add.s(1, 2))()
    result = result.get()
    assert result == [7, 3]


@validate_transaction_metrics(
    name="_target_application.tsum",
    group="Celery",
    scoped_metrics=FORGONE_TASK_METRICS,
    rollup_metrics=FORGONE_TASK_METRICS,
    background_task=True,
)
@validate_transaction_metrics(
    name="_target_application.add",
    group="Celery",
    scoped_metrics=FORGONE_TASK_METRICS,
    rollup_metrics=FORGONE_TASK_METRICS,
    background_task=True,
    index=-2,
)
@validate_transaction_metrics(
    name="_target_application.add",
    group="Celery",
    scoped_metrics=FORGONE_TASK_METRICS,
    rollup_metrics=FORGONE_TASK_METRICS,
    background_task=True,
    index=-3,
)
@validate_code_level_metrics("_target_application", "tsum")
@validate_code_level_metrics("_target_application", "add", index=-2)
@validate_code_level_metrics("_target_application", "add", index=-3)
@validate_transaction_count(3)
def test_celery_chord(application):
    """
    Executes 2 add tasks, followed by a tsum task on the worker process and returns an AsyncResult.
    """
    result = celery.chord([application.add.s(3, 4), application.add.s(1, 2)])(application.tsum.s())
    result = result.get()
    assert result == 10


@validate_transaction_metrics(
    name="celery.map/_target_application.tsum",
    group="Celery",
    scoped_metrics=[("Function/_target_application.tsum", 2)],
    rollup_metrics=[("Function/_target_application.tsum", 2)],
    background_task=True,
)
@validate_code_level_metrics("_target_application", "tsum", count=3)
@validate_transaction_count(1)
def test_celery_task_map(application):
    """
    Executes map task on worker process with original task as a subtask and returns an AsyncResult.
    """
    result = application.tsum.map([(3, 4), (1, 2)]).apply()
    result = result.get()
    assert result == [7, 3]


@validate_transaction_metrics(
    name="celery.starmap/_target_application.add",
    group="Celery",
    scoped_metrics=[("Function/_target_application.add", 2)],
    rollup_metrics=[("Function/_target_application.add", 2)],
    background_task=True,
)
@validate_code_level_metrics("_target_application", "add", count=3)
@validate_transaction_count(1)
def test_celery_task_starmap(application):
    """
    Executes starmap task on worker process with original task as a subtask and returns an AsyncResult.
    """
    result = application.add.starmap([(3, 4), (1, 2)]).apply_async()
    result = result.get()
    assert result == [7, 3]


@validate_transaction_metrics(
    name="celery.starmap/_target_application.add",
    group="Celery",
    scoped_metrics=[("Function/_target_application.add", 1)],
    rollup_metrics=[("Function/_target_application.add", 1)],
    background_task=True,
)
@validate_transaction_metrics(
    name="celery.starmap/_target_application.add",
    group="Celery",
    scoped_metrics=[("Function/_target_application.add", 1)],
    rollup_metrics=[("Function/_target_application.add", 1)],
    background_task=True,
    index=-2,
)
@validate_code_level_metrics("_target_application", "add", count=2)
@validate_code_level_metrics("_target_application", "add", count=2, index=-2)
@validate_transaction_count(2)
def test_celery_task_chunks(application):
    """
    Executes multiple tasks on worker process and returns an AsyncResult.
    """
    result = application.add.chunks([(3, 4), (1, 2)], n=1).apply_async()
    result = result.get()
    assert result == [[7], [3]]
