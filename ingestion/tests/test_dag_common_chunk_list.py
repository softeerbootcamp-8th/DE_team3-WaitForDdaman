"""dag_common.chunk_list 테스트 (#249 - EMR 초기 적재 배치 분할)."""
import os
import sys

import pytest

DAGS_DIR = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..", "..", "airflow", "dags")
)
if DAGS_DIR not in sys.path:
    sys.path.insert(0, DAGS_DIR)

import dag_common  # noqa: E402


def test_splits_into_batch_size_groups():
    batches = dag_common.chunk_list(["a", "b", "c", "d", "e"], batch_size=2)
    assert batches == [["a", "b"], ["c", "d"], ["e"]]


def test_batch_size_larger_than_list_returns_single_batch():
    batches = dag_common.chunk_list(["a", "b"], batch_size=10)
    assert batches == [["a", "b"]]


def test_empty_list_returns_no_batches():
    assert dag_common.chunk_list([], batch_size=3) == []


def test_batch_size_one_returns_one_file_per_batch():
    batches = dag_common.chunk_list(["a", "b", "c"], batch_size=1)
    assert batches == [["a"], ["b"], ["c"]]


def test_preserves_order():
    batches = dag_common.chunk_list(list(range(7)), batch_size=3)
    assert batches == [[0, 1, 2], [3, 4, 5], [6]]


@pytest.mark.parametrize("batch_size", [0, -1])
def test_invalid_batch_size_raises(batch_size):
    with pytest.raises(ValueError):
        dag_common.chunk_list(["a"], batch_size=batch_size)


def test_chunks_files_by_count_and_total_size_preserving_input_order():
    sizes = {"a": 3, "b": 2, "c": 4, "d": 1}
    batches = dag_common.chunk_files_by_size(
        ["a", "b", "c", "d"], max_files=3, max_bytes=5, size_fn=sizes.__getitem__
    )
    assert batches == [["a", "b"], ["c", "d"]]


def test_large_file_is_alone_even_when_over_size_limit():
    sizes = {"large": 10, "small": 1}
    batches = dag_common.chunk_files_by_size(
        ["large", "small"], max_files=6, max_bytes=4, size_fn=sizes.__getitem__
    )
    assert batches == [["large"], ["small"]]


def test_invalid_size_limits_raise():
    with pytest.raises(ValueError):
        dag_common.chunk_files_by_size(["a"], max_files=0, max_bytes=1, size_fn=lambda _: 1)
    with pytest.raises(ValueError):
        dag_common.chunk_files_by_size(["a"], max_files=1, max_bytes=0, size_fn=lambda _: 1)
