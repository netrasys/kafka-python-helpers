from kafka_python_helpers.utils import compact_int_list


def test_compact_int_list():
    assert compact_int_list([]) == []
    assert compact_int_list([1]) == [1]
    assert compact_int_list([1, 2, 3, 4]) == [(1, 4)]
    assert compact_int_list([1, 3, 4]) == [1, (3, 4)]
    assert compact_int_list([1, 3, 5]) == [1, 3, 5]
    assert compact_int_list([1, 4]) == [1, 4]
    assert compact_int_list([1, 3, 4, 5]) == [1, (3, 5)]
