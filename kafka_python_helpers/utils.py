from itertools import islice

from six.moves import zip


def compact_int_list(lst):
    if len(lst) == 0:
        return []
    elif len(lst) == 1:
        return [(lst[0], lst[0])]

    results = []

    start = lst[0]
    for a, b in zip(lst, islice(lst, 1, None)):
        if b != a + 1:
            results.append((start, a))
            start = b

    results.append((start, lst[-1]))
    return results
