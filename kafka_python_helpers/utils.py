from itertools import islice

from six.moves import zip


def compact_int_list(lst):
    if len(lst) == 0:
        return []
    elif len(lst) == 1:
        return lst

    results = []

    start = lst[0]
    for a, b in zip(lst, islice(lst, 1, None)):
        if b != a + 1:
            if a != start:
                results.append((start, a))
            else:
                results.append(start)
            start = b

    if start != lst[-1]:
        results.append((start, lst[-1]))
    else:
        results.append(start)

    return results
