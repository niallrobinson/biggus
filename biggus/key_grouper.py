def normalize_slice(slice_instance, dim_length):
    start = slice_instance.start or 0
    stop = slice_instance.stop if slice_instance.stop is not None else dim_length
    step = slice_instance.step
    if step == 1:
        step = None
    if stop is not None and stop < 0:
        stop = dim_length + stop
    if start is not None and start < 0:
        start = dim_length + start
    if start == 0:
        start = None
    if stop == dim_length:
        stop = None
    return slice(start, stop, step)


def dimension_group_to_lowest_common(dim_length, dim_keys):
    # NOTE: Grouping does not currently take step into account.
    # It is assumed that step == 1.
    # NOTE: It is assumed that the keys are ordered.

    # NOTE: Currently in order to group, one of the groups must have a spanning slice...

    dim_keys = [[normalize_slice(key, dim_length) for key in keys]
                for keys in dim_keys]
    def sort_slice(key):
        return (key.start or 0, key.stop if key.stop is not None else dim_length)
    dim_keys = [sorted(keys, key=sort_slice)
                for keys in dim_keys]

    def all_equal(array):
        return all([array[0] == item for item in array])

    # We keep track of where our group starts with the start_index, and
    # once our group condition is met, we use the current_index to
    # identify the end of a group.

    current_index = start_index = [0] * len(dim_keys)
    lengths = [len(keys) for keys in dim_keys]
    i = 0

    # A dictionary mapping a (start, stop, step) tuple to the list of
    # keys that should be combined.
    groups = {}
    while current_index != lengths:
        starts = [keys[index].start or 0 for keys, index in zip(dim_keys, start_index)]
        assert all_equal(starts)
        current_stops = [keys[index].stop or dim_length for keys, index in zip(dim_keys, current_index)]
        if all_equal(current_stops):
            # We have a group of slices that all start and stop at the same point, so
            # put this into the groups dictionary, and move on.

            # It is legitimate for a repeated stop value (e.g. two slice(None) objects),
            # so move the index forward for each dimension for as long as it takes for the
            # stop value to change.
            adjusted_current_index = []
            for index, keys in zip(current_index, dim_keys):
                n_keys = len(keys)
                while n_keys > index + 1:
                    if (keys[index + 1].stop or dim_length) != current_stops[0]:
                        break
                    index += 1
                adjusted_current_index.append(index)
            current_index = adjusted_current_index

            # Capture the full slice tuple for this group. Replace numbers with None if 
            # the start is 0 and/or the end is the length of the dimension. This is just
            # a cleanliness feature that helps with familiarity when looking at a slice.
            slice_tuple = (starts[0] or None,
                           current_stops[0] if current_stops[0] != dim_length else None,
                           None)
            groups[slice_tuple] = [keys[start_index:stop_index+1]
                                   for keys, start_index, stop_index in
                                   zip(dim_keys, start_index, current_index)]
            current_index = [index + 1 for index in current_index]
            start_index = current_index

        else:
            # Move the lowest stop(s) forward by one.
            min_stop = min(current_stops)
            current_index = [index + 1 if stop == min_stop else index
                             for stop, index in zip(current_stops, current_index)]

        # Prevent an infinite loop.
        i += 1
        if i > 9999:
            # This should never happen, but it may be possible with some inputs (unverified).
            raise RuntimeError('The algorithm failed to find key groups.')

    return groups


def group_keys(shape, *inputs_keys):
    """
    Usecase: Two sets of chunks, one spans the whole of a dimension, the other chunked it up.
    We need to know that we need to collect together the chunked form, so that we can
    work with both sets at the same time.

    Conceptually we have multiple source inputs, each with multiple key sets for indexing.

    NOTE: We treat the grouping independently per dimension. In practice this means we may be
    grouping more than is strictly necessary if we were being smart about multi-dimensional
    grouping. Anecdotally, that optimisation is currently not worth the implementation effort.

    """
    # Store the result as a slice mapping to a subset of the inputs_keys. We start
    # with the assumption that there will be only one group, and subdivide when we find this
    # not to be the case.
    ndim = len(inputs_keys[0][0])
    grouped_inputs_keys = {tuple((None, None, None) for _ in range(ndim)): inputs_keys}

    for dim, dim_len in enumerate(shape):
        # Compute the groups for this dimension.
        for group_keys, group_inputs_keys in grouped_inputs_keys.copy().items():
            group_inputs_key_for_dim = [[keys[dim] for keys in input_keys]
                                         for input_keys in group_inputs_keys]
            grouped_inputs_key = dimension_group_to_lowest_common(dim_len, group_inputs_key_for_dim)
            # If this group hasn't sub-divided, continue on to next group.
            if len(grouped_inputs_key) == 1:
                continue
            else:
                
                print('g:', grouped_inputs_key)
                print(grouped_inputs_keys)
                # Drop the bigger group from the result dictionary and in its place,
                # add all of the subgroups.
                grouped_inputs_keys.pop(group_keys)
                # Make the group keys mutable so that we can inject our subgroups.
                group_keys = list(group_keys)
                group_inputs_keys = list(group_inputs_keys)
                for subgroup_key, subgroup_inputs_key in grouped_inputs_key.items():
                    group_keys[dim] = subgroup_key

                    # Start with an empty list, one for each input.
                    subgroup_inputs_keys = [[] for _ in subgroup_inputs_key]
                    per_input = zip(group_inputs_keys, subgroup_inputs_key, subgroup_inputs_keys)
                    for input_keys, subgroup_input_key, new_input_keys in per_input:
                        for keys in input_keys[:]:
                            if normalize_slice(keys[dim], dim_len) in subgroup_input_key:
                                input_keys.remove(keys)
                                new_input_keys.append(keys)

                    grouped_inputs_keys[tuple(group_keys)] = tuple(subgroup_inputs_keys)
    return grouped_inputs_keys


import unittest

class Test_normalize_slice(unittest.TestCase):
    def assertSlice(self, expected_start, expected_stop, expected_step, result):
        self.assertEqual(expected_start, result.start)
        self.assertEqual(expected_stop, result.stop)
        self.assertEqual(expected_step, result.step)

    def test_step_1(self):
        r = normalize_slice(slice(None, None, 1), 3)
        self.assertSlice(None, None, None, r)

    def test_step_m1(self):
        r = normalize_slice(slice(None, None, None), None)
        self.assertSlice(None, None, None, r)

    def test_stop_m1(self):
        r = normalize_slice(slice(None, -1), 10)
        self.assertSlice(None, 9, None, r)

    def test_start_m1(self):
        r = normalize_slice(slice(-2, -1), 10)
        self.assertSlice(8, 9, None, r)

    def test_start_0(self):
        r = normalize_slice(slice(0, None), 10)
        self.assertSlice(None, None, None, r)

    def test_reverse_negative(self):
        r = normalize_slice(slice(-1, -3, -1), 10)
        self.assertSlice(9, 7, -1, r)

    def test_reverse_negative_nothing_there(self):
        r = normalize_slice(slice(-3, -1, -1), 10)
        # Would actually result in no index, but it is still valid.
        self.assertSlice(7, 9, -1, r)


class Test_dim_grouper(unittest.TestCase):
    def setUp(self):
        class Foo(object):
            def __getitem__(self, keys):
                return keys
        self.indexer = Foo()

    def test_one_set(self):
        r = dimension_group_to_lowest_common(4, [[self.indexer[0:]]])
        e = {(None, None, None): [[self.indexer[:]]]}
        self.assertEqual(e, r)

    def test_identical(self):
        r = dimension_group_to_lowest_common(4, [[self.indexer[:]], [self.indexer[:]]])
        e = {(None, None, None): [[self.indexer[:]], [self.indexer[:]]]}
        self.assertEqual(e, r)

    def test_repeat(self):
        r = dimension_group_to_lowest_common(4, [[self.indexer[:], self.indexer[:]], [self.indexer[:]]])
        e = {(None, None, None): [[self.indexer[:], self.indexer[:]], [self.indexer[:]]]}
        self.assertEqual(e, r)

    def test_equal(self):
        r = dimension_group_to_lowest_common(4, [[self.indexer[0:]],
                                                 [self.indexer[::1]]])
        e = {(None, None, None): [[self.indexer[:]], [self.indexer[:]]]}
        self.assertEqual(e, r)

    def test_single_subset(self):
        r = dimension_group_to_lowest_common(4, [[self.indexer[:2], self.indexer[2:]],
                                                 [self.indexer[:]]])
        e = {(None, None, None): [[self.indexer[:2], self.indexer[2:]],
                                  [self.indexer[:]]]}
        self.assertEqual(e, r)

    def test_multiple_offset_subsets(self):
        indices = [[self.indexer[:2], self.indexer[2:4], self.indexer[4:7]],
                   [self.indexer[:4], self.indexer[4:5], self.indexer[5:]]]
        r = dimension_group_to_lowest_common(7, indices)
        e = {(None, 4, None): [[self.indexer[:2], self.indexer[2:4]],
                               [self.indexer[:4]]],
             (4, None, None): [[self.indexer[4:]],
                               [self.indexer[4:5], self.indexer[5:]]]
             }
        self.assertEqual(e, r)

    def test_unordered(self):
        # As test_multiple_offset_subsets, but with the input order switched.
        indices = [[self.indexer[2:4], self.indexer[:2], self.indexer[4:7]],
                   [self.indexer[:4], self.indexer[4:5], self.indexer[5:]]]
        r = dimension_group_to_lowest_common(7, indices)
        e = {(None, 4, None): [[self.indexer[:2], self.indexer[2:4]],
                               [self.indexer[:4]]],
             (4, None, None): [[self.indexer[4:]],
                               [self.indexer[4:5], self.indexer[5:]]]
             }
        self.assertEqual(e, r)


class Test_group_keys(unittest.TestCase):
    def setUp(self):
        class Foo(object):
            def __getitem__(self, keys):
                return keys
        self.ind = Foo()
        self.maxDiff = None

    def test_one_group(self):
        colon = self.ind[:]
        r = group_keys((6, 2),
                       [(self.ind[:3], colon), (self.ind[3:], colon)],
                       [(colon, colon)])
        e = {((None, None, None), (None, None, None)): ([(self.ind[:3], colon), (self.ind[3:], colon)],
                                                        [(colon, colon)])
             }
        self.assertEqual(e, r)

    def test_two_groups_same_dim(self):
        colon = self.ind[:]
        r = group_keys((6, 2),
                       [(self.ind[:2], colon), (self.ind[2:4], colon), (self.ind[4:], colon)],
                       [(self.ind[:4], colon), (self.ind[4:], colon)])
        e = {((None, 4, None), (None, None, None)): ([(self.ind[:2], colon), (self.ind[2:4], colon)],
                                                     [(self.ind[:4], colon)]),
             ((4, None, None), (None, None, None)): ([(self.ind[4:], colon)],
                                                     [(self.ind[4:], colon)]),
             }
        self.assertEqual(sorted(e.keys()), sorted(r.keys()))
        self.assertEqual(e, r)

    def test_one_input(self):
        # One input always results in n keys groups (because they naturally split perfectly).
        colon = self.ind[:]
        r = group_keys((6, 2),
                       [(self.ind[:2], colon), (self.ind[2:4], colon), (self.ind[4:], colon)])
        e = {((None, 2, None), (None, None, None)): ([(self.ind[:2], colon)],),
             ((2, 4, None), (None, None, None)): ([(self.ind[2:4], colon)],),
             ((4, None, None), (None, None, None)): ([(self.ind[4:], colon)],)}
        self.assertEqual(sorted(e.keys()), sorted(r.keys()))
        self.assertEqual(e, r)

    def test_one_input_one_dim(self):
        r = group_keys((6,), [(slice(0, 5, None),), (slice(5, 6, None),)])
        e = {((None, 5, None),): ([(slice(0, 5, None),)],),
             ((5, None, None),): ([(slice(5, 6, None), )],)
             }
        self.assertEqual(sorted(e.keys()), sorted(r.keys()))
        self.assertEqual(e, r)


if __name__ == '__main__':
    if True:
        unittest.main()
    else:
        r = group_keys((6,), [(slice(0, 5, None),), (slice(5, 6, None),)])
        from pprint import pprint
        pprint(r)