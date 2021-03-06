# (C) British Crown Copyright 2012 - 2013, Met Office
#
# This file is part of Biggus.
#
# Biggus is free software: you can redistribute it and/or modify it under
# the terms of the GNU Lesser General Public License as published by the
# Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Biggus is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with Biggus. If not, see <http://www.gnu.org/licenses/>.
import unittest

import numpy as np

import biggus


class TestAdapter(unittest.TestCase):
    longMessage = True

    def test_dtype(self):
        dtypes = ['f4', 'i1', 'O', 'm8', '<f4', '>f4', '=f4']
        keys = [(), (5,), (slice(1, 3),)]
        for dtype in dtypes:
            for key in keys:
                ndarray = np.zeros(10, dtype=dtype)
                array = biggus.ArrayAdapter(ndarray, keys=key)
                self.assertEqual(array.dtype, np.dtype(dtype))

    def test_shape_0d(self):
        pairs = [
            [(), ()],
        ]
        for key, shape in pairs:
            ndarray = np.zeros(())
            array = biggus.ArrayAdapter(ndarray, keys=key)
            self.assertEqual(array.shape, shape)

    def test_shape_1d(self):
        pairs = [
            [(), (10,)],
            [(5,), ()],
            [(slice(1, 3),), (2,)],
        ]
        for key, shape in pairs:
            ndarray = np.zeros(10)
            array = biggus.ArrayAdapter(ndarray, keys=key)
            self.assertEqual(array.shape, shape)

    def test_shape_2d(self):
        pairs = [
            [(), (30, 40)],
            [(5,), (40,)],
            [(slice(1, 3),), (2, 40)],
            [(slice(None, None),), (30, 40)],
            [(5, 3), ()],
            [(5, slice(2, 6)), (4,)],
            [(slice(2, 3), slice(2, 6)), (1, 4)],
        ]
        for key, shape in pairs:
            ndarray = np.zeros((30, 40))
            array = biggus.ArrayAdapter(ndarray, keys=key)
            self.assertEqual(array.shape, shape)

    def test_getitem(self):
        # Sequence of tests, defined as:
        #   1. Original array shape,
        #   2. sequence of indexing operations to apply,
        #   3. expected result shape or exception.
        tests = [
            [(30, 40), [], (30, 40)],
            [(30, 40), [5], (40,)],
            [(30, 40), [(5,)], (40,)],
            [(30, 40), [5, 3], ()],
            [(30, 40), [(5,), (4,)], ()],
            [(30, 40), [(slice(None, None), 6)], (30,)],
            [(30, 40), [(slice(None, None), slice(1, 5))], (30, 4)],
            [(30, 40), [(slice(None, None),), 4], (40,)],
            [(30, 40), [5, (slice(None, None),)], (40,)],
            [(30, 40), [(slice(None, 10),)], (10, 40)],
            [(30, 40), [(slice(None, None),)], (30, 40)],
            [(30, 40), [(slice(None, None, -2),)], (15, 40)],
            [(30, 40), [(slice(None, 10),), 5], (40,)],
            [(30, 40), [(slice(None, 10),), (slice(None, 3),)], (3, 40)],
            [(30, 40), [(slice(None, 10),), (slice(None, None, 2),)], (5, 40)],
            [(30, 40), [(slice(5, 10),),
                        (slice(None, None), slice(2, 6))], (5, 4)],
            [(30, 40), [(slice(None, None), slice(2, 6)),
                        (slice(5, 10),)], (5, 4)],
            [(30, 40), [3.5], TypeError],
            [(30, 40), ['foo'], TypeError],
            [(30, 40), [object()], TypeError],
            # Fancy indexing
            [(21, 5, 70, 30, 40), [((1, 5), 0, (2, 5, 10), slice(None, 15))],
                (2, 3, 15, 40)],
            [(21, 5, 2, 70, 30, 40), [(0, (1, 4), 1, (2, 5, 10),
                                       slice(None, 15))], (2, 3, 15, 40)],
        ]
        for src_shape, cuts, target in tests:
            ndarray = np.zeros(src_shape)
            array = biggus.ArrayAdapter(ndarray)
            if isinstance(target, type):
                with self.assertRaises(target):
                    for cut in cuts:
                        array = array.__getitem__(cut)
            else:
                for cut in cuts:
                    array = array.__getitem__(cut)
                    self.assertIsInstance(array, biggus.Array)
                msg = '\nCuts: {!r}'.format(cuts)
                self.assertEqual(array.shape, target, msg)
                ndarray = array.ndarray()
                self.assertEqual(ndarray.shape, target, msg)

    def test_ndarray(self):
        tests = [
            [(3,), (), [0, 1, 2]],
            [(3,), (1,), [1]],
            [(3,), (slice(None, None, 2),), [0, 2]],
            [(3, 4), (), [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9, 10, 11]]],
            [(3, 4), (1, ), [4, 5, 6, 7]],
            [(3, 4), (1, 3), 7],
        ]
        for src_shape, src_keys, target in tests:
            size = reduce(lambda x, y: x * y, src_shape)
            ndarray = np.arange(size).reshape(src_shape)
            array = biggus.ArrayAdapter(ndarray, keys=src_keys)
            result = array.ndarray()
            self.assertIsInstance(result, np.ndarray)
            self.assertEqual(array.dtype, result.dtype)
            self.assertEqual(array.shape, result.shape,
                             '\nKeys: {!r}'.format(src_keys))
            np.testing.assert_array_equal(result, target)


if __name__ == '__main__':
    unittest.main()
