# (C) British Crown Copyright 2015, Met Office
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
"""Unit tests for `biggus.Rationals`."""
from __future__ import division

import unittest

import numpy as np
from numpy.testing import assert_array_equal

from biggus import Rationals


class Test_attributes(unittest.TestCase):
    def test_dtype_computed(self):
        array = Rationals(2, 3)
        self.assertEqual(array.dtype, np.dtype('int64'))

    def test_dtype_computed_float_start(self):
        array = Rationals(1.5, 1)
        self.assertEqual(array.dtype, np.dtype('float64'))

    def test_dtype_computed_float_step(self):
        array = Rationals(2, 0.5)
        self.assertEqual(array.dtype, np.dtype('float64'))

    def test_dtype_computed_complex_step(self):
        array = Rationals(2, 0.5j)
        self.assertEqual(array.dtype, np.dtype('complex128'))

    def test_shape(self):
        array = Rationals()
        self.assertEqual(array.shape, (np.inf, ))

    def test_shape_integers(self):
        array = Rationals(0, 10, 4.)
        self.assertIsInstance(array.shape[0], int)

    def test_stop_positive_inf(self):
        array = Rationals()
        self.assertEqual(array.stop, np.inf)

    def test_stop_negative_inf(self):
        array = Rationals(step=-1)
        self.assertEqual(array.stop, -np.inf)

    def test_stop_finite(self):
        array = Rationals(1, step=0.5, count=11)
        self.assertEqual(array.stop, 6.5)

    def test_nbytes_infinite(self):
        array = Rationals(dtype=np.dtype('f2'))
        self.assertEqual(array.nbytes, np.inf)

    def test_nbytes_finite(self):
        array = Rationals(1, step=0.5, count=11,
                          dtype=np.dtype('f2'))
        self.assertEqual(array.nbytes, 22)

    def test_ndarray_infinite(self):
        array = Rationals()
        msg = 'Could not realise the Rationals array - it has infinite length'
        with self.assertRaisesRegexp(MemoryError, msg):
            array.ndarray()

    def test_ndarray_finite_positive_step(self):
        array = Rationals(1, 0.5, 5, dtype=np.dtype('float64'))
        assert_array_equal(array.ndarray(),
                           np.array([1., 1.5, 2., 2.5, 3.],
                                     dtype=np.dtype('float64')))

    def test_ndarray_finite_negative_step(self):
        array = Rationals(1, -0.5, 5, dtype=np.dtype('float64'))
        assert_array_equal(array.ndarray(),
                           np.array([1., 0.5, 0., -0.5, -1.],
                                    dtype=np.dtype('float64')))


class Test___getitem__(unittest.TestCase):
    def test_infinite_to_infinite_float_step(self):
        array = Rationals(start=10, step=2, dtype=np.dtype('int64'))
        result = array[2::2.5]
        self.assertEqual(result.step, 5)
        self.assertEqual(result.dtype, np.dtype('float64'))
        self.assertEqual(result.stop, np.inf)
        self.assertEqual(result.start, 12)

    def test_finite_to_finite_float_step(self):
        array = Rationals(start=10, step=2, count=5)
        result = array[2::2.5]
        self.assertEqual(result.step, 5)
        self.assertEqual(result.count, 5)
        self.assertEqual(result.start, 12)

    def test_infinite_to_infinite_negative_indexing(self):
        array = Rationals(start=10, step=2, dtype=np.dtype('int64'))
        result = array[::-1]
        self.assertEqual(result.step, -2)
        self.assertEqual(result.dtype, np.dtype('int64'))
        self.assertEqual(result.stop, -np.inf)
        self.assertEqual(result.start, 10)

    def test_infinite_to_finite(self):
        array = Rationals(start=10, step=2, dtype=np.dtype('int64'))
        result = array[:5]
        self.assertEqual(result.stop, 20)

    def test_infinite_to_finite_negative_indexing(self):
        array = Rationals(start=10, step=2, dtype=np.dtype('int64'))
        msg = ''
        with self.assertRaisesRegexp(IndexError, msg):
            array[:-2]

    def test_finite_to_finite_negative_indexing(self):
        array = Rationals(start=10, step=2, count=5,
                          dtype=np.dtype('int64'))
        result = array[:-2]
        self.assertEqual(result.stop, 16)

    def test_multiple_keys(self):
        array = Rationals(start=10, step=2, dtype=np.dtype('int64'))
        msg = ''
        with self.assertRaisesRegexp(IndexError, msg):
            array[:-2, 9, np.newaxis]

    def test_newaxis(self):
        array = Rationals()
        result = array[np.newaxis, :]
        self.assertEqual(result.shape, (1, np.inf))

    def test_ellipsis(self):
        array = Rationals()
        result = array[...]
        self.assertEqual(result.shape, (np.inf,))

    def test_scalar_index(self):
        array = Rationals(5, 1.5, count=6)
        self.assertEqual(array[0], 5)
        self.assertEqual(array[1], 6.5)
        self.assertEqual(array[-1], 12.5)

        # Numpy raises an index error for these.
        self.assertEqual(array[-7], 3.5)
        self.assertEqual(array[6], 14.0)

    def test_negative_scalar_index_of_inf_array(self):
        array = Rationals(5, 1.5)
        msg = 'Cannot index an infinite sequence with a negative index'
        with self.assertRaisesRegexp(IndexError, msg):
            array[-1]


if __name__ == '__main__':
    unittest.main()
