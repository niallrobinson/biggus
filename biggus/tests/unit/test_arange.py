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
"""Unit tests for `biggus.arange`."""
from __future__ import division

import unittest

import numpy as np

from biggus import arange


class Test_arange(unittest.TestCase):
    def test_no_args(self):
        a = arange()
        self.assertEqual(a.start, 0)
        self.assertEqual(a.step, 1)
        self.assertEqual(a.count, np.inf)
        self.assertEqual(a.dtype, np.dtype('int64'))

    def test_no_args_dtype(self):
        a = arange(dtype='int16')
        self.assertEqual(a.dtype, np.dtype('int16'))

    def test_no_args_step_defined(self):
        a = arange(step=0.5)
        self.assertEqual(a.start, 0)
        self.assertEqual(a.step, 0.5)
        self.assertEqual(a.count, np.inf)
        self.assertEqual(list(a[:4].ndarray()), [0.0, 0.5, 1.0, 1.5])
        self.assertEqual(a.dtype, np.dtype('float64'))

    def test_one_arg(self):
        a = arange(10)
        self.assertEqual(a.start, 0)
        self.assertEqual(a.step, 1)
        self.assertEqual(a.count, 10)
        self.assertEqual(a.dtype, np.dtype('int64'))

    def test_one_arg_float(self):
        a = arange(4.)
        self.assertEqual(a.start, 0)
        self.assertEqual(a.step, 1)
        self.assertEqual(a.count, 4.0)
        self.assertEqual(a.shape, (4, ))
        self.assertEqual(a.dtype, np.dtype('float64'))

    def test_one_arg_dtype(self):
        a = arange(2j, dtype='float32')
        self.assertEqual(a.dtype, np.dtype('float32'))

    def test_two_arg_float_start(self):
        a = arange(2., 4)
        self.assertEqual(a.start, 2.0)
        self.assertEqual(a.step, 1)
        self.assertEqual(a.count, 2)
        self.assertEqual(a.dtype, np.dtype('float64'))

    def test_two_arg_float_stop(self):
        a = arange(2, 4.)
        self.assertEqual(a.dtype, np.dtype('float64'))

    def test_two_arg_dtype_defined(self):
        a = arange(2, 4., dtype='int16')
        self.assertEqual(a.dtype, np.dtype('int16'))

    def test_three_arg_dtype_defined(self):
        a = arange(2, 4., dtype='int16')
        self.assertEqual(a.dtype, np.dtype('int16'))


if __name__ == '__main__':
    unittest.main()
