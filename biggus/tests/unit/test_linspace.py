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
"""Unit tests for `biggus.linspace`."""
from __future__ import division

import unittest

import numpy as np

from biggus import linspace


class Test_linspace(unittest.TestCase):
    def test_simple(self):
        a = linspace(0, 10, num=5)
        self.assertEqual(list(a.ndarray()), [0.0, 2.5, 5.0, 7.5, 10.0])
        self.assertEqual(a.start, 0)
        self.assertEqual(a.step, 2.5)
        self.assertEqual(a.count, 5)
        self.assertEqual(a.dtype, np.dtype('float64'))

    def test_negative(self):
        a = linspace(10, 0, num=5)
        self.assertEqual(a.start, 10)
        self.assertEqual(a.step, -2.5)
        self.assertEqual(a.count, 5)
        self.assertEqual(a.dtype, np.dtype('float64'))

    def test_retstep(self):
        a, step = linspace(10, 0, num=5, retstep=True)
        self.assertEqual(a.start, 10)
        self.assertEqual(a.step, step)
        self.assertEqual(step, -2.5)
        self.assertEqual(a.count, 5)
        self.assertEqual(a.dtype, np.dtype('float64'))

    def test_dtype(self):
        a = linspace(10, 0, num=5, dtype='int32')
        self.assertEqual(a.dtype, np.dtype('int32'))

    def test_endpoint(self):
        a = linspace(10, 0, num=2, endpoint=False)
        self.assertEqual(list(a.ndarray()), [10, 5])
        self.assertEqual(a.start, 10)
        self.assertEqual(a.step, -5)
        self.assertEqual(a.count, 2)


if __name__ == '__main__':
    unittest.main()
