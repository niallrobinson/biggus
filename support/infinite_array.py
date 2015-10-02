import biggus
import numpy as np


class Rationals(biggus.Array):
    def __init__(self, start=0, step=1, count=np.inf, dtype=None):
        super(Rationals, self).__init__()
        if dtype is None:
            dtype = np.find_common_type([], [np.array(start).dtype,
                                             np.array(step).dtype,
                                             np.array(count).dtype])
        self.start = start
        self.step = step
        self.count = count
        self._dtype = dtype

    @property
    def shape(self):
        return (self.count, )

    @property
    def dtype(self):
        return self._dtype

    @property
    def nbytes(self):
        """The total number of bytes required to store the array data."""
        return self.count * self.dtype.itemsize

    def ndarray(self):
        if np.isinf(self.count):
            raise MemoryError("Could not realise the Rationals array - it has"
                              "infinite length, for which you don't have "
                              "enough memory.")
        

    def masked_array(self):
        return np.ma.masked_array(self.ndarray())

    def _getitem_full_keys(self, keys):
        # TODO: Prevent negative indexes being interpreted.
        if len(keys) != 1:
            raise IndexError('Unexpected keys when indexing ().'.format(keys))
        key = keys[0]

        if isinstance(key, slice):
            # TODO: Worry about [::-1]
            start = (key.start or 0) + self.start
            step = (key.step or 1) * self.step

            stop = key.stop
            if stop is None:
                # We maintain an infinite sequence.
                return Rationals(start, step)
            else:
                # We know how long the sequence should be.
                return np.arange(start, stop + self.start, step, dtype=self.dtype)
        elif biggus._is_scalar(key):
            if key < 0:
                raise IndexError('Cannot index an infinite sequence with a '
                                 'negative index.')
            return key + self.start
        else:
            raise IndexError('Unsupported key ({}) for indexing.'.format(type(key)))


non_negative = Rationals(0)
natural = Rationals(1)

a = non_negative

print a.shape

print a[0:10], a[0], a[10]

print a[1:]

print (a[1:] - a)[:10].ndarray()




if True:
    x = a[np.newaxis, :]
    print type(x)
    print x.shape
    print x
    print x[:, 0:10].shape

    r = x * a[:, np.newaxis]
    print a[:, np.newaxis]
    print r

    print r[0:10, 0:10].ndarray()

    print r[10:20, 0:10].ndarray()
