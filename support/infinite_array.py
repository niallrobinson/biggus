import biggus
import numpy as np


class Integers(biggus.Array):
    def __init__(self, start=0, step=1):
        super(Integers, self).__init__()
        self.start = start
        self.step = step

    @property
    def shape(self):
        return (np.inf, )

    @property
    def dtype(self):
        return np.int32

    @property
    def nbytes(self):
        """The total number of bytes required to store the array data."""
        return np.inf

    def ndarray(self):
        raise MemoryError('You have infinite memory, huh?')

    def masked_array(self):
        raise MemoryError('You have infinite memory, huh?')

    def _getitem_full_keys(self, keys):
        # TODO: Prevent negative indexes being interpreted.
        if len(keys) != 1:
            raise IndexError('Unexpected keys when indexing')
        key = keys[0]
        if isinstance(key, slice):
            # TODO: Worry about [::-1]
            start = (key.start or 0) + self.start
            step = (key.step or 1) * self.step

            stop = key.stop
            if stop is None:
                # We maintain an infinite sequence.
                return Integers(start, step)
            else:
                # We know how long the sequence should be.
                return np.arange(start, stop + self.start, step)

#            if key == slice(None):
#                return self
#            else:
#                if key.stop is None:
#                    return Integers(start + self.start)
#                else:
#                    return np.arange(start + self.start,
#                                     key.stop + self.start,
#                                     key.step)
        elif biggus._is_scalar(key):
            if key < 0:
                raise IndexError('Cannot index an infinite sequence with a '
                                 'negative index.')
            return key + self.start
        else:
            raise IndexError('Unsupported key ({}) for indexing.'.format(type(key)))


non_negative = Integers(0)
natural = Integers(1)

a = non_negative

print a.shape

print a[0:10], a[0], a[10]

print a[1:]

print (a[1:] - a)[:10].ndarray()




if False:
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
