from biggus import *
from biggus._init import Engine
import biggus._init
import numpy as np


def ider(input):
    if isinstance(input, biggus._init.Chunk):
        result = 'chunk {} <{}>'.format(input.shape, id(input))
    elif isinstance(input, biggus._init.Array):
        result = 'array {} <{}>'.format(input.shape, id(input))
    else:
        result = 'id_{}'.format(id(input))
    return result

def array_id(array, iteration_order=None, masked=False):
    if iteration_order is None:
        iteration_order = range(array.ndim)
    result = '{}array {}; chunk-order: {}; id <{}>;'.format('[masked]' if masked else '',
                                                            array.shape, ','.join(map(str, iteration_order)),
                                                            id(array))
    return result

class DaskGroup(AllThreadedEngine.Group):
    def __init__(self, arrays):
        self.arrays = arrays
        self._node_cache = {}

    def _make_nodes(self, dsk_graph, array, iteration_order, masked, top=False):
        cache_key = array_id(array, iteration_order, masked)
        nodes = self._node_cache.get(cache_key, None)
        if nodes is None:
            nodes = {}
            if hasattr(array, 'streams_handler'):
                node = biggus._init.StreamsHandlerNode(array,
                                          array.streams_handler(masked))
                handler = array.streams_handler(masked)
                input_iteration_order = handler.input_iteration_order(iteration_order)
                print('iT order:', iteration_order, input_iteration_order)
                import collections
                sources_chunks = []
                for input_array in array.sources:
                    source_chunks_by_key = collections.defaultdict(list)
                    sources_chunks.append(source_chunks_by_key)

                    # Make nodes for the source arrays (if they don't already exist) before
                    # we do anything else.
                    self._make_nodes(dsk_graph, input_array, input_iteration_order,
                                     masked)
                    for chunk_id, chunks in self._node_cache[array_id(input_array, input_iteration_order, masked)].items():
                        print('blah:', chunks)
                        keys = chunks[2]
                        print(keys)
                        transformer = getattr(handler, 'output_keys', lambda keys: keys)
                        source_chunks_by_key[str(transformer(keys))].append([chunk_id, chunks])
                all_chunks = tuple(self._node_cache[array_id(arr, input_iteration_order, masked)].items()
                                   for arr in array.sources)
                all_keys = []
                [all_keys.extend(chunks_by_key.keys()) for chunks_by_key in sources_chunks]
                all_keys = set(all_keys)



                def produce_chunks(*all_chunks):
                    import random
                    import time
                    
                    try:
                        from itertools import izip_longest as zip_longest
                    except ImportError:
                        from itertools import zip_longest
                    def grouper(iterable, n, fillvalue=None):
                        "Collect data into fixed-length chunks or blocks"
                        # grouper('ABCDEFG', 3, 'x') --> ABC DEF Gxx
                        args = [iter(iterable)] * n
                        return zip_longest(fillvalue=fillvalue, *args)
                    
                    all_chunks = list(grouper(all_chunks, len(array.sources)))
                    
                    time.sleep(random.randint(0, 10) / 10.)
                    print('all_chunks', all_chunks)
                    for chunks in all_chunks:
                        print('chunks:', chunks)
                        process_result = node.process_chunks(*[chunks])
                    result = node.finalise()
                    if result is None:
                        # Itself returns a chunk.
                        return process_result
                    else:
                        return result

                for key in all_keys:
                    print('key:', key)
                    # For all source, pick out all tasks that match the current key.
                    all_chunks = [[key_id for key_id, chunk in chunks_by_key[key]]
                                  for chunks_by_key in sources_chunks]
                    print('asdads:', key)
                    print(all_chunks)
                    # Pivot the chunks so that they come in groups, one per source.
                    pivoted = zip(*all_chunks)

                    produce_chunks.__name__ = handler.__class__.__name__
                    # Flatten out the pivot so that dask de-references the IDs
                    task = tuple([produce_chunks] + [item for sublist in pivoted for item in sublist])
                    chunk_id = ider(task)
                    dsk_graph[chunk_id] = task
                    nodes[chunk_id] = task

                # Pair up the chunks for each source (TODO: Check that chunks line up...)
#                 for chunks in zip(*all_chunks):
#                     print(chunks)
#                     keys, chunks = zip([zip(*pair) for pair in chunks])
#                     
#                     print('keys:', chunks)
#                     print(keys)
#                     produce_chunks.__name__ = handler.__class__.__name__
#                     task = tuple([produce_chunks] + [list(chunks)])
# #                     print(chunks)
#                     # TODO: Pass a chunk to get a nicer ID.
#                     chunk_id = ider(task)
#                     dsk_graph[chunk_id] = task
#                     nodes[chunk_id] = task
            else:
                print('it_order:', iteration_order, iteration_order[::-1])
                node = biggus._init.ProducerNode(array, iteration_order[::-1], masked)
                chunks = []

                def biggus_chunk(biggus_array, chunk_key, masked):
                    if masked:
                        array = biggus_array.masked_array()
                    else:
                        array = biggus_array.ndarray()

                    return biggus._init.Chunk(chunk_key, array)

                biggus_chunk.__name__ = '{}'.format(array.__class__.__name__)

                for chunk_key in node.chunk_index_gen():
                    print('Chunk:', chunk_key)
                    biggus_array = node.array[chunk_key]
                    chunk_id = array_id(biggus_array, iteration_order, masked)
#                     print(chunk_id, id(node.array[tuple(chunk_key)]))
#                     print('CHUNKY:', chunk_key)
                    task = (biggus_chunk, biggus_array, chunk_key, masked)
                    chunks.append(task)
                    dsk_graph[chunk_id] = task
                    nodes[chunk_id] = task
            self._node_cache[cache_key] = nodes
        return nodes

    def dask(self, masked=False):
        # Construct nodes starting from the producers.
        dsk_graph = {}
#         result_nodes = []
#         result_threads = []
        for array in self.arrays:
            self.dask_task(dsk_graph, array, masked=masked, top=True)
            def foo(array):
                def collect_array(all_chunks):
                    result_node = biggus._init.NdarrayNode(array, masked)
                    for chunks in all_chunks:
                        # TODO: Factor it so that this isn't necessary...
                        if isinstance(chunks, biggus._init.Chunk):
                            chunks = [chunks]
                        result_node.process_chunks(chunks)
                    return result_node.result
                return collect_array
            array_id_val = array_id(array, masked=masked)
            if array_id_val not in dsk_graph:
                dsk_graph[array_id_val] = (foo(array), list(self._node_cache[array_id_val].keys()))
        return dsk_graph

    def dask_task(self, dsk_graph, array, masked=False, top=False):
        return self._make_nodes(dsk_graph, array, range(array.ndim), masked)


class DaskEngine(Engine):
    """
    An engine that converts the biggus expression graph into a
    dask task graph.

    """
    def __init__(self, dask_getter=None):
        if dask_getter is None:
            import dask.multiprocessing
            import dask.threaded
            dask_getter = dask.threaded.get
        self.dask_getter = dask_getter

    def _daskify(self, arrays, masked=False):
        return DaskGroup(arrays).dask(masked)

    def masked_arrays(self, *arrays):
        ids = [array_id(array, masked=True) for array in arrays]
        return self.dask_getter(self._daskify(arrays, masked=True), ids)

    def ndarrays(self, *arrays):
        ids = [array_id(array, masked=False) for array in arrays]
        return self.dask_getter(self._daskify(arrays, masked=False), ids)

    def graph(self, *arrays):
        return self._daskify(arrays)


if __name__ == '__main__':
    # Put the sys prefix directory on the path.
    import os, sys
    os.environ['PATH'] += ':{}/bin'.format(sys.prefix)
    
    import biggus
    from dask.multiprocessing import get
    import dask.dot
    from pprint import pprint

    e = DaskEngine()

    biggus._init.MAX_CHUNK_SIZE = 2 * 2
    
    if False:
        a = biggus.zeros([8, 2, 2])
        add = a + 1
        b = biggus.mean(a - a, axis=0)
        m = biggus.mean(a, axis=0)
        m1 = biggus.var(add, axis=1)
        s = biggus.std(a, axis=0)
        delta = add - m
     
    #     print e._daskify(a)
    #     print pprint(e.graph(a + 1))
    #     print pprint(e.ndarrays(a + 1, a - 1))
    #     print pprint(e.ndarrays(biggus.mean(a, axis=0)))
        arr = biggus.mean(a + 1, axis=0)
     
        
        expr = add
        graph = e.graph(expr, b, s, s, m, delta)
        dask.dot.dot_graph(graph)
        pprint(graph)
        print(e.ndarrays(delta)[0].shape)
    
    print('-' * 80)
    
    biggus.engine = DaskEngine()
    import numpy.testing
    axis = 0
    data = np.arange(3 * 4 * 2, dtype='f4').reshape(3, 4, -1)
    array = biggus.NumpyArrayAdapter(data)
    mean = biggus.mean(array, axis=axis)
    
    graph = e.graph(mean)
    dask.dot.dot_graph(graph)
    pprint(graph)

    op_result, = biggus.ndarrays([mean])
    np_result = np.mean(data, axis=axis)
    print(op_result)
    print(np_result)


    import dask.threaded
    dask_getter = dask.threaded.get
    targets = sorted(graph.keys())
    results = dask_getter(graph, targets)
    pprint(dict(zip(targets, results)))

    np.testing.assert_array_almost_equal(op_result, np_result)
