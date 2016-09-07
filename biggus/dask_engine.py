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


class DaskGroup(AllThreadedEngine.Group):
    def __init__(self, arrays):
        self.arrays = arrays
        self._node_cache = {}

    def _make_nodes(self, dsk_graph, array, iteration_order, masked, top=False):
        cache_key = ider(array)
        nodes = self._node_cache.get(cache_key, None)
        if nodes is None:
            nodes = {}
            if hasattr(array, 'streams_handler'):
                handler = array.streams_handler(masked)
                input_iteration_order = handler.input_iteration_order(iteration_order)
                for input_array in array.sources:
                    # Make nodes for the source arrays (if they don't already exist) before
                    # we do anything else.
                    self._make_nodes(dsk_graph, input_array, input_iteration_order,
                                     masked)

                all_chunks = tuple(self._node_cache[ider(arr)].keys()
                                   for arr in array.sources)

                node = biggus._init.StreamsHandlerNode(array,
                                          array.streams_handler(masked))

                def produce_chunks(*all_chunks):
                    process_result = node.process_chunks(*all_chunks)
                    result = node.finalise()
                    if result is None:
                        # Itself returns a chunk.
                        return process_result
                    else:
                        return result

                # Pair up the chunks for each source (TODO: Check that chunks line up...)
                for chunks in zip(*all_chunks):


                    produce_chunks.__name__ = handler.__class__.__name__
                    task = tuple([produce_chunks] + [list(chunks)])
#                     print(chunks)
                    # TODO: Pass a chunk to get a nicer ID.
                    chunk_id = ider(task)
                    dsk_graph[chunk_id] = task
                    nodes[chunk_id] = task
            else:
                node = biggus._init.ProducerNode(array, iteration_order, masked)
                chunks = []

                def biggus_chunk(biggus_array, chunk_key, masked):
                    if masked:
                        array = biggus_array.masked_array()
                    else:
                        array = biggus_array.ndarray()

                    return biggus._init.Chunk(chunk_key, array)

                biggus_chunk.__name__ = '{}'.format(array.__class__.__name__)

                for chunk_key in node.chunk_index_gen():
                    biggus_array = node.array[chunk_key]
                    chunk_id = ider(biggus_array)
#                     print(chunk_id, id(node.array[tuple(chunk_key)]))
#                     print('CHUNKY:', chunk_key)
                    task = (biggus_chunk, biggus_array, chunk_key, masked)
                    chunks.append(task)
                    dsk_graph[chunk_id] = task
                    nodes[chunk_id] = task
                print('CHUNKS:', chunks)
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
            array_id = ider(array)
            if array_id not in dsk_graph:
                dsk_graph[array_id] = (foo(array), list(self._node_cache[ider(array)].keys()))
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
        ids = [ider(array) for array in arrays]
        return self.dask_getter(self._daskify(arrays, True), ids)

    def ndarrays(self, *arrays):
        ids = [ider(array) for array in arrays]
        return self.dask_getter(self._daskify(arrays, False), ids)

    def graph(self, *arrays):
        return self._daskify(arrays)


if __name__ == '__main__':
    # Put the sys prefix directory on the path.
    import os, sys
    os.environ['PATH'] += ':{}/bin'.format(sys.prefix)
    
    import biggus
    from dask.multiprocessing import get

    e = DaskEngine()

    biggus._init.MAX_CHUNK_SIZE = 2 * 2
    a = biggus.zeros([8, 2, 2])
    add = a + 1
    b = biggus.mean(a - a, axis=0)
    m = biggus.mean(a, axis=0)
    m1 = biggus.var(add, axis=1)
    s = biggus.std(a, axis=0)
    delta = add - m

#     print e._daskify(a)
    from pprint import pprint
#     print pprint(e.graph(a + 1))
#     print pprint(e.ndarrays(a + 1, a - 1))
#     print pprint(e.ndarrays(biggus.mean(a, axis=0)))
    arr = biggus.mean(a + 1, axis=0)

    import dask.dot
    expr = add
    graph = e.graph(expr, b, s, s, m, delta)
    dask.dot.dot_graph(graph)
    pprint(graph)
    print(e.ndarrays(delta)[0].shape)
    
    biggus.engine = DaskEngine()
    import numpy.testing
    axis = 0
    data = np.arange(3 * 4, dtype='f4').reshape(3, 4)
    array = biggus.NumpyArrayAdapter(data)
    mean = biggus.mean(array, axis=axis)
    op_result, = biggus.ndarrays([mean])
    np_result = np.mean(data, axis=axis)
    print(op_result)
    print(np_result)
    graph = e.graph(mean)
    dask.dot.dot_graph(graph)
    pprint(graph)


    import dask.threaded
    dask_getter = dask.threaded.get
    targets = sorted(graph.keys())
    results = dask_getter(graph, targets)
    pprint(dict(zip(targets, results)))

    np.testing.assert_array_almost_equal(op_result, np_result)
