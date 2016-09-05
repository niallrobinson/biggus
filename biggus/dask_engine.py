from biggus import *
from biggus._init import Engine
import biggus._init
import numpy as np


def ider(array):
    return 'id_{}'.format(str(id(array)))


class DaskGroup(AllThreadedEngine.Group):
    def __init__(self, arrays):
        self.arrays = arrays
        self._node_cache = {}

    def _make_nodes(self, dsk_graph, array, iteration_order, masked, top=False):
        cache_key = id(array)
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

                all_chunks = tuple(self._node_cache[id(arr)].keys()
                                   for arr in array.sources)

                node = biggus._init.StreamsHandlerNode(array,
                                          array.streams_handler(masked))
                iteration_order = node.input_iteration_order(
                    iteration_order)
                sub_tasks = []
                # Pair up the chunks for each source (TODO: Check that chunks line up...)
                for chunks in zip(*all_chunks):
                    def produce_chunks(*all_chunks):
#                         for chunks in all_chunks:
#                             assert all(chunk.keys == chunks[0].keys for chunk in chunks)
                        process_result = node.process_chunks(*all_chunks)
                        result = node.finalise()
                        if result is None:
                            # Itself returns a chunk.
                            return process_result
                        else:
                            return result

                    produce_chunks.__name__ = handler.__class__.__name__
                    task = tuple([produce_chunks] + [list(chunks)])
                    sub_tasks.append(ider(task))
                    dsk_graph[ider(task)] = task
                    nodes[ider(task)] = task
            else:
                node = biggus._init.ProducerNode(array, iteration_order, masked)
                chunks = []
                def lazy_chunks(chunk_key):
                    subarray = node.array[chunk_key]
                    if masked:
                        fn = subarray.masked_array
                    else:
                        fn = subarray.ndarray
                    return biggus._init.Chunk(chunk_key, fn())
                lazy_chunks.__name__ = '{}'.format(array.__class__.__name__)

                for chunk_key in node.chunk_index_gen():
                    task = (lazy_chunks, chunk_key)
                    chunks.append(task)

                    dsk_graph[ider(task)] = task
                    nodes[ider(task)] = task

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
                def make_result(all_chunks):
                    result_node = biggus._init.NdarrayNode(array, masked)
                    for chunks in all_chunks:
                        # TODO: Factor it so that this isn't necessary...
                        if isinstance(chunks, biggus._init.Chunk):
                            chunks = [chunks]
                        result_node.process_chunks(chunks)
                    return result_node.result
                return make_result
            if ider(array) not in dsk_graph:
                dsk_graph[ider(array)] = (foo(array), list(self._node_cache[id(array)].keys()))
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
            dask_getter = dask.multiprocessing.get
        self.dask_getter = dask_getter

    def _daskify(self, arrays, masked=False):
        return DaskGroup(arrays).dask()

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

    biggus._init.MAX_CHUNK_SIZE = 2 * 8
    a = biggus.zeros([8, 2, 2])
    add = a + 1
    b = biggus.mean(a - a, axis=0)
    m = biggus.mean(a, axis=0)
    s = biggus.std(a, axis=0)

#     print e._daskify(a)
    from pprint import pprint
#     print pprint(e.graph(a + 1))
#     print pprint(e.ndarrays(a + 1, a - 1))
#     print pprint(e.ndarrays(biggus.mean(a, axis=0)))
    arr = biggus.mean(a + 1, axis=0)

    import dask.dot
    expr = add
    graph = e.graph(expr, b, s, s, m)
    dask.dot.dot_graph(graph)
    pprint(graph)
    print(ider(expr))
    print(get(graph, ider(expr)))
    print(get(graph, ider(s)))

    dask_graph = e.ndarrays(arr)
