from biggus import *
from biggus._init import Engine
import biggus._init
import numpy as np
import collections


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

        #: A place to track node info for naming and debug.
        self._node_info = {}

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

                sources_chunks = []
                for input_array in array.sources:
                    source_chunks_by_key = collections.defaultdict(list)
                    sources_chunks.append(source_chunks_by_key)

                    # Make nodes for the source arrays (if they don't already exist) before
                    # we do anything else.
                    self._make_nodes(dsk_graph, input_array, input_iteration_order,
                                     masked)
                    for chunk_id, chunks in self._node_cache[array_id(input_array, input_iteration_order, masked)].items():
                        keys = chunks[1]
                        transformer = getattr(handler, 'output_keys', lambda keys: keys)
                        source_chunks_by_key[str(transformer(keys))].append([chunk_id, chunks])
                all_chunks = tuple(self._node_cache[array_id(arr, input_iteration_order, masked)].items()
                                   for arr in array.sources)
                all_keys = []
                [all_keys.extend(chunks_by_key.keys()) for chunks_by_key in sources_chunks]
                all_keys = set(all_keys)

                def produce_chunks(produced_keys, *all_chunks):
                    import random
                    import time
                    time.sleep(random.randint(0, 1000) / 10000.)

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
                    print('all:', all_chunks)
                    process_result = None
                    for chunks in all_chunks:
                        process_result = node.process_chunks(chunks)
                    result = node.finalise()
                    if result is None:
                        # Itself returns a chunk.
                        return process_result
                    else:
                        return result

                for key in all_keys:
                    # For all source, pick out all tasks that match the current key.
                    all_chunks = [[key_id for key_id, chunk in chunks_by_key[key]]
                                  for chunks_by_key in sources_chunks]
                    all_chunks_real = [[chunk for key_id, chunk in chunks_by_key[key]]
                                  for chunks_by_key in sources_chunks]
                    # Pivot the chunks so that they come in groups, one per source.
                    pivoted = zip(*all_chunks)
                    produce_chunks.__name__ = handler.__class__.__name__
                    # Flatten out the pivot so that dask de-references the IDs
                    import random
                    chunk = [item for sublist in all_chunks_real for item in sublist][0]
                    keys = chunk[1]
                    task = tuple([produce_chunks, chunk[1]] + [item for sublist in pivoted for item in sublist])
                    chunk_id = ider(task)
                    dsk_graph[chunk_id] = task
                    nodes[chunk_id] = task
            else:
                node = biggus._init.ProducerNode(array, iteration_order[::-1], masked)
                chunks = []

                def biggus_chunk(chunk_key, biggus_array, masked):
                    if masked:
                        array = biggus_array.masked_array()
                    else:
                        array = biggus_array.ndarray()

                    return biggus._init.Chunk(chunk_key, array)

                biggus_chunk.__name__ = '{}'.format(array.__class__.__name__)

                for chunk_key in node.chunk_index_gen():
                    biggus_array = node.array[chunk_key]
                    chunk_id = array_id(biggus_array, iteration_order, masked)
                    task = (biggus_chunk, chunk_key, biggus_array, masked)
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
    biggus.engine = e
#     biggus._init.MAX_CHUNK_SIZE = 2 * 8 * 1024
    
    if True:
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
        graph = e.graph(expr)

        dask.dot.dot_graph(graph)
        pprint(graph)
        print(e.ndarrays(delta)[0].shape)
    
    print('-' * 80)
    
    if True:
        shape = (500, 30, 40)
        size = np.prod(shape)
        raw_data = np.linspace(0, 1, num=size).reshape(shape)
        counter = raw_data
        array = biggus.NumpyArrayAdapter(counter)
        mean_array = biggus.mean(array, axis=0)
        std_array = biggus.std(array, axis=0)
        graph = e.graph(mean_array, std_array)
        dask.dot.dot_graph(graph)
    print('-' * 80)
    
    if False:
        
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
        
        
    if True:
        dtype = np.float32

        def _biggus_filter(data, weights):
            # Filter a data array (time, <other dimensions>) using information in
            # weights dictionary.
            #
            # Args:
            #
            # * data:
            #     biggus array of the data to be filtered
            # * weights:
            #     dictionary of absolute record offset : weight
    
            # Build filter_matrix (time to time' mapping).
            shape = data.shape
    
            # Build filter matrix as a numpy array and then populate.
            filter_matrix_np = np.zeros((shape[0], shape[0])).astype(dtype)
    
            for offset, value in weights.items():
                filter_matrix_np += np.diag([value] * (shape[0] - offset),
                                            k=offset)
                if offset > 0:
                    filter_matrix_np += np.diag([value] * (shape[0] - offset),
                                                k=-offset)
    
            # Create biggus array for filter matrix, adding in other dimensions.
            for _ in shape[1:]:
                filter_matrix_np = filter_matrix_np[..., np.newaxis]
    
            filter_matrix_bg_single = biggus.NumpyArrayAdapter(filter_matrix_np)
    
            # Broadcast to correct shape (time, time', lat, lon).
            filter_matrix_bg = biggus.BroadcastArray(
                filter_matrix_bg_single, {i+2: j for i, j in enumerate(shape[1:])})
    
            # Broadcast filter to same shape.
            biggus_data_for_filter = biggus.BroadcastArray(data[np.newaxis, ...],
                                                           {0: shape[0]})
    
            # Multiply two arrays together and sum over second time dimension.
            filtered_data = biggus.sum(biggus_data_for_filter * filter_matrix_bg,
                                       axis=1)
    
            # Cut off records at start and end of output array where the filter
            # cannot be fully applied.
            filter_halfwidth = len(weights) - 1
            filtered_data = filtered_data[filter_halfwidth:-filter_halfwidth]
    
            return filtered_data

        def test__biggus_filter():
            shape = (1451, 1, 1)
    
            # Generate dummy data as biggus array.
            numpy_data = np.random.random(shape).astype(dtype)
            biggus_data = biggus.NumpyArrayAdapter(numpy_data)
    
            # Information for filter...
            # Dictionary of weights: key = offset (absolute value), value = weight
            weights = {0: 0.4, 1: 0.2, 2: 0.1}
            # This is equivalent to a weights array of [0.1, 0.2, 0.4, 0.2, 0.1].
            filter_halfwidth = len(weights) - 1
    
            # Filter data
            filtered_biggus_data = _biggus_filter(biggus_data, weights)
    
            # Extract eddy component (original data - filtered data).
            eddy_biggus_data = (biggus_data[filter_halfwidth:-filter_halfwidth] -
                                filtered_biggus_data)
    
            # Aggregate over time dimension.
            mean_eddy_biggus_data = biggus.mean(eddy_biggus_data, axis=0)
    
            graph = e.graph(mean_eddy_biggus_data)
            pprint(graph)
            dask.dot.dot_graph(graph)
            # Force evaluation.
            mean_eddy_numpy_data = mean_eddy_biggus_data.ndarray()
    
            # Confirm correct shape.
            np.testing.assert_array_equal(mean_eddy_numpy_data.shape, shape[1:])

        test__biggus_filter()