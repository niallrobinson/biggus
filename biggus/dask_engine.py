from biggus import *
from biggus._init import Engine
import biggus._init
import numpy as np
import collections
import copy
from uuid import uuid4


def array_id(array, iteration_order=None, masked=False):
    if iteration_order is None:
        iteration_order = range(array.ndim)
    result = '{}array {}\n\n(id: {})'.format('[masked]' if masked else '',
                                             array.shape, id(array))
    return result


def slice_repr(slice_instance):
    """
    Turn things like `slice(None, 2, -1)` into `[:2:-1]`.

    """
    if not isinstance(slice_instance, slice):
        raise TypeError('Unhandled type {}'.format(type(slice_instance)))
    start = slice_instance.start or ''
    stop = slice_instance.stop or ''
    step = slice_instance.step or ''

    msg = '{}:'.format(start)
    if stop:
        msg += '{}'.format(stop)
        if step:
            msg += ':'
    if step:
        msg += '{}'.format(step)
    return msg


class DaskGroup(AllThreadedEngine.Group):
    def __init__(self, arrays):
        self.arrays = arrays
        self._node_cache = {}

    @staticmethod
    def biggus_chunk(chunk_key, biggus_array, masked):
        """
        A function that lazily evaluates a biggus.Chunk. This is useful for passing through
        as a dask task so that we don't have to compute the chunk in order to compute the graph.

        """
        if masked:
            array = biggus_array.masked_array()
        else:
            array = biggus_array.ndarray()

        return biggus._init.Chunk(chunk_key, array)

    @staticmethod
    def wibble(nicename):
        def produce_chunks(produced_keys, handler, n_sources, *all_chunks):
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
            all_chunks = list(grouper(all_chunks, n_sources))
            process_result = None
            for chunks in all_chunks:
                process_result = handler.process_chunks(chunks)
            result = handler.finalise()
            if result is None:
                if process_result is None:
                    raise RuntimeError('No result to process. Please raise an issue on the biggus '
                                       'issue tracker with the calculation that was performed.')
                # Itself returns a chunk.
                return process_result
            else:
                return result
        produce_chunks.__name__ = nicename
        return produce_chunks

    def _make_stream_handler_nodes(self, dsk_graph, array, iteration_order, masked):
        """
        Produce task graph entries for an array that comes from a biggus StreamsHandler.
        This is essentially every type of array that isn't already a thing on
        disk/in-memory. StreamsHandler arrays include all aggregations and
        elementwise operations.

        """
        nodes = {}
        handler = array.streams_handler(masked)
        input_iteration_order = handler.input_iteration_order(iteration_order)

        def input_keys_transform(input_array, keys):
            identity_transformer = lambda keys: keys
            input_transformer = identity_transformer
            if hasattr(input_array, 'streams_handler'):
                handler = input_array.streams_handler(masked)
                # Get the transformer of the input array, and apply it to the keys.
                input_transformer = getattr(handler,
                                            'output_keys', None)
                if input_transformer is None:
                    print('no transform')
                else:
                    orig = keys
                    keys = input_transformer(keys)
                    print('transforming:', orig, keys, getattr(handler, 'axis', None))
            else:
                print('not a hander', type(input_array))
#             this_transformer = getattr(handler, 'output_keys', identity_transformer)
#             transformer = lambda keys: this_transformer(input_transformer(keys))
            
            return keys

        sources_keys = []
        sources_chunks = []
        all_keys = set()
        for input_array in array.sources:
            # Bring together all chunks that influence the same part of this (resultant) array.
#             source_chunks_by_key = collections.defaultdict(list)
            source_chunks_by_key = {}
            sources_chunks.append(source_chunks_by_key)
            source_keys = []
            sources_keys.append(source_keys)

            # Make nodes for the source arrays (if they don't already exist) before
            # we do anything else.
            input_nodes = self._make_nodes(dsk_graph, input_array, input_iteration_order,
                                           masked)

            for chunk_id, task in input_nodes.items():
                chunk_keys = task[1]
                source_keys.append(chunk_keys)
                # Define a function that can take a keys specification and turn it into
                # the keys that the resultant array would write into.
#                 target_key_transformer = getattr(handler, 'output_keys', lambda key: key)
                this_key = str(input_keys_transform(input_array, chunk_keys))
#                 this_key = str(chunk_keys)
                all_keys.add(this_key)
                source_chunks_by_key.setdefault(this_key, []).append([chunk_id, task])

#         import key_grouper
#         sources_keys_grouped = key_grouper.group_keys(array.shape, source_keys)





        for key in all_keys:
            # For all source, pick out all tasks that match the current key.
            all_chunks = [[key_id for key_id, chunk in chunks_by_key.get(key, [])]
                          for chunks_by_key in sources_chunks]
            all_chunks_real = [[chunk for key_id, chunk in chunks_by_key.get(key, [])]
                          for chunks_by_key in sources_chunks]

            # TODO: Assert all chunks same length?
            nchunks = [len(chunks) for chunks in all_chunks]
            if not all([chunks == nchunks[0] for chunks in nchunks]):
                print(array.sources)
                print(array)
                print(all_chunks)
                from pprint import pprint
                for source in sources_chunks:
                    pprint(source)
                print(handler)
                raise ValueError('Not all sources have the same number of chunks...')

            # Pivot the chunks so that they come in groups, one per source.
            pivoted = zip(*all_chunks)
            print(all_chunks)
            handler = array.streams_handler(masked)
            name = getattr(handler, 'nice_name', handler.__class__.__name__)

            if hasattr(handler, 'axis'):
                name += '\n(axis={})'.format(handler.axis)
            # For ElementwiseStreams handlers, use the function that they wrap (e.g "add")
            if hasattr(handler, 'operator'):
                name = handler.operator.__name__

            produce_chunks = self.wibble(name)

            # Flatten out the pivot so that dask can dereferences the IDs
            task0 = [item for sublist in all_chunks_real for item in sublist][0]
            keys = task0[1]

#             from pprint import pprint
#             pprint(dsk_graph)
#             dask.dot.dot_graph(dsk_graph)

            if False:
                shape = []
                subset = ''
            else:
                t_keys = keys
                print(', '.join(map(slice_repr, keys)), 
                      ', '.join(map(slice_repr, t_keys)),
                      array.sources[0].shape, array.shape)

                shape = array.sources[0][keys].shape
                if all(key == slice(None) for key in t_keys):
                    subset = ''
                else:
                    pretty_index = ', '.join(map(slice_repr, t_keys))
                    subset = 'subset from [{}]\n'.format(pretty_index)

            source_chunks = [item for sublist in pivoted for item in sublist]

            task = tuple([produce_chunks, t_keys, handler, len(array.sources)] + source_chunks)
            chunk_id = 'array ({})\n\n{}(id: {})'.format(', '.join(map(str, shape)),
                                                         subset, uuid4())

            if chunk_id in dsk_graph:
                raise ValueError('what?')
            dsk_graph[chunk_id] = task
            nodes[chunk_id] = task
        return nodes

    def _make_nodes(self, dsk_graph, array, iteration_order, masked, top=False):
        """
        Recursive function that returns the dask items for the given array.

        NOTE: Currently assuming that all tasks are a tuple, with the second item
        being the keys used to index the source of the respective input array.

        """
        cache_key = array_id(array, iteration_order, masked)
        # By the end of this function Nodes will be a dictionary with one item
        # per chunk to be processed for this array (uuid: the-dask-computation).
        nodes = self._node_cache.get(cache_key, None)

        if nodes is None:
            if hasattr(array, 'streams_handler'):
                nodes = self._make_stream_handler_nodes(dsk_graph, array, iteration_order, masked)
            else:
                nodes = {}
                node = biggus._init.ProducerNode(array, iteration_order[::-1], masked)
                chunks = []
                def foo(name):
                    def biggus_chunk(chunk_key, biggus_array, masked):
                        """
                        A function that lazily evaluates a biggus.Chunk. This is useful for passing through
                        as a dask task so that we don't have to compute the chunk in order to compute the graph.
                
                        """
                        if masked:
                            array = biggus_array.masked_array()
                        else:
                            array = biggus_array.ndarray()

                        return biggus._init.Chunk(chunk_key, array)
                    biggus_chunk.__name__ = name
                    return biggus_chunk

                biggus_chunk = copy.deepcopy(self.biggus_chunk)
                # TODO: Don't this this naming is working - it is the same for all producer nodes.
                biggus_chunk.__name__ = '{}\n{}'.format(array.__class__.__name__, array.shape)

                biggus_chunk = foo(biggus_chunk.__name__)

                for chunk_key in node.chunk_index_gen():
                    biggus_array = node.array[chunk_key]
#                     chunk_id = array_id(biggus_array, iteration_order, masked)
                    chunk_id = uuid4()
                    task = (biggus_chunk, chunk_key, biggus_array, masked)
                    chunks.append(task)
                    if chunk_id in dsk_graph:
                        raise ValueError('what?')
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
            array_id_val = array_id(array, masked=masked)
            dependencies = list(self._node_cache[array_id_val].keys())
            def foo(array):
                def collect_array(all_chunks):
                    # We make the NdarrayNode inside the calling function as it is this that
                    # ultimately
                    result_node = biggus._init.NdarrayNode(array, masked)
                    for chunks in all_chunks:
                        # TODO: Factor it so that this isn't necessary...
                        if isinstance(chunks, biggus._init.Chunk):
                            chunks = [chunks]
                        result_node.process_chunks(chunks)
                    return result_node.result
                return collect_array

            if array_id_val not in dsk_graph:
                dsk_graph[array_id_val] = (foo(array), dependencies)

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
#     biggus._init.engine = e
    # TODO: Make this smaller and have the tests pass!
    biggus._init.MAX_CHUNK_SIZE = 4 * 8# * 1024 * 1000
    
    if True:
        shape = (6,)
        a_n = np.random.random(shape)
        a = biggus.zeros(shape)
        b_n = np.zeros([shape[0], shape[0]])
        b = biggus.zeros([shape[0], shape[0]])

        c = biggus.mean(b, axis=1)
        d = a - c

#         r = d.ndarray()
        expected = (a_n - b_n.mean(axis=1))
#         np.testing.assert_array_equal(r, expected)

        graph = e.graph(d)
        print(d.ndarray())
        print(biggus._init.ndarrays(d))
        exit(0)
        pprint(graph)
        dask.dot.dot_graph(graph)
    exit(0)
    print('-' * 80)
    
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
        a = biggus.ConstantArray((2, 5))
        expr = (a/3.) + a - (a * 2)
        
        graph = e.graph(expr)

        dask.dot.dot_graph(graph)
        pprint(graph)
        print(e.ndarrays(add)[0].shape)
    
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
    
    if True:
        
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


