from xarray.namedarray.parallelcompat import ChunkManagerEntrypoint
from typing import Any, Callable, Sequence, List, Iterable
from xarray.namedarray._typing import (
    T_Chunks,
    _NormalizedChunks,
    _DType_co,
)

from arkouda.array_api import Array as ArkoudaArray


class ArkoudaManager(ChunkManagerEntrypoint["ArkoudaArray"]):  # type: ignore[type-var]
    array_cls: type[ArkoudaArray]

    def __init__(self) -> None:
        from arkouda.array_api import Array

        self.array_cls = Array

    def is_chunked_array(self, data: Any) -> bool:
        return isinstance(data, self.array_cls)

    def chunks(self, data: Any) -> _NormalizedChunks:
        block_starts = data.chunk_info()
        full_block_starts = [block_starts[i] + [data.shape[i]] for i in range(data.ndim)]

        return tuple([tuple(c) for c in self._chunk_outer_prod(full_block_starts)])

    def normalize_chunks(
        self,
        chunks: _NormalizedChunks | T_Chunks,
        shape: tuple[int, ...] | None = None,
        limit: int | None = None,
        dtype: _DType_co | None = None,
        previous_chunks: _NormalizedChunks | None = None,
    ) -> _NormalizedChunks:
        return chunks

    def from_array(self, data: Any, chunks: T_Chunks | _NormalizedChunks, **kwargs: Any) -> ArkoudaArray | Any:
        import arkouda.array_api as aa

        if isinstance(chunks, tuple) and len(chunks) > 1 and len(chunks) != data.ndim:
            raise ValueError("Number of dimensions in data and chunks must match, or chunks must be a single value")
        else:
            if isinstance(chunks, tuple) and len(chunks) == 1:
                chunk_sizes = chunks * data.ndim
            else:
                chunk_sizes = tuple(chunks)

        # # create array to store data
        # print("data dtype: ", data.dtype)
        # print("shape: ", data.shape)
        # print("first elem: ", data[0])
        # print("type of first elem: ", type(data[0]))

        # create array to store data
        arr = aa.zeros(data.shape, dtype=data.dtype)

        # copy data into array one chunk at a time
        n_chunks_per_dim = [data.shape[i] // chunk_sizes[i] for i in range(data.ndim)]
        chunk_starts = [[i * chunk_sizes[j] for i in range(n_chunks_per_dim[j])] for j in range(data.ndim)]
        chunk_starts = [cs + [data.shape[i]] for i, cs in enumerate(chunk_starts)]

        for chunk_indices in self._chunk_outer_prod(chunk_starts):
            indexer = tuple(chunk_indices)
            arr[indexer] = data[indexer]

        return arr

    @property
    def array_api(self) -> Any:
        import arkouda.array_api as aa

        return aa

    def compute(self, *data: Any, **kwargs: Any) -> tuple[ArkoudaArray, ...]:
        # arkouda uses eager evaluation, this is a no-op
        return tuple(data)

    def reduction(
        self,
        arr: Any,
        func: Callable[..., Any],
        combine_func: Callable[..., Any] | None = None,
        aggregate_func: Callable[..., Any] | None = None,
        axis: int | Sequence[int] | None = None,
        dtype: _DType_co | None = None,
        keepdims: bool = False,
    ) -> ArkoudaArray | Any:
        # from arkouda.array_api import asarray

        # return asarray(func(arr))

        raise NotImplementedError("Reduction not implemented")

    def scan(
        self,
        func: Callable[..., Any],
        binop: Callable[..., Any],
        ident: float,
        arr: Any,
        axis: int | None = None,
        dtype: _DType_co | None = None,
        **kwargs: Any,
    ) -> ArkoudaArray | Any:
        # from arkouda.array_api import asarray

        # return asarray(func(arr))

        raise NotImplementedError("Scan not implemented")

    def apply_gufunc(
        self,
        func: Callable[..., Any],
        signature: str,
        *args: Any,
        axes: Sequence[tuple[int, ...]] | None = None,
        axis: int | None = None,
        keepdims: bool = False,
        output_dtypes: Sequence[_DType_co] | None = None,
        output_sizes: dict[str, int] | None = None,
        vectorize: bool | None = None,
        **kwargs: Any,
    ) -> Any:
        # from arkouda.array_api import asarray

        # return asarray(func(arr))

        raise NotImplementedError("Apply gufunc not implemented")

    def map_blocks(
        self,
        func: Callable[..., Any],
        *args: Any,
        dtype: _DType_co | None = None,
        chunks: tuple[int, ...] | None = None,
        drop_axis: int | Sequence[int] | None = None,
        new_axis: int | Sequence[int] | None = None,
        **kwargs: Any,
    ) -> Any:
        # from arkouda.array_api import asarray

        # return asarray(func(arr))

        raise NotImplementedError("Map blocks not implemented")

    def blockwise(
        self,
        func: Callable[..., Any],
        out_ind: Iterable[Any],
        *args: Any,  # can't type this as mypy assumes args are all same type, but dask blockwise args alternate types
        adjust_chunks: dict[Any, Callable[..., Any]] | None = None,
        new_axes: dict[Any, int] | None = None,
        align_arrays: bool = True,
        **kwargs: Any,
    ) -> ArkoudaArray | Any:
        # from arkouda.array_api import asarray

        # return asarray(func(arr))

        raise NotImplementedError("Blockwise not implemented")

    def unify_chunks(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> tuple[dict[str, _NormalizedChunks], list[ArkoudaArray]]:
        chunkss = {}
        arrays = []

        for arg in args:
            if isinstance(arg, self.array_cls):
                arrays.append(arg)
            elif isinstance(arg, str):
                for index in arg.split(""):
                    chunkss[index] = (1,)

        return (chunkss, arrays)

    def store(
        self,
        sources: Any | Sequence[Any],
        targets: Any,
        **kwargs: Any,
    ) -> Any:
        _sources = list(sources)
        _targets = list(targets)

        if len(_sources) != len(_targets):
            raise ValueError("Number of sources and targets must match")

        for i, s in enumerate(sources):
            if isinstance(s, self.array_cls):
                # copy the source array to the target array one block at a time
                block_starts = s.chunk_info()
                full_block_starts = [block_starts[i] + [s.shape[i]] for i in range(s.ndim)]

                for chunk_indices in self._chunk_outer_prod(full_block_starts):
                    indexer = tuple(chunk_indices)

                    _targets[i][indexer] = s[indexer]
            else:
                raise ValueError("Only Arkouda arrays can be stored")

        if "return_stored" in kwargs and kwargs["return_stored"]:
            return targets

    @staticmethod
    def _chunk_outer_prod(index_starts: List[List[int]]) -> List[slice]:
        perms = [[]]
        for dim in index_starts:
            new_perms = []
            for p in perms:
                new_perms.append(p + [slice(dim[i], dim[i + 1], 1) for i in range(len(dim) - 1)])
            perms = new_perms
        return perms
