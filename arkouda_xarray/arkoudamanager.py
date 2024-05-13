from xarray.namedarray.parallelcompat import ChunkManagerEntrypoint
from xarray.namedarray.utils import module_available
from typing import Any, TYPE_CHECKING
import numpy as np

if TYPE_CHECKING:
    from xarray.namedarray._typing import (
        T_Chunks,
        _NormalizedChunks,
        _DType_co,
        duckarray,
    )

    try:
        from arkouda.array_api import Array as ArkoudaArray
    except ImportError:
        ArkoudaArray = np.ndarray[Any, Any]

arkouda_available = module_available("arkouda")


class ArkoudaManager(ChunkManagerEntrypoint["ArkoudaArray"]):  # type: ignore[type-var]
    array_cls: type[ArkoudaArray]
    available: bool = arkouda_available

    def __init__(self) -> None:
        from arkouda.array_api import Array
        self.array_cls = Array

    def is_chunked_array(self, data: duckarray[Any, Any]) -> bool:
        return isinstance(data, self.array_cls)

    def chunks(self, data: Any) -> _NormalizedChunks:
        return data.chunks  # type: ignore[no-any-return]

    def from_array(
        self, data: Any, chunks: T_Chunks | _NormalizedChunks, **kwargs: Any
    ) -> ArkoudaArray | Any:
        import arkouda.array_api as aa

        # Arkouda's chunking is opaque, ignore the chunks argument
        return aa.asarray(data)

    @property
    def array_api(self) -> Any:
        import arkouda.array_api as aa
        return aa

    def compute(
        self, *data: Any, **kwargs: Any
    ) -> tuple[ArkoudaArray, ...]:
        # arkouda uses eager evaluation, this is a no-op
        return tuple(data)

    def reduction(  # type: ignore[override]
        self,
        arr: T_ChunkedArray,
        func: Callable[..., Any],
        combine_func: Callable[..., Any] | None = None,
        aggregate_func: Callable[..., Any] | None = None,
        axis: int | Sequence[int] | None = None,
        dtype: _DType_co | None = None,
        keepdims: bool = False,
    ) -> DaskArray | Any:
        from dask.array import reduction

        return reduction(
            arr,
            chunk=func,
            combine=combine_func,
            aggregate=aggregate_func,
            axis=axis,
            dtype=dtype,
            keepdims=keepdims,
        )  # type: ignore[no-untyped-call]
