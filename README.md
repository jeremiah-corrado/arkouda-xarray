
# arkouda-xarray

Interface for using [arkouda](https://github.com/Bears-R-Us/arkouda) with [xarray](https://github.com/pydata/xarray).

(modeled after: https://github.com/cubed-dev/cubed-xarray)

## Dependencies

* Arkouda (see installation instructions: https://bears-r-us.github.io/arkouda/setup/install_menu.html)
* XArray

## Installation

```
conda install -c conda-forge arkouda-xarray
```

With this library installed, XArray's [entry point system](https://docs.xarray.dev/en/stable/internals/chunked-arrays.html#registering-a-new-chunkmanagerentrypoint-subclass) should automatically recognize that the Arkouda ChunkManager is available. Importing `arkouda-xarray` is not necessary.

## Usage

Xarray objects backed by Arkouda's Array-API compliant arrays can be created in 3 ways:

* passing the array(s) to the [`DataArray`](https://docs.xarray.dev/en/stable/generated/xarray.DataArray.html#xarray.DataArray) or [`DataSet`](https://docs.xarray.dev/en/stable/generated/xarray.Dataset.html) constructors
* calling [`chunk`](https://docs.xarray.dev/en/stable/generated/xarray.Dataset.chunk.html#xarray.Dataset.chunk) on an existing XArray object, passing `chunked_array_type='arkouda'`
* calling [`open_dataset`](https://docs.xarray.dev/en/stable/generated/xarray.open_dataset.html#xarray.open_dataset) with `chunked_array_type='arkouda'`
