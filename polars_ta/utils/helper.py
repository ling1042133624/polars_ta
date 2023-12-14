"""
以下是在github issues中 @cmdlineluser 提供的 注册命名空间 方案

https://github.com/pola-rs/polars/issues/9261

本人做了一定的调整。使用方法如下

expr.ta.func(..., skip_nan=False, output_idx=None, schema=None, schema_format='{}')

skip_nan: bool
    是否跳过空值。可以处理停牌无数据的问题，但会降低运行速度
output_idx: int
    多列输出时，选择只输出其中一列
schema: list or tuple
    返回为多列时，会组装成struct，可以提前设置子列的名字
schema_format: str
    为子列名指定格式

其它参数按**位置参数**和**命名参数**输入皆可
"""
import numpy as np
import polars as pl


def func_wrap_mn(func, cols, *args, skip_nan=False, output_idx=None, schema=None, schema_format='{}', **kwargs):
    """多输入多输出，兼容一输入一输出

    Parameters
    ----------
    func
    cols
    args
    skip_nan
    output_idx
    schema
    schema_format
    kwargs

    Returns
    -------
    pl.Series

    """
    if cols.dtype.base_type() == pl.Struct:
        # pl.struct(['A', 'B']).ta.AROON
        _cols = [cols.struct[field] for field in cols.dtype.to_schema()]
    else:
        # pl.col('A').ta.BBANDS
        _cols = [cols]

    if skip_nan:
        _cols = [c.cast(pl.Float64).to_numpy() for c in _cols]
        _cols = np.vstack(_cols)

        # move nan to head
        idx1 = (~np.isnan(_cols).any(axis=0)).argsort(kind='stable')
        # restore nan
        idx2 = idx1.argsort(kind='stable')

        _cols = [_cols[i, idx1] for i in range(len(_cols))]
        result = func(*_cols, *args, **kwargs)

        if isinstance(result, tuple):
            result = tuple([_[idx2] for _ in result])
        else:
            result = result[idx2]
    else:
        result = func(*_cols, *args, **kwargs)

    if isinstance(result, tuple):
        if output_idx is None:
            # pl.struct(['A').ta.BBANDS
            # you need alias outside, finally unnest
            if schema is None:
                schema = [f'column_{i}' for i in range(len(result))]

            schema = [schema_format.format(name) for name in schema]
            return pl.DataFrame(result, schema=schema).to_struct('')
        # output only one column
        if 0 <= output_idx < len(result):
            return pl.Series(result[output_idx])
    elif not isinstance(result, pl.Series):
        # pl.col('A').bn.move_rank
        return pl.Series(result)
    else:
        # pl.col('A').ta.COS
        return result


def func_wrap_11(func, cols, *args, skip_nan=False, output_idx=None, schema=None, schema_format='{}', **kwargs):
    """一输入，一输出。处理速度能更快一些"""
    _cols = cols

    if skip_nan:
        _cols = _cols.cast(pl.Float64).to_numpy()

        # move nan to head
        idx1 = (~np.isnan(_cols)).argsort(kind='stable')
        # restore nan
        idx2 = idx1.argsort(kind='stable')

        _cols = _cols[idx1]
        result = func(_cols, *args, **kwargs)

        result = result[idx2]
    else:
        result = func(_cols, *args, **kwargs)

    return pl.Series(result)


class FuncHelper:
    def __init__(self, expr: pl.Expr, lib=None, wrap=None) -> None:
        """初始化

        Parameters
        ----------
        expr
        lib:
            需要调用的第三方库
        wrap:
            对第三方库的封装方法

        """
        object.__setattr__(self, '_expr', expr)
        object.__setattr__(self, '_lib', lib)
        object.__setattr__(self, '_wrap', wrap)

    def __getattribute__(self, name: str):
        _expr: pl.Expr = object.__getattribute__(self, '_expr')
        _lib = object.__getattribute__(self, '_lib')
        _wrap = object.__getattribute__(self, '_wrap')
        _func = getattr(_lib, name)
        return (
            lambda *args, skip_nan=False, output_idx=None, schema=None, schema_format='{}', **kwargs:
            _expr.map_batches(
                lambda cols: _wrap(_func, cols, *args,
                                   skip_nan=skip_nan, output_idx=output_idx, schema=schema, schema_format=schema_format, **kwargs)
            )
        )


@pl.api.register_expr_namespace('ta')
class TaLibHelper(FuncHelper):
    def __init__(self, expr: pl.Expr) -> None:
        import talib as ta
        super().__init__(expr, ta, func_wrap_mn)


@pl.api.register_expr_namespace('bn')
class BottleneckHelper(FuncHelper):
    def __init__(self, expr: pl.Expr) -> None:
        import bottleneck as bn
        super().__init__(expr, bn, func_wrap_11)
