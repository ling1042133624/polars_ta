"""
Thisis how polars implements calling third-party packages
以下是polars提供的实现调用第三方库的方案
expr.ta.func

"""
import talib
import pandas as pd
import polars as pl

from polars_ta.performance.drawdown import ts_max_drawdown
from polars_ta.utils.helper import TaLibHelper

_ = TaLibHelper

# df = pl.DataFrame(
#     {
#         "A": [5, None, 3, 2, 1],
#         "B": [5, 4, None, 2, 1],
#         "C": [5, 4, 3, 2, 1],
#     }
# )
df =pl.read_csv("2019-09-26.csv")
# talib.AROON()
df = df.with_columns([
    # single input single ouput, no need to handle null/nan values
    # 一输入一输出，不需处理空值
    pl.col('close').ta.COS().alias('COS'),
    # single input, multi output
    # 一输入多输出
    pl.col('close').ta.BBANDS(timeperiod=2, skip_nan=True, schema=['upperband', 'middleband', 'lowerband']).alias('BBANDS'),
    # multi input, single output
    # 多输入一输出
    pl.struct(['high', 'low', 'close']).ta.ATR(timeperiod=2, skip_nan=True).alias('ATR'),
    # multi input, multi output
    # 多输入多输出
    pl.struct(['high', 'low']).ta.AROON(timeperiod=2, skip_nan=True, schema=('aroondown', 'aroonup'), schema_format='XX_{}_YY').alias('AROON'),
    # multi input, single output
    # 多输入一输出
    pl.struct(['high', 'low']).ta.AROON(timeperiod=2, skip_nan=True, output_idx=1).alias('aroonup1'),
    # call third-party packages
    # 调用另一库
    pl.col('close').bn.move_rank(window=2, skip_nan=False).alias('move_rank'),
    ts_max_drawdown(pl.col('close')).alias('max_drawdown'),
])
# 设置 Polars 显示所有行
pl.Config.set_tbl_rows(df.shape[0])
pl.Config.set_tbl_cols(df.shape[1])
print(df)

df = df.unnest('BBANDS', 'AROON')
# 设置 Polars 显示所有行
pl.Config.set_tbl_rows(df.shape[0])
pl.Config.set_tbl_cols(df.shape[1])
df.write_excel('result.xlsx')
print(df)

pd_df = df.to_pandas()

print(pd_df)