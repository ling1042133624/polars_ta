# polars_ta

基于`polars`的算子库。实现量化投研中常用的技术指标、数据处理等函数。对于不易翻译成`Expr`的库（如：`TA-Lib`）也提供了函数式调用的封装

## 安装

> pip install -i https://pypi.org/simple --upgrade polars_ta # 官方源
> pip install -i https://pypi.tuna.tsinghua.edu.cn/simple --upgrade polars_ta # 国内镜像源

## 设计原则

1. 调用方法由`成员函数`换成`独立函数`
2. 输入数据使用`Expr`，少用`Series`
3. 函数命名优先向`WorldQuant`靠拢，然后向`TA-Lib`靠拢，最后才仿国内通达信等表达式
4. 计算速度优先，标准化优先。与其它系统结果不一致时需示明区别

## 指标区别

### EMA指标

1. EMA(CLOSE, 10)，`talib.set_compatibility(0)`，此为默认设置
    - 第一个有效值为`talib.SMA(CLOSE, 10)`
2. EMA(CLOSE, 10)，`talib.set_compatibility(1)`
    - 第一个有效值为`CLOSE`

由于`TA-Lib`的`兼容模式0`在`EMA`计算时逻辑发生了变动，用表达式实现起来复杂、计算效率低。
所以本库只实现`兼容模式1`。正好国内股票软件其实也只实现了`兼容模式1`。可以全量数据与`TA-Lib`进行单元测试比较

因`EMA`受影响的指标有`MACD, DEMA, TEMA, TRIX, T3`等。

### 中国版SMA(X, N, M)

本质上是国外的`RMA 兼容模式0`，即第一个有效值为移动平均，然后就是`alpha`区别

1. `SMA(X, N, M) = X.ema_mean(alpha=M/N)`
2. `RMA(X, N) = X.ema_mean(alpha=1/N) = SMA(X, N, 1)`
3. `EMA(x, N) = X.ema_mean(alpha=2/(N+1)) = X.ema_mean(span=N)`

换算关系可参考: [ewm_mean](https://pola-rs.github.io/polars/py-polars/html/reference/expressions/api/polars.Expr.ewm_mean.html#polars.Expr.ewm_mean)

遇到这种情况，本项目还是用`RMA 兼容模式1`来代替，数据误差由大到小，所以请预先提供一定长度的数据。后面一段的数据可以单元测试

受影响的的指标有`ATR, RSI`等

### 移动求和

`ADX`等一类的指标第一个有效值算法为`SUM`，而不是`SMA`，之后使用`ema_mean(alpha=1/N)`。此类暂不实现

## TA-Lib封装的演化

1. `Expr.map_batches`可以实现调用第三方库，如`TA-Lib, bottleneck`。但因为对输入与输出格式有要求，所以还需要用函数对第三方API封装一下。
    - 输入输出都只能是一列，如要支持多列需转换成`pl.Struct`。事后`pl.Struct`要拆分需使用`unnest`
    - 输出必须是`pl.Series`
2. 参数多，代码长。开始使用`register_expr_namespace`来简化代码
    - 实现代码[helper.py](polars_ta/utils/helper.py)
    - 使用演示[demo_ta1.py](examples/demo_ta1.py)
    - 优点：使用简单
    - 不足：`成员函数调用模式`不便于输入到遗传算法中进行因子挖掘
    - 不足：`__getattribute__`动态方法调用非常灵活，但失去了`IDE`智能提示
3. 前缀表达式。将所有的成员函数都转换成公式
    - 实现代码[wrapper.py](polars_ta/utils/wrapper.py)
    - 使用演示[demo_ta2.py](examples/demo_ta2.py)
    - 优点：可以输入到遗传算法
    - 不足：`__getattribute__`动态方法调用非常灵活，但失去了`IDE`智能提示
4. 代码自动生成。
    - 实现代码[codegen_talib.py](tools/codegen_talib.py)
    - 生成结果[\_\_init\_\_.py](polars_ta/talib/__init__.py)
    - 使用演示[demo_ta3.py](examples/demo_ta3.py)
    - 优点：即可以输入到遗传算法，`IDE`还有智能提示

## 参考

- https://github.com/pola-rs/polars
- https://github.com/TA-Lib/ta-lib
- https://github.com/twopirllc/pandas-ta
- https://github.com/bukosabino/ta

