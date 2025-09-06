# 股票分析系统 v2.0

## 概述

这是一个重构后的模块化股票分析框架，提供了灵活的策略组合、评分系统和输出管理功能。

## 架构设计

```
analyze/
├── core/                    # 核心业务层
│   ├── stock_analyzer.py    # 主分析器（协调器）
│   └── scoring_engine.py    # 评分引擎
├── data/                    # 数据访问层
│   ├── stock_data_loader.py # 数据加载器
│   └── stock_repository.py  # 数据仓库
├── strategies/              # 分析策略层
│   ├── base_strategy.py     # 策略基类
│   ├── j_value_strategy.py  # J值筛选策略
│   ├── volume_pattern_strategy.py # 量价关系策略
│   └── strategy_registry.py # 策略注册器
├── output/                  # 输出展示层
│   ├── formatters.py        # 结果格式化器
│   ├── exporters.py         # 文件导出器
│   └── reporters.py         # 报告生成器
└── utils/                   # 工具层
    ├── config.py            # 配置管理
    └── compatibility.py     # 向后兼容层
```

## 主要特性

### 1. 模块化设计
- **职责分离**：数据、策略、评分、输出各层独立
- **可扩展性**：轻松添加新的分析策略
- **灵活组合**：可以任意组合不同策略

### 2. 评分系统
- **多策略评分**：支持多种策略的综合评分
- **权重配置**：可以为不同策略设置权重
- **多种评分方法**：加权平均、乘积法、最高分法

### 3. 输出管理
- **多种格式**：控制台、CSV、Excel输出
- **专项报告**：J值报告、量价关系报告、综合报告
- **自定义格式化**：可以自定义输出格式

### 4. 向后兼容
- **保持原有接口**：现有代码无需修改
- **逐步迁移**：可以渐进式升级到新架构

## 快速开始

### 1. 基本使用

```python
from analyze import StockAnalyzer, AnalysisReporter

# 创建分析器（自动加载默认策略）
analyzer = StockAnalyzer()

# 执行分析
results = analyzer.analyze_stocks()

# 生成报告
reporter = AnalysisReporter()
reporter.generate_full_report(results)
```

### 2. J值分析

```python
from analyze import StockAnalyzer, AnalysisReporter

analyzer = StockAnalyzer()

# 分析J值小于13的股票
results = analyzer.analyze_j_under_value(max_j_value=13.0)

# 生成专项报告
reporter = AnalysisReporter()
reporter.generate_j_value_report(results, max_j_value=13.0)
```

### 3. 组合策略分析

```python
from analyze import StockAnalyzer, AnalysisReporter

analyzer = StockAnalyzer()

# 分析同时满足J值和量价关系的股票
results = analyzer.analyze_j_with_volume_pattern(
    max_j_value=13.0, 
    days_to_analyze=20
)

# 生成综合报告
reporter = AnalysisReporter()
reporter.generate_combined_report(results)
```

### 4. 自定义策略

```python
from analyze import BaseStrategy, StrategyResult, StockAnalyzer
import pandas as pd

class RSIStrategy(BaseStrategy):
    def __init__(self, rsi_threshold: float = 30.0, **kwargs):
        super().__init__(**kwargs)
        self.rsi_threshold = rsi_threshold
    
    def get_default_name(self) -> str:
        return "RSI超卖策略"
    
    def analyze(self, stock_code: str, stock_name: str, 
               stock_data: pd.DataFrame) -> StrategyResult:
        # 实现RSI分析逻辑
        # ...
        pass

# 使用自定义策略
analyzer = StockAnalyzer()
rsi_strategy = RSIStrategy(rsi_threshold=30.0, weight=0.8)
analyzer.add_strategy(rsi_strategy)

results = analyzer.analyze_stocks()
```

### 5. 高级配置

```python
from analyze import StockAnalyzer, AnalysisReporter, ExcelExporter

analyzer = StockAnalyzer()

# 配置策略权重
analyzer.set_strategy_weight("J值筛选策略", 0.6)
analyzer.set_strategy_weight("量价关系策略", 0.4)

# 配置评分方法
analyzer.set_scoring_method("weighted_average")

# 分析指定股票
results = analyzer.analyze_stocks(
    stock_codes=["000001.SZ", "000002.SZ"]
)

# 使用Excel导出
reporter = AnalysisReporter(exporter=ExcelExporter())
reporter.generate_full_report(results, "advanced_analysis")
```

## 向后兼容

原有的代码可以继续使用，无需修改：

```python
# 原有接口继续可用
from analyze import analyze_j_under_13, analyze_j13_volume_pattern

# 或者使用兼容性分析器
from analyze import LegacyStockAnalyzer

analyzer = LegacyStockAnalyzer()
j_results = analyzer.get_j_under_value_stocks(max_j_value=13.0)
```

## 策略说明

### J值筛选策略
- **目的**：筛选J值小于指定阈值的股票，识别超卖机会
- **参数**：
  - `max_j_value`: J值上限阈值（默认13.0）
- **评分逻辑**：J值越低，得分越高

### 量价关系策略
- **目的**：寻找放量上涨、缩量下跌的股票
- **参数**：
  - `days_to_analyze`: 分析天数（默认20天）
  - `min_price_change`: 最小价格变化阈值（默认1%）
  - `min_volume_contrast`: 最小量比对比度（默认1.2）
- **评分逻辑**：量价关系越明显，得分越高

## 输出格式

### 控制台输出
- 分析总结
- 股票排名表格
- 策略详细结果

### CSV/Excel导出
- 综合排名数据
- 各策略得分详情
- 策略表现统计
- 分析参数记录

## 配置选项

可以通过Config类管理配置：

```python
from analyze import Config

config = Config()

# 修改配置
config.set('analysis.j_value_threshold', 15.0)
config.set('scoring.method', 'multiplicative')

# 获取配置
threshold = config.get('analysis.j_value_threshold')
```

## 扩展指南

### 添加新策略
1. 继承`BaseStrategy`类
2. 实现`analyze`和`get_default_name`方法
3. 注册到`StrategyRegistry`

### 自定义输出格式
1. 继承`BaseFormatter`类
2. 实现格式化方法
3. 配置到`AnalysisReporter`

### 新增导出器
1. 继承`BaseExporter`类
2. 实现导出方法
3. 配置到`AnalysisReporter`

## 性能优化

- 数据缓存：`StockRepository`提供数据缓存
- 批量处理：支持批量股票分析
- 并行策略：策略分析可以并行执行
- 增量更新：支持增量数据更新

## 故障排除

### 常见问题

1. **导入错误**：确保Python路径正确
2. **数据文件不存在**：检查数据目录和文件路径
3. **策略分析失败**：检查数据格式和必要列

### 调试模式

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# 启用详细输出
analyzer = StockAnalyzer()
results = analyzer.analyze_stocks()
```

## 版本信息

- **当前版本**：v2.0.0
- **兼容性**：向后兼容v1.x接口
- **Python要求**：Python 3.7+
- **依赖**：pandas, numpy, pathlib

## 更新日志

### v2.0.0
- 完全重构为模块化架构
- 新增评分系统
- 新增策略注册机制
- 新增多种输出格式
- 保持向后兼容性

---

更多详细信息和示例，请参考 `example_usage.py` 文件。
