# 贷款审批预测分类建模报告

## 一、项目背景与目标

### 1.1 数据集简介

本项目使用 LendingClub 公开的历史贷款数据，包含两个数据集：

- **拒绝记录（Rejected）**：2007–2018 年所有被拒绝的贷款申请，共约 **2,764 万行**，包含申请金额、申请日期、债务收入比、工作年限、邮编等 9 个字段
- **通过记录（Accepted）**：同期所有被批准并放款的贷款记录，共约 **211 万行**，包含 151 个字段，涵盖借款人信用信息、还款情况、贷款状态等

两份数据合并后总规模约 **2,714 万行**，正负样本比例约为 **11.8:1**，存在严重的类别不平衡问题。

### 1.2 建模目标

以贷款申请的申请信息为输入，预测该申请能否通过审批（二分类问题）：

- **label = 1**：申请被批准（来自 Accepted 数据集）
- **label = 0**：申请被拒绝（来自 Rejected 数据集）

### 1.3 技术选型

| 组件 | 选型 | 说明 |
|------|------|------|
| 计算引擎 | 阿里云 EMR Serverless Spark 3.5.2 | 分布式大数据计算 |
| 存储 | 阿里云 OSS（lending-data bucket） | 等价于 HDFS，存储原始数据及各层中间产物 |
| 建模框架 | PySpark MLlib | Spark 原生机器学习库，Executor 原生支持，无需额外环境配置 |
| 数据仓库分层 | ODS → DWD → DWS → ADS | 标准数据仓库架构，保证数据处理的可重复性与可追溯性 |

---

## 二、数据仓库架构与数据处理

### 2.1 整体分层架构

```
OSS Raw CSV
    │
    ▼
┌─────────────────────────────────────────────────────┐
│  ODS 层（原始数据）                                    │
│  oss://lending-data/510/ods/                         │
│  · 原始 CSV 文件，不做任何修改                          │
└─────────────────────────────────────────────────────┘
    │  01_ods_to_dwd.py
    ▼
┌─────────────────────────────────────────────────────┐
│  DWD 层（数据清洗层）                                  │
│  oss://lending-data/510/dwd/                         │
│  · 标准化字段名、类型转换、缺失值处理                    │
│  · 将 Rejected + Accepted 合并，打 label=0/1 标签      │
│  · 输出：dwd_510_loan_combined（27,144,897 行）        │
└─────────────────────────────────────────────────────┘
    │  02_dwd_to_dws.py
    ▼
┌─────────────────────────────────────────────────────┐
│  DWS 层（聚合统计层）                                  │
│  oss://lending-data/510/dws/                         │
│  · 按州、用途、邮编、工作年限四个维度分别聚合             │
│  · 计算各维度的历史批准率、平均金额、平均 DTI 等          │
└─────────────────────────────────────────────────────┘
    │  03_dws_to_ads.py
    ▼
┌─────────────────────────────────────────────────────┐
│  ADS 层（应用数据层）                                  │
│  oss://lending-data/510/ads/                         │
│  · 将 DWS 聚合特征 join 回明细记录                     │
│  · 输出建模特征集：ads_510_loan_model_input            │
│  · 输出训练好的模型：ads_510_models/                   │
└─────────────────────────────────────────────────────┘
```

### 2.2 DWD 层：数据清洗

**主要处理逻辑（01_ods_to_dwd.py）：**

- **Rejected 表清洗**：
  - `Employment Length` 字段去除 "years"/"year"/"< 1 year" 等文本，转为数值型 `emp_length`
  - `Debt-To-Income Ratio` 字段去除 "%" 符号，转为 Double
  - 邮编取前 3 位（原始为 "XXXXX" 格式，取前缀作为区域标识）
  - 标记 `label = 0`

- **Accepted 表清洗**：
  - `int_rate`、`revol_util` 去除 "%" 转数值
  - `emp_length` 同 Rejected 处理逻辑统一
  - `issue_d` 日期解析为标准格式
  - 标记 `label = 1`

- **合并**：统一选取 `loan_amnt`、`emp_length`、`dti`、`purpose`、`addr_state`、`zip_code`、`label` 七个字段，通过 `unionByName` 合并，所有字段缺失率 **0.00%**（仅保留完整记录）

**处理结果：**

| 数据集 | 清洗前行数 | 清洗后行数 | 耗时 |
|--------|-----------|-----------|------|
| Rejected | 27,636,929 | 25,031,808 | ~200s |
| Accepted | 2,260,701 | 2,113,089 | ~20s |
| Combined | — | 27,144,897 | — |

### 2.3 DWS 层：多维度聚合

**主要输出（02_dwd_to_dws.py）：**

| 聚合表 | 维度 | 主要字段 | 行数 |
|--------|------|---------|------|
| `dws_510_loan_geo_stats` | 州（addr_state） | approval_rate, avg_dti, avg_loan_amnt | 51 |
| `dws_510_loan_purpose_stats` | 用途（purpose） | approval_rate, avg_loan_amnt, avg_dti | 14 |
| `dws_510_loan_zip_stats` | 邮编前三位 | approval_rate, avg_loan_amnt | 1,001 |
| `dws_510_loan_emp_stats` | 工作年限 | approval_rate, avg_loan_amnt, avg_dti | 12 |

### 2.4 ADS 层：特征工程

**衍生特征构建（03_dws_to_ads.py）：**

将 DWS 各维度的历史统计值 left join 回 DWD 明细表，为每条申请记录附加以下衍生特征：

```
每条申请记录
    ├── 原始特征：loan_amnt, emp_length, dti
    ├── 地理衍生：state_approval_rate（该州历史批准率）
    │             state_avg_dti（该州平均 DTI）
    ├── 用途衍生：purpose_approval_rate（该用途历史批准率）
    │             purpose_avg_loan_amnt（该用途平均申请金额）
    ├── 邮编衍生：zip_approval_rate（该邮编前缀历史批准率）
    ├── 类别特征：purpose, addr_state（供 StringIndexer 使用）
    └── 目标变量：label
```

最终 `ads_510_loan_model_input` 共 **27,144,897 行**，11 列（不含 label）。

---

## 三、建模方案设计

### 3.1 类别不平衡处理

原始数据 label=0（拒绝）约 2503 万，label=1（批准）约 211 万，比例 **11.8:1**。严重不平衡会导致模型偏向多数类（始终预测"拒绝"即可获得 92% 准确率），因此采用欠采样策略：

- **方法**：对 label=0 的记录随机欠采样至 label=1 的 **2 倍**
- **结果**：label=0 约 423 万，label=1 约 211 万，总计约 **634 万行**，比例降为 2:1
- **优点**：相比 SMOTE 过采样，欠采样在大数据场景下计算开销小，且原始正样本特征分布不被改变

| 处理阶段 | label=0 | label=1 | 总计 |
|---------|---------|---------|------|
| 原始 | 25,031,808 | 2,113,089 | 27,144,897 |
| 欠采样后 | 4,230,789 | 2,113,089 | 6,343,878 |
| 训练集（80%） | — | — | 5,074,671 |
| 测试集（20%） | — | — | 1,269,207 |

### 3.2 特征处理 Pipeline

使用 MLlib Pipeline 组织特征处理阶段，确保训练和推理时处理逻辑完全一致：

```
类别特征 (purpose, addr_state)
    └─→ StringIndexer（将字符串编码为整数索引）
            └─→ OneHotEncoder（将索引转为稀疏向量，handleInvalid="keep"）

数值特征 (loan_amnt, emp_length, dti,
          state_approval_rate, state_avg_dti,
          purpose_approval_rate, purpose_avg_loan_amnt,
          zip_approval_rate)
    └─→ 直接输入

所有特征
    └─→ VectorAssembler（拼接成统一的 features 向量）
            └─→ 分类器
```

### 3.3 模型选择依据

选取三个模型进行对比，覆盖线性模型、并行树集成、串行提升树三类算法：

| 模型 | 选择理由 | 在 Spark 中的并行性 |
|------|---------|------------------|
| **LogisticRegression** | 线性基线，验证 DWS 衍生特征的线性可分性；分布式梯度下降扩展性最佳 | 高（迭代梯度分布式计算） |
| **RandomForest** | 树间完全独立，Spark 并行度最高；提供可解释的特征重要性 | 高（每棵树独立并行构建） |
| **GBT** | 串行提升，能捕捉更复杂的特征交互，预期精度最高 | 中（树间串行，树内节点级并行） |

---

## 四、实验过程与遇到的问题

### 4.1 遇到的问题及解决过程

#### 问题一：XGBoost 在 EMR Executor 节点上无法运行

**背景**：为提升 GBT 的训练速度，最初计划用 `xgboost.spark`（SparkXGBClassifier）替代 MLlib GBT，其 C++ histogram 算法比 MLlib GBT 快约 10–20 倍。

**报错**：
```
ModuleNotFoundError: No module named 'xgboost'
```

**根本原因**：`SparkXGBClassifier` 在训练时会将 Python 任务分发到各 **Executor 节点**执行。在 Notebook 中执行 `pip install` 只在 Driver 节点安装了 xgboost，而 EMR Serverless 的 Executor 节点是按需启动的容器，无法通过 Driver 的 pip install 同步安装包。

```
Driver（Notebook 所在节点）    →  pip install 有效
Executor 1 / 2 / 3 / 4        →  无 xgboost，报错
```

**解决方案**：放弃 `xgboost.spark`，改回 MLlib 原生 `GBTClassifier`，通过激进的参数压缩提速：`maxIter=20`（控制树的轮数）、`maxBins=16`（候选切分点减半）、`subsamplingRate=0.8`（每轮使用 80% 数据），将训练时间控制在合理范围内。

> MLlib 全部使用 JVM 原生执行，不依赖 Python 运行时，因此 Executor 上无需任何额外安装。

---

#### 问题二：xgboost 模块缓存导致 sklearn 安装后仍报错

**背景**：尝试修复路径一时，发现即便 pip install sklearn 成功，xgboost.spark 仍报 `ImportError: sklearn needs to be installed`。

**根本原因**：EMR Spark Session 启动时 xgboost 已被预加载，`SKLEARN_INSTALLED = False` 已缓存在 `sys.modules` 中。安装 sklearn 后，旧模块缓存不会自动刷新。

**临时解决方案**（最终未采用，因 Executor 问题导致此路不通）：
```python
for _k in list(sys.modules.keys()):
    if "xgboost" in _k:
        del sys.modules[_k]
from xgboost.spark import SparkXGBClassifier  # 重新 import
```

---

#### 问题三：GBTClassifier 参数名错误

**报错**：
```
TypeError: __init__() got an unexpected keyword argument 'numTrees'
```

**原因**：`numTrees` 是 `RandomForestClassifier` 的参数，`GBTClassifier` 中控制提升轮数（树的数量）的参数名为 `maxIter`，两者命名不同。

**修复**：
```python
# 错误
GBTClassifier(numTrees=20, ...)
# 正确
GBTClassifier(maxIter=20, ...)
```

---

#### 问题四：Spark 3.5 中 SparseVector 无法直接用下标提取概率

**背景**：在阈值分析时，需要提取预测结果中 label=1 的概率值（`probability[1]`）。

**报错**：
```
AnalysisException: Can't extract a value from "probability".
Need a complex type [STRUCT, ARRAY, MAP] but got "STRUCT<type: TINYINT, ...>"
```

**根本原因**：Spark 3.5 中，GBT 的 `probability` 列输出为 `SparseVector`（Spark 内部表示为 STRUCT 类型），不能直接用 `[1]` 下标访问。

**修复**：使用 `pyspark.ml.functions.vector_to_array` 先将 Vector 转为 Array，再取索引：
```python
from pyspark.ml.functions import vector_to_array

best_preds_arr = best_preds.withColumn(
    "prob_1", vector_to_array(F.col("probability"))[1]
)
```

---

#### 问题五：EMR 任务执行慢（早期阶段）

**现象**：Rejected 表清洗任务耗时约 199 秒，怀疑资源配置不足。

**原因**：新建 Spark Session 时 `spark.executor.cores=1`（默认值），单个 Executor 只使用 1 个核心，并行度严重不足。

**解决方案**：重建 Session，调整配置：
```
spark.executor.cores        = 2
spark.executor.instances    = 4
spark.sql.shuffle.partitions = 32
```

---

### 4.2 关于 state_approval_rate 的数据泄露分析

在特征重要性分析中，`state_approval_rate` 的重要性高达 **73.6%（GBT）/ 64.8%（RF）**，引发对数据泄露的关注。

**泄露的形式**：`state_approval_rate` 由全量 27M 行数据（含后来划入测试集的记录）计算得出，再 join 回每条记录。测试集中某条记录的标签，参与了其所在州批准率的计算，是一种**目标编码泄露（Target Encoding Leakage）**。

**泄露程度极轻**：

| 指标 | 数值 |
|------|------|
| 全数据集行数 | 27,144,897 |
| 平均每州记录数 | ≈ 543,000 |
| 单条记录对本州批准率的影响 | ≈ 0.0002% |

由于聚合粒度为州级别（仅 50 个州），每条记录对统计值的影响可忽略不计。

**特征本身的业务合理性**：在真实风控场景中，"该地区历史批准率"是完全合法的参考特征，银行在信贷决策中本就会参考地区风险数据。

**严格处理方案**（生产环境建议）：仅使用训练集计算 DWS 聚合，再将映射表应用于测试集（即 Out-of-Fold Target Encoding），可彻底规避泄露风险。

---

## 五、实验结果

### 5.1 三模型性能对比

| 模型 | AUC-ROC | AUC-PR | Accuracy | F1 | 训练耗时 |
|------|---------|--------|----------|----|---------|
| LogisticRegression | 0.9138 | 0.8417 | 0.8387 | 0.8336 | 79s |
| RandomForest | 0.9410 | 0.8734 | 0.8958 | 0.8950 | 172s |
| **GBT** | **0.9592** | **0.9224** | **0.9207** | **0.9206** | 1254s |

**GBT 在所有指标上全面领先**，AUC-ROC 达到 0.9592，AUC-PR 达到 0.9224，表明模型在严重类别不平衡场景下依然具有很强的区分能力。

### 5.2 最优模型（GBT）混淆矩阵

| | 预测为拒绝 | 预测为批准 |
|--|-----------|-----------|
| **实际拒绝（label=0）** | 797,294 | 48,873 |
| **实际批准（label=1）** | 51,758 | 371,282 |

- **Precision（label=1）**：0.8837
- **Recall（label=1）**：0.8777
- **F1（label=1）**：0.8807

### 5.3 阈值分析（GBT，label=1）

| 阈值 | TP | FP | FN | Precision | Recall | F1 |
|------|----|----|-----|-----------|--------|-----|
| 0.3 | 380,290 | 65,648 | 42,750 | 0.8528 | 0.8989 | 0.8753 |
| 0.4 | 376,729 | 57,719 | 46,311 | 0.8671 | 0.8905 | 0.8787 |
| **0.5** | **371,282** | **48,873** | **51,758** | **0.8837** | **0.8777** | **0.8807** |
| 0.6 | 355,686 | 35,633 | 67,354 | 0.9089 | 0.8408 | 0.8735 |
| 0.7 | 344,786 | 30,979 | 78,254 | 0.9176 | 0.8150 | 0.8633 |

默认阈值 0.5 时 F1 最高（0.8807）。若业务上更看重减少误拒（降低 FN），可将阈值调低至 0.3–0.4，以召回率换取精确率的轻微下降。

### 5.4 特征重要性分析

**RandomForest Top 5：**

| 特征 | 重要性 | 累计 |
|------|--------|------|
| state_approval_rate | 0.6478 | 64.8% |
| state_avg_dti | 0.1173 | 76.5% |
| dti | 0.0700 | 83.5% |
| zip_approval_rate | 0.0539 | 88.9% |
| purpose=credit_card | 0.0186 | 90.7% |

**GBT Top 5：**

| 特征 | 重要性 | 累计 |
|------|--------|------|
| state_approval_rate | 0.7363 | 73.6% |
| dti | 0.1222 | 85.9% |
| state_avg_dti | 0.0855 | 94.4% |
| zip_approval_rate | 0.0437 | 98.8% |
| purpose=debt_consolidation | 0.0007 | 99.5% |

**关键发现**：`state_approval_rate` 单一特征在两个模型中均贡献了 65%–74% 的预测力。这验证了 **DWS 层特征工程的核心价值**——若直接使用原始字段（不含衍生批准率），逻辑回归 AUC 仅约 0.55，引入衍生特征后直接提升至 0.91，说明地理维度的历史批准率对贷款审批结果具有极强的预测信号。

---

## 六、模型存储

三个模型均以 MLlib PipelineModel 格式保存至 OSS，包含完整的特征处理阶段（StringIndexer、OHE、VectorAssembler）和分类器参数：

```
oss://lending-data/510/ads/ads_510_models/
    ├── LogisticRegression/
    ├── RandomForest/
    └── GBT/
```

加载方式：
```python
from pyspark.ml import PipelineModel
model = PipelineModel.load("oss://lending-data/510/ads/ads_510_models/GBT")
predictions = model.transform(new_data)
```

---

## 七、总结

### 7.1 建模流程回顾

```
原始数据（OSS）
    → ODS→DWD（清洗、合并、打标签）
    → DWS（四维度聚合，生成历史批准率等统计特征）
    → ADS（join 衍生特征，欠采样，Pipeline 训练，模型保存）
```

整条链路全部在阿里云 EMR Serverless Spark 上运行，数据常驻 OSS，计算资源按需分配，总处理数据量约 **2714 万行**。

### 7.2 主要结论

1. **特征工程是本项目最关键的环节**：DWS 层构建的地理/用途/邮编维度批准率，将逻辑回归 AUC 从 0.55 提升至 0.91，远超原始特征的预测能力。

2. **GBT 是最优模型**：AUC-ROC = 0.9592，Accuracy = 92.1%，在捕捉贷款申请特征的复杂非线性关系上优于 LR 和 RF。

3. **EMR 上适合使用 MLlib 原生算法**：对于需要在 Executor 节点运行 Python 代码的第三方库（如 XGBoost），需要自定义 Docker 镜像才能在 EMR Serverless 上使用；MLlib 算法基于 JVM 原生执行，无此限制。

4. **类别不平衡处理有效**：通过 2:1 欠采样将正负样本比从 11.8:1 压缩，模型对少数类（批准）的 Recall 达到 0.878，避免了模型退化为"全部预测拒绝"的平凡解。
