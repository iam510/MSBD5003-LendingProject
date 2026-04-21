"""
贷款审批预测 — 快速训练版（非 Spark）
适合 Kaggle / 本地运行，充分利用 GPU（XGBoost）

依赖：pip install xgboost lightgbm scikit-learn pandas
Kaggle 已预装以上全部库，无需额外安装。

【GPU 说明】
  XGBoost：开 GPU 比 CPU 快 5-20 倍，强烈建议开启
  LightGBM：本脚本用 CPU 版（GPU 版需重新编译，Kaggle 默认不支持）
  Logistic Regression：纯 CPU，GPU 无帮助
"""

import os, sys, time
import numpy as np
import pandas as pd
from sklearn.preprocessing import LabelEncoder
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (roc_auc_score, f1_score, precision_score,
                              recall_score, accuracy_score, confusion_matrix,
                              classification_report)
import xgboost as xgb
import lightgbm as lgb
import joblib
import warnings
warnings.filterwarnings("ignore")

# ─────────────────────────────────────────────────────────────
# 路径配置（自动识别 Kaggle / 本地）
# ─────────────────────────────────────────────────────────────
IS_KAGGLE = os.path.exists("/kaggle/input")

if IS_KAGGLE:
    DATA_CSV  = "/kaggle/input/MSBD5003-lending/dwd_loan_combined.csv.gz"
    MODEL_DIR = "/kaggle/working/models_fast"
else:
    ROOT_DIR  = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    DATA_CSV  = os.path.join(ROOT_DIR, "data", "export", "dwd_loan_combined.csv")
    MODEL_DIR = os.path.join(ROOT_DIR, "data", "models_fast")

os.makedirs(MODEL_DIR, exist_ok=True)

USE_GPU = IS_KAGGLE   # 本地没 GPU 则关闭，Kaggle 自动开启

SEP = "=" * 65
T0  = time.time()

print(SEP)
print("  Loan Approval Prediction — Fast Training (non-Spark)")
print(f"  Environment: {'Kaggle' if IS_KAGGLE else 'Local'}  |  GPU: {USE_GPU}")
print(SEP, flush=True)


# ══════════════════════════════════════════════════════════════
# 1. 读取数据
# ══════════════════════════════════════════════════════════════
print("\n[Step 1] Loading data ...", flush=True)
t = time.time()

df = pd.read_csv(DATA_CSV,
                 dtype={"purpose": "category", "addr_state": "category",
                        "zip_code": "category", "label": "int8"})

n0 = (df["label"] == 0).sum()
n1 = (df["label"] == 1).sum()
print(f"  Total rows : {len(df):,}")
print(f"  label=0 (rejected): {n0:,}   label=1 (accepted): {n1:,}")
print(f"  Imbalance  : {n0/n1:.1f}:1   Load time: {time.time()-t:.1f}s", flush=True)


# ══════════════════════════════════════════════════════════════
# 2. 特征编码
# ══════════════════════════════════════════════════════════════
print("\n[Step 2] Encoding features ...", flush=True)

CAT_COLS = ["purpose", "addr_state", "zip_code"]
NUM_COLS = ["loan_amnt", "emp_length", "dti"]

encoders = {}
for col in CAT_COLS:
    le = LabelEncoder()
    df[col] = le.fit_transform(df[col].astype(str))
    encoders[col] = le

df[NUM_COLS] = df[NUM_COLS].fillna(0)

FEATURES = CAT_COLS + NUM_COLS
X = df[FEATURES].values
y = df["label"].values
print(f"  Features: {FEATURES}  shape: {X.shape}", flush=True)


# ══════════════════════════════════════════════════════════════
# 3. 欠采样 + 划分
# ══════════════════════════════════════════════════════════════
print("\n[Step 3] Undersampling & splitting ...", flush=True)

# 欠采样：rejected 取 accepted 的 2 倍
idx0 = np.where(y == 0)[0]
idx1 = np.where(y == 1)[0]
rng  = np.random.default_rng(42)
idx0_sampled = rng.choice(idx0, size=min(len(idx1) * 2, len(idx0)), replace=False)
idx_all = np.concatenate([idx0_sampled, idx1])
rng.shuffle(idx_all)

X_bal, y_bal = X[idx_all], y[idx_all]
n0b, n1b = (y_bal == 0).sum(), (y_bal == 1).sum()
print(f"  Balanced — label=0: {n0b:,}  label=1: {n1b:,}  total: {len(y_bal):,}")

X_train, X_test, y_train, y_test = train_test_split(
    X_bal, y_bal, test_size=0.2, random_state=42, stratify=y_bal
)
print(f"  Train: {len(X_train):,}   Test: {len(X_test):,}", flush=True)


# ══════════════════════════════════════════════════════════════
# 4. 模型训练
# ══════════════════════════════════════════════════════════════
def evaluate(name, y_true, y_prob, y_pred, elapsed):
    auc   = roc_auc_score(y_true, y_prob)
    f1    = f1_score(y_true, y_pred)
    prec  = precision_score(y_true, y_pred)
    rec   = recall_score(y_true, y_pred)
    acc   = accuracy_score(y_true, y_pred)
    return {"name": name, "auc": auc, "f1": f1,
            "precision": prec, "recall": rec, "accuracy": acc, "time": elapsed}

results = []

# ── 4a. Logistic Regression（baseline）
print("\n[Step 4a] Logistic Regression ...", flush=True)
t = time.time()
lr = LogisticRegression(max_iter=200, C=10, solver="saga", n_jobs=-1)
lr.fit(X_train, y_train)
lr_prob = lr.predict_proba(X_test)[:, 1]
lr_pred = lr.predict(X_test)
elapsed = time.time() - t
results.append(evaluate("LogisticRegression", y_test, lr_prob, lr_pred, elapsed))
joblib.dump(lr, os.path.join(MODEL_DIR, "logistic_regression.pkl"))
print(f"  Done in {elapsed:.1f}s  AUC={results[-1]['auc']:.4f}", flush=True)

# ── 4b. LightGBM
print("\n[Step 4b] LightGBM ...", flush=True)
t = time.time()
lgb_model = lgb.LGBMClassifier(
    n_estimators=500, learning_rate=0.05,
    max_depth=7, num_leaves=63,
    min_child_samples=50, subsample=0.8,
    colsample_bytree=0.8, reg_lambda=1.0,
    n_jobs=-1, random_state=42, verbose=-1,
)
lgb_model.fit(X_train, y_train,
              eval_set=[(X_test, y_test)],
              callbacks=[lgb.early_stopping(50, verbose=False),
                         lgb.log_evaluation(100)])
lgb_prob = lgb_model.predict_proba(X_test)[:, 1]
lgb_pred = lgb_model.predict(X_test)
elapsed = time.time() - t
results.append(evaluate("LightGBM", y_test, lgb_prob, lgb_pred, elapsed))
lgb_model.booster_.save_model(os.path.join(MODEL_DIR, "lightgbm.txt"))
print(f"  Done in {elapsed:.1f}s  AUC={results[-1]['auc']:.4f}", flush=True)

# ── 4c. XGBoost（GPU / CPU 自动切换）
print(f"\n[Step 4c] XGBoost ({'GPU' if USE_GPU else 'CPU'}) ...", flush=True)
t = time.time()
xgb_params = dict(
    n_estimators=500, learning_rate=0.05,
    max_depth=7, min_child_weight=5,
    subsample=0.8, colsample_bytree=0.8,
    reg_lambda=1.0, gamma=0.1,
    eval_metric="auc", early_stopping_rounds=50,
    random_state=42, n_jobs=-1,
)
if USE_GPU:
    xgb_params["device"] = "cuda"

xgb_model = xgb.XGBClassifier(**xgb_params)
xgb_model.fit(X_train, y_train,
              eval_set=[(X_test, y_test)],
              verbose=100)
xgb_prob = xgb_model.predict_proba(X_test)[:, 1]
xgb_pred = xgb_model.predict(X_test)
elapsed = time.time() - t
results.append(evaluate("XGBoost", y_test, xgb_prob, xgb_pred, elapsed))
xgb_model.save_model(os.path.join(MODEL_DIR, "xgboost.ubj"))
print(f"  Done in {elapsed:.1f}s  AUC={results[-1]['auc']:.4f}", flush=True)


# ══════════════════════════════════════════════════════════════
# 5. 汇总对比
# ══════════════════════════════════════════════════════════════
print(f"\n{SEP}")
print("  Model Comparison")
print(SEP)
print(f"  {'Model':<22} {'AUC-ROC':>8} {'F1':>8} {'Precision':>10} {'Recall':>8} {'Accuracy':>9} {'Time':>7}")
print("  " + "-"*78)
best = max(results, key=lambda r: r["auc"])
for r in results:
    mark = " ◀ best" if r["name"] == best["name"] else ""
    print(f"  {r['name']:<22} {r['auc']:>8.4f} {r['f1']:>8.4f} "
          f"{r['precision']:>10.4f} {r['recall']:>8.4f} "
          f"{r['accuracy']:>9.4f} {r['time']:>5.0f}s{mark}")
print(SEP)


# ══════════════════════════════════════════════════════════════
# 6. 最优模型详细分析
# ══════════════════════════════════════════════════════════════
print(f"\n[Step 5] Best Model Detail: {best['name']}")
print(SEP)

name_to_prob = {
    "LogisticRegression": lr_prob,
    "LightGBM": lgb_prob,
    "XGBoost": xgb_prob,
}
best_prob = name_to_prob[best["name"]]
best_pred = (best_prob >= 0.5).astype(int)

# 混淆矩阵
cm = confusion_matrix(y_test, best_pred)
tn, fp, fn, tp = cm.ravel()
print(f"\n  Confusion Matrix:")
print(f"  {'':20} Predicted 0   Predicted 1")
print(f"  Actual 0 (Rejected)  {tn:>10,}    {fp:>10,}")
print(f"  Actual 1 (Accepted)  {fn:>10,}    {tp:>10,}")

# 分类报告
print(f"\n  Classification Report:")
print(classification_report(y_test, best_pred,
                             target_names=["Rejected", "Accepted"],
                             digits=4))

# 阈值分析
print(f"  Threshold Analysis (label=1):")
print(f"  {'Threshold':>9} {'TP':>9} {'FP':>9} {'FN':>9} {'Precision':>10} {'Recall':>9} {'F1':>8}")
print("  " + "-"*67)
for thresh in [0.3, 0.4, 0.5, 0.6, 0.7]:
    pred_t = (best_prob >= thresh).astype(int)
    _tp = ((pred_t==1)&(y_test==1)).sum()
    _fp = ((pred_t==1)&(y_test==0)).sum()
    _fn = ((pred_t==0)&(y_test==1)).sum()
    _p  = _tp/(_tp+_fp) if (_tp+_fp)>0 else 0
    _r  = _tp/(_tp+_fn) if (_tp+_fn)>0 else 0
    _f1 = 2*_p*_r/(_p+_r) if (_p+_r)>0 else 0
    print(f"  {thresh:>9.1f} {_tp:>9,} {_fp:>9,} {_fn:>9,} {_p:>10.4f} {_r:>9.4f} {_f1:>8.4f}")

# 特征重要性
print(f"\n  Feature Importances — {best['name']} (Top 10):")
print(f"  {'Feature':<20} {'Importance':>12}")
print("  " + "-"*34)
if best["name"] == "XGBoost":
    fi = xgb_model.feature_importances_
elif best["name"] == "LightGBM":
    fi = lgb_model.feature_importances_
else:
    fi = np.abs(lr.coef_[0])

fi_pairs = sorted(zip(FEATURES, fi), key=lambda x: -x[1])[:10]
for fname, imp in fi_pairs:
    print(f"  {fname:<20} {imp:>12.4f}")

print(f"\n{SEP}")
print(f"  All models saved to: {MODEL_DIR}")
print(f"  Total time: {time.time()-T0:.1f}s")
print(SEP, flush=True)
