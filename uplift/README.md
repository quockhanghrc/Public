# Hillstrom Uplift Modeling Project

## Overview

This project applies **Uplift Modeling** (also known as Incremental Modeling or Persuasion Modeling) to the classic **Hillstrom Email Campaign dataset**. The goal is to identify which customers are **persuadable** — i.e., those who will only take the desired action (visit, convert, spend) if they receive the email, and would not do so otherwise.

The dataset contains 64,000 customers from an email test by an e-commerce company, with three treatment groups: **Mens E-Mail**, **Womens E-Mail**, and **No E-Mail** (control).

---

## Dataset

| Column | Type | Description |
|---|---|---|
| `recency` | numeric | Months since last purchase |
| `history` | numeric | Dollar amount of past purchases |
| `mens` | categorical | Mens merchandise indicator |
| `womens` | categorical | Womens merchandise indicator |
| `zip_code` | categorical | Urban / Suburban / Rural |
| `newbie` | categorical | New customer flag |
| `channel` | categorical | Phone / Web / Multichannel |
| `history_segment` | categorical | Binned history value |
| `visit` | binary (target) | Visited website within 2 weeks |
| `conversion` | binary (target) | Purchased within 2 weeks |
| `spend` | continuous (target) | Dollars spent within 2 weeks |
| `treatment` | categorical | Campaign received |

---

## Uplift Modeling Approach

### Two Main Approaches

1. **Meta-Learners (S/T-Model)**
   - Treatment is used as a feature; the model predicts the target directly.
   - At inference, you must run **two predictions** (treatment=1 vs treatment=0) and subtract to get the uplift score.
   - Predicts the **target variable** directly.

2. **Label Transformation (ClassTransformation)**
   - Implemented via `sklift.models.ClassTransformation`.
   - Focuses on the **causal effect** — higher scores mean a stronger relationship between treatment and target.
   - **No additional calculation needed at inference** — the model outputs uplift scores directly.
   - This is the primary method used in this project.

### Label Transformation (Z-label)

The ClassTransformation method transforms the target `y` and treatment `t` into a single **Z-label**:

```
Z = 1  if (treatment=1 AND y=1) OR (treatment=0 AND y=0)
Z = 0  otherwise
```

A classifier is then trained to predict `Z`. The resulting probability `P(Z=1 | X)` is the uplift score.

---

## Project Structure

```
hillstrom/
├── README.md
├── EDA_Hillstrom_dataset.ipynb    # Exploratory Data Analysis
├── hillstrom_eda.ipynb            # Extended EDA with visualizations
├── hillstrom_analysis.ipynb       # Model loading, scoring, calibration & evaluation
├── hillstrom_modeling_visit.ipynb # Uplift model for visit (binary target)
├── hillstrom_modeling_conversion.ipynb / .py  # Uplift model for conversion (binary target)
├── hillstrom_modeling_spending.ipynb  # Two-stage model: conversion uplift + spend regression
├── data/
│   ├── hillstrom.joblib           # Raw dataset (sklearn Bunch)
│   └── hillstrom.parquet          # Processed DataFrame
└── models/
    ├── mens_uplift_model.pkl              # Visit model (Men's campaign)
    ├── mens_uplift_model_visit.pkl        # Visit model (Men's campaign, alt)
    ├── mens_uplift_model_conversion.pkl   # Conversion model (Men's campaign)
    ├── mens_uplift_model_spending_classification.pkl  # Conversion stage (spending pipeline)
    ├── womens_uplift_model.pkl            # Visit model (Women's campaign)
    ├── womens_uplift_model_visit.pkl      # Visit model (Women's campaign, alt)
    └── womens_uplift_model_conversion.pkl # Conversion model (Women's campaign)
```

---

## Models by Target

### 1. Visit (Binary Classification)
- **Notebook:** `hillstrom_modeling_visit.ipynb`
- **Models:** `mens_uplift_model.pkl`, `womens_uplift_model.pkl`
- **Approach:** ClassTransformation with CatBoostClassifier
- **Evaluation:** Qini AUC, Uplift@K, AUUC curves

### 2. Conversion (Binary Classification)
- **Notebook:** `hillstrom_modeling_conversion.ipynb` / `.py`
- **Models:** `mens_uplift_model_conversion.pkl`, `womens_uplift_model_conversion.pkl`
- **Approach:** ClassTransformation with CatBoostClassifier
- **Evaluation:** Qini AUC, Uplift@K, feature importance

### 3. Spend (Two-Stage Pipeline)
- **Notebook:** `hillstrom_modeling_spending.ipynb`
- **Stage 1 — Classification:** Predict probability of conversion (uplift model)
  - Model: `mens_uplift_model_spending_classification.pkl`
- **Stage 2 — Regression:** Predict spend amount for converted customers
  - Uses `CatBoostRegressor` on log-transformed spend (`log1p`)
  - Features include engineered `freq_score` and `segment_rank`
- **Evaluation:** MAE (log & dollar space), R², correlation, actual-vs-predicted plots

---

## Calibration

Since raw uplift scores from different models are on different scales, a **calibration step** is applied to make them comparable:

1. Bucket raw predictions into 15 quantiles.
2. For each bucket, calculate the **actual real-world uplift** (treatment conversion rate − control conversion rate).
3. Fit an **Isotonic Regression** to map raw scores → true incremental conversion rates.

This allows fair comparison between Men's and Women's campaign models when deciding which treatment to assign.

---

## Evaluation Metrics

### Uplift Curve
- Plots the **difference in conversion rate** between treatment and control as you go deeper into the population ranked by uplift score.
- A rising curve indicates the model successfully identifies persuadable customers at the top.

### Qini Curve
- Plots the **cumulative incremental conversions** (treatment conversions − control conversions) against the population fraction.
- **Qini AUC** (Area Under the Qini Curve) is the primary metric — higher is better.

### AUUC (Area Under the Uplift Curve)
- Measures the area between the treatment and control conversion ratio curves.

### Uplift@K
- Measures the uplift achieved in the top K% of customers ranked by predicted uplift score.

---

## Key Findings

- **Label Transformation (ClassTransformation)** is preferred over meta-learners because it directly outputs causal effect scores without needing two separate predictions at inference.
- **Calibration** is essential when comparing uplift scores across different models (e.g., Men's vs Women's campaign).
- For **spend**, a two-stage pipeline is used: first predict whether the customer will convert (uplift classification), then predict how much they will spend (regression on converted customers only).
- The **decision rule**: apply a minimum uplift threshold, then assign the treatment (Men's or Women's email) with the higher calibrated uplift score.

---

## Dependencies

- `scikit-uplift` (`sklift`)
- `catboost`
- `scikit-learn`
- `pandas`, `numpy`
- `matplotlib`, `seaborn`
- `joblib`, `dill`

---

## References

- [Hillstrom Dataset (MineThatData)](https://blog.minethatdata.com/2008/03/minethatdata-e-mail-analytics-and-data.html)
- [scikit-uplift Documentation](https://www.uplift-modeling.com/en/latest/)
- [ClassTransformation Paper](https://link.springer.com/article/10.1007/s10115-011-0439-4)
