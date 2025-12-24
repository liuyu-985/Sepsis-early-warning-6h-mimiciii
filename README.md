# Sepsis Early Warning (6-hour) on MIMIC-III

This project builds a **6-hour-ahead sepsis early-warning** model using **MIMIC-III** structured EHR features and clinical notes.  
It compares (1) classic ML baselines and (2) a deep learning text model, and explores **multimodal fusion** of tabular + notes signals.

---

## Project overview

**Goal:** Predict whether a patient will develop sepsis **6 hours before sepsis onset**.  
**Why it matters:** Earlier warnings can support clinical decision-making by enabling earlier evaluation and treatment.

**Modalities**
- **Tabular (structured):** vitals/labs/derived clinical features over time (engineered features)
- **Notes (text):** clinical notes transformed into features (TF-IDF) or embeddings (ClinicalBERT)
- **Fusion:** combine tabular and text signals into a single risk score

---

## Dataset

This project uses **MIMIC-III (v1.4)** (critical care EHR dataset). Access requires:
1. Credentialed access via PhysioNet
2. Completion of required training and agreement to the MIMIC data use terms

✅ This repository contains **code and documentation only** (no patient-level data).  
❌ Raw/derived MIMIC-III datasets and note text are **not** included to comply with data use restrictions.

---

## Methods

### 1) Feature / cohort extraction (SQL)
- `Data_preprocessing.sql`
- Extracts/constructs the cohort and structured features used for modeling (6-hour prediction horizon setup).

### 2) Baseline models (classic ML)
Notebook: `Baseline_model.ipynb`

Models:
- **CatBoost (Tabular features)**
- **TF-IDF + Logistic Regression (Notes)**
- **Logistic fusion (CatBoost + TF-IDF)**

### 3) Deep learning text model + fusion
Notebook: `Deep_learning_model.ipynb`

Models:
- **ClinicalBERT (Notes)**
- **MLP fusion (CatBoost + ClinicalBERT)**

### 4) Evaluation metrics
Because sepsis prediction is typically imbalanced, the following metrics are reported:
- **AUROC**
- **AUPRC**
- **F1 (test)**
- **Precision**
- **Recall**

---

## Results

Below are the summary results used in the presentation.

### Baseline model performance
| Model | Modality | AUROC | AUPRC | F1 (test) | Precision | Recall |
|---|---|---:|---:|---:|---:|---:|
| CatBoost | Tabular | ~0.99 | ~0.63 | ~0.58 | ~0.51 | ~0.66 |
| TF-IDF + Logistic | Notes | ~0.87 | ~0.60 | ~0.41 | ~0.27 | ~0.94 |
| Logistic fusion (CatBoost+TF-IDF) | Tabular+Notes | ~0.99 | ~0.63 | ~0.47 | ~0.32 | ~0.84 |

### Deep learning model performance
| Model | Modality | AUROC | AUPRC | F1 (test) | Precision | Recall |
|---|---|---:|---:|---:|---:|---:|
| CatBoost | Tabular | ~0.99 | ~0.63 | ~0.58 | ~0.51 | ~0.66 |
| ClinicalBERT | Notes | ~0.78 | ~0.42 | ~0.46 | ~0.39 | ~0.57 |
| MLP fusion (CatBoost+ClinicalBERT) | Tabular+Notes | ~0.99 | ~0.63 | ~0.50 | ~0.37 | ~0.79 |

---

## Reproducibility

### What’s included
- SQL for cohort/features construction
- Two notebooks covering baseline ML and deep learning + fusion
- Summary figures for performance comparison

### What’s not included
- Any MIMIC-III data (raw or derived)
- Full feature CSVs, note text, or patient-level intermediate files

### How to reproduce (recommended workflow)

1) **Set up MIMIC-III locally**
- Install MIMIC-III into your database environment (commonly PostgreSQL).
- Ensure you have appropriate access permissions.

2) **Run SQL to create cohort/features**
- Use `Data_preprocessing.sql`
- Export resulting tables to your local files or connect notebooks directly to the database (depending on the setup).

1) **Run baseline notebook**
- `Baseline_model.ipynb`
- Trains CatBoost + TF-IDF Logistic + logistic fusion and computes metrics.

1) **Run deep learning notebook**
- `Deep_learning_model.ipynb`
- Trains ClinicalBERT + fusion MLP and computes metrics.

---
