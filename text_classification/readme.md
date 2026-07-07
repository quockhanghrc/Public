## Text Classification Experiment

## Overview
This repository contains experiments for Vietnamese text classification using a subset of the Vietnam Curated Dataset. The goal of this project is to evaluate and compare different modeling approaches—ranging from zero-shot generative classification to supervised deep learning architectures—to find an optimal balance between classification performance and computational efficiency.

By analyzing multiple methodologies (including Hierarchical Attention Networks and direct transformer fine-tuning), these experiments aim to identify practical trade-offs in resource consumption, training complexity, and handling of class imbalances.

### Dataset & Configuration
* **Dataset:** Vietnam Curated Dataset (100k record subset for experimentation)
* **Embedding Model:** PhoBERT-v2

### Current Approaches & Findings
* **Hierarchical Attention Network (HAN):** Yields higher performance compared to the generative approach, but requires substantial GPU resources during the embedding conversion stage. Finding an optimal maximum sentence threshold is a key factor in reducing both embedding generation overhead and overall training time.
* **Generative (Zero-shot):** Evaluated as a baseline, resulting in lower performance compared to HAN. Furthermore, because this approach relies on external APIs, routine testing and validation can lead to high operational costs.
* **BERT finetuning:** Achieves higher overall accuracy but shows limited performance on minority classes. However, it requires less computational and development effort compared to HAN due to its single-stage modeling approach.

