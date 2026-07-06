## Text Classification Experiment

### Dataset & Configuration
* **Dataset:** Vietnam Curated Dataset (100k record subset for experimentation)
* **Embedding Model:** PhoBERT-v2

### Current Approaches & Findings
* **Hierarchical Attention Network (HAN):** Yields higher performance compared to the generative approach, but requires substantial GPU resources during the embedding conversion stage.
* **Generative (Zero-shot):** Tested as a baseline, showing lower performance compared to HAN.

### Next Steps / TODO
* **Single-Stage Fine-tuning:** Fine-tune PhoBERT-v2 directly on the classification task to streamline the pipeline into a single stage and optimize resource utilization.