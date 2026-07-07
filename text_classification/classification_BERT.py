import os
import pandas as pd
import numpy as np
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder
from sklearn.metrics import classification_report, accuracy_score
import torch
from torch.utils.data import Dataset, DataLoader
from transformers import (
    AutoTokenizer,
    AutoModelForSequenceClassification,
    TrainingArguments,
    Trainer,
    EarlyStoppingCallback,
)
import warnings
warnings.filterwarnings("ignore")

# ──────────────────────────────────────────────
# 1. CONFIGURATION
# ──────────────────────────────────────────────
MODEL_NAME = "vinai/phobert-base-v2"
MODEL_CACHE = Path("artifacts/models--vinai--phobert-base-v2")  # local cache
DATA_DIR = Path("data/raw")
ARTIFACTS_DIR = Path("artifacts")
ARTIFACTS_DIR.mkdir(exist_ok=True)


# ✅ Correct local path — inside the snapshots hash folder
LOCAL_MODEL_PATH = (
    ARTIFACTS_DIR
    / "models--vinai--phobert-base-v2"
    / "snapshots"
    / "e2375d266bdf39c6e8e9a87af16a5da3190b0cc8"
)

MAX_LENGTH = 256
BATCH_SIZE = 64
EPOCHS = 1
LEARNING_RATE = 2e-5

# ──────────────────────────────────────────────
# 2. LOAD & PREPARE DATA
# ──────────────────────────────────────────────
def load_data(data_dir: Path) -> pd.DataFrame:
    """Load all parquet files from data directory."""
    parquet_files = list(data_dir.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {data_dir}")

    df_list = [pd.read_parquet(f) for f in parquet_files]
    df = pd.concat(df_list, ignore_index=True)
    print(f"✅ Loaded {len(df)} rows from {len(parquet_files)} file(s)")
    return df

def prepare_labels(df: pd.DataFrame, label_col: str = "domain"):
    """Encode string labels into integers."""
    le = LabelEncoder()
    df["label"] = le.fit_transform(df[label_col].astype(str))
    num_labels = len(le.classes_)
    print(f"📊 Number of classes: {num_labels}")
    print(f"   Classes: {list(le.classes_)}")
    return df, le, num_labels

df = load_data(DATA_DIR)
df, label_encoder, num_labels = prepare_labels(df, "domain")

# Stratified train/val split
train_df, val_df = train_test_split(
    df, test_size=0.2, random_state=42, stratify=df["label"]
)
print(f"📦 Train: {len(train_df)} | Val: {len(val_df)}")

# ──────────────────────────────────────────────
# 3. DATASET CLASS
# ──────────────────────────────────────────────
class PhobertDataset(Dataset):
    def __init__(self, texts, labels, tokenizer, max_length=256):
        self.texts = texts.tolist() if isinstance(texts, pd.Series) else texts
        self.labels = labels.tolist() if isinstance(labels, pd.Series) else labels
        self.tokenizer = tokenizer
        self.max_length = max_length

    def __len__(self):
        return len(self.texts)

    def __getitem__(self, idx):
        text = str(self.texts[idx])
        label = self.labels[idx]

        encoding = self.tokenizer(
            text,
            truncation=True,
            padding="max_length",
            max_length=self.max_length,
            return_tensors="pt",
        )

        return {
            "input_ids": encoding["input_ids"].squeeze(0),
            "attention_mask": encoding["attention_mask"].squeeze(0),
            "labels": torch.tensor(label, dtype=torch.long),
        }

# ──────────────────────────────────────────────
# 4. INIT TOKENIZER & MODEL
# ──────────────────────────────────────────────
tokenizer = AutoTokenizer.from_pretrained(str(LOCAL_MODEL_PATH), local_files_only=True)
model = AutoModelForSequenceClassification.from_pretrained(
    str(LOCAL_MODEL_PATH),
    num_labels=num_labels,
    ignore_mismatched_sizes=True,
    local_files_only=True,
)
print(f"🧠 Loaded {MODEL_NAME} with {num_labels} classes")

# Move to GPU if available
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
model.to(device)
print(f"⚡ Device: {device}")

# Build datasets
train_dataset = PhobertDataset(
    train_df["text"], train_df["label"], tokenizer, MAX_LENGTH
)
val_dataset = PhobertDataset(
    val_df["text"], val_df["label"], tokenizer, MAX_LENGTH
)

# ──────────────────────────────────────────────
# 5. TRAINING
# ──────────────────────────────────────────────
training_args = TrainingArguments(
    output_dir=str(ARTIFACTS_DIR / "checkpoints"),
    eval_strategy="steps",
    save_strategy="steps",
    save_total_limit=2,
    load_best_model_at_end=True,
    metric_for_best_model="accuracy",
    greater_is_better=True,
    learning_rate=LEARNING_RATE,
    per_device_train_batch_size=BATCH_SIZE,
    per_device_eval_batch_size=BATCH_SIZE * 2,
    num_train_epochs=EPOCHS,
    weight_decay=0.01,
    logging_dir=str(ARTIFACTS_DIR / "logs"),
    logging_steps=50,
    report_to="none",               # disable wandb / tensorboard
    fp16=torch.cuda.is_available(),
    dataloader_num_workers=2,
    seed=42,
)

def compute_metrics(eval_pred):
    predictions, labels = eval_pred
    preds = np.argmax(predictions, axis=1)
    accuracy = accuracy_score(labels, preds)
    return {"accuracy": accuracy}

trainer = Trainer(
    model=model,
    args=training_args,
    train_dataset=train_dataset,
    eval_dataset=val_dataset,
    tokenizer=tokenizer,
    compute_metrics=compute_metrics,
    callbacks=[EarlyStoppingCallback(early_stopping_patience=2)],
)

print("🚀 Starting training...")
trainer.train()

# ──────────────────────────────────────────────
# 6. SAVE FINAL MODEL & LABEL ENCODER
# ──────────────────────────────────────────────
final_model_path = ARTIFACTS_DIR / "phobert-domain-classifier"
trainer.save_model(str(final_model_path))
tokenizer.save_pretrained(str(final_model_path))

# Save label encoder
import joblib
joblib.dump(label_encoder, ARTIFACTS_DIR / "label_encoder.joblib")
print(f"💾 Model saved to {final_model_path}")
print(f"💾 Label encoder saved to {ARTIFACTS_DIR / 'label_encoder.joblib'}")

# ──────────────────────────────────────────────
# 7. EVALUATION
# ──────────────────────────────────────────────
eval_results = trainer.evaluate()
print(f"\n📊 Validation results: {eval_results}")

# Detailed classification report on validation set
val_preds = trainer.predict(val_dataset)
pred_labels = np.argmax(val_preds.predictions, axis=1)
true_labels = val_preds.label_ids

print("\n🎯 Classification Report:\n")
print(classification_report(
    true_labels,
    pred_labels,
    target_names=label_encoder.classes_,
    digits=4,
))

# ─── Confusion Matrix ────────────────────────
from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay
import matplotlib.pyplot as plt

cm = confusion_matrix(true_labels, pred_labels)
class_names = label_encoder.classes_

print("\n📊 Confusion Matrix (raw counts):\n")
header = f"{'':>20}"
for name in class_names:
    header += f"{name:>20}"
print(header)
for i, row in enumerate(cm):
    line = f"{class_names[i]:>20}"
    for val in row:
        line += f"{val:>20}"
    print(line)

# Plot + save
fig, ax = plt.subplots(figsize=(max(10, len(class_names) * 1.2),
                                max(8, len(class_names) * 0.9)))
disp = ConfusionMatrixDisplay(confusion_matrix=cm, display_labels=class_names)
disp.plot(ax=ax, cmap="Blues", values_format="d", xticks_rotation=45)
plt.title("Confusion Matrix — Validation Set", fontsize=14, fontweight="bold")
plt.tight_layout()
plt.savefig(str(ARTIFACTS_DIR / "confusion_matrix.png"), dpi=150, bbox_inches="tight")
plt.show()
print(f"📁 Confusion matrix saved to {ARTIFACTS_DIR / 'confusion_matrix.png'}")