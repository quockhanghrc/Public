import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
import numpy as np
import os
import joblib
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
import matplotlib.pyplot as plt
import seaborn as sns

# ============================================================
# HAN MODEL
# ============================================================
class WordAttention(nn.Module):
    """Word-level attention layer."""
    def __init__(self, hidden_size):
        super().__init__()
        self.attention = nn.Sequential(
            nn.Linear(hidden_size, hidden_size),
            nn.Tanh(),
            nn.Linear(hidden_size, 1)
        )
    
    def forward(self, x, mask=None):
        # x: (batch, n_sentences, hidden_size)
        # mask: (batch, n_sentences)
        
        scores = self.attention(x).squeeze(-1)  # (batch, n_sentences)
        
        if mask is not None:
            scores = scores.masked_fill(mask == 0, -1e9)
        
        weights = torch.softmax(scores, dim=-1)  # (batch, n_sentences)
        context = torch.bmm(weights.unsqueeze(1), x).squeeze(1)  # (batch, hidden_size)
        
        return context, weights


class SentenceAttention(nn.Module):
    """Sentence-level attention layer."""
    def __init__(self, hidden_size):
        super().__init__()
        self.attention = nn.Sequential(
            nn.Linear(hidden_size, hidden_size),
            nn.Tanh(),
            nn.Linear(hidden_size, 1)
        )
    
    def forward(self, x, mask=None):
        # x: (batch, n_docs, hidden_size)
        # mask: (batch, n_docs)
        
        scores = self.attention(x).squeeze(-1)
        
        if mask is not None:
            scores = scores.masked_fill(mask == 0, -1e9)
        
        weights = torch.softmax(scores, dim=-1)
        context = torch.bmm(weights.unsqueeze(1), x).squeeze(1)
        
        return context, weights


class HANClassifier(nn.Module):
    """
    Hierarchical Attention Network for document classification.
    
    Expects pre-computed sentence embeddings as input.
    """
    def __init__(self, hidden_size, num_classes, dropout=0.3):
        super().__init__()
        
        self.word_attention = WordAttention(hidden_size)
        self.sentence_attention = SentenceAttention(hidden_size)
        
        self.classifier = nn.Sequential(
            nn.Linear(hidden_size, hidden_size // 2),
            nn.ReLU(),
            nn.Dropout(dropout),
            nn.Linear(hidden_size // 2, num_classes)
        )
        
        self.dropout = nn.Dropout(dropout)
    
    def forward(self, sentence_embeddings, sentence_mask):
        """
        Args:
            sentence_embeddings: (batch, max_sentences, hidden_size)
            sentence_mask: (batch, max_sentences) — 1 for valid, 0 for padding
        
        Returns:
            logits: (batch, num_classes)
            word_attn_weights: (batch, max_sentences)
        """
        # Sentence-level attention
        doc_vector, sent_attn_weights = self.sentence_attention(
            sentence_embeddings, sentence_mask
        )
        
        doc_vector = self.dropout(doc_vector)
        
        # Classification
        logits = self.classifier(doc_vector)
        
        return logits, sent_attn_weights


# ============================================================
# DATASET
# ============================================================
class EmbeddingDataset(Dataset):
    """Dataset for pre-computed embeddings."""
    def __init__(self, embeddings, masks, labels):
        self.embeddings = torch.FloatTensor(embeddings)
        self.masks = torch.FloatTensor(masks)
        self.labels = torch.LongTensor(labels)
    
    def __len__(self):
        return len(self.labels)
    
    def __getitem__(self, idx):
        return self.embeddings[idx], self.masks[idx], self.labels[idx]


# ============================================================
# TRAINING LOOP
# ============================================================
def train_epoch(model, dataloader, optimizer, criterion, device):
    model.train()
    total_loss = 0
    all_preds = []
    all_labels = []
    
    for embeddings, masks, labels in dataloader:
        embeddings = embeddings.to(device)
        masks = masks.to(device)
        labels = labels.to(device)
        
        optimizer.zero_grad()
        
        logits, _ = model(embeddings, masks)
        loss = criterion(logits, labels)
        
        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=5.0)
        optimizer.step()
        
        total_loss += loss.item()
        preds = torch.argmax(logits, dim=1)
        all_preds.extend(preds.cpu().numpy())
        all_labels.extend(labels.cpu().numpy())
    
    avg_loss = total_loss / len(dataloader)
    accuracy = accuracy_score(all_labels, all_preds)
    
    return avg_loss, accuracy


def evaluate(model, dataloader, criterion, device):
    model.eval()
    total_loss = 0
    all_preds = []
    all_labels = []
    all_attentions = []
    
    with torch.no_grad():
        for embeddings, masks, labels in dataloader:
            embeddings = embeddings.to(device)
            masks = masks.to(device)
            labels = labels.to(device)
            
            logits, attn_weights = model(embeddings, masks)
            loss = criterion(logits, labels)
            
            total_loss += loss.item()
            preds = torch.argmax(logits, dim=1)
            all_preds.extend(preds.cpu().numpy())
            all_labels.extend(labels.cpu().numpy())
            all_attentions.append(attn_weights.cpu().numpy())
    
    avg_loss = total_loss / len(dataloader)
    accuracy = accuracy_score(all_labels, all_preds)
    
    return avg_loss, accuracy, all_preds, all_labels, np.concatenate(all_attentions)


import re

def visualize_attention(text, sentences, attn_weights, label_encoder, predicted_class, confidence, top_k=5):
    """
    Print attention weights per sentence in a readable format.
    """
    print(f"\n{'='*70}")
    print(f"📝 TEXT ANALYSIS — Predicted: {predicted_class} (conf={confidence:.2%})")
    print(f"{'='*70}")
    
    # Normalize attention to [0, 1] for readability
    attn_norm = attn_weights / (attn_weights.max() + 1e-8)
    
    for i, (sent, weight, norm_weight) in enumerate(zip(sentences, attn_weights, attn_norm)):
        bar = '█' * int(norm_weight * 30) + '░' * (30 - int(norm_weight * 30))
        print(f"[{i:3d}] {bar} | weight={weight:.4f} | {sent[:100]}{'...' if len(sent) > 100 else ''}")
    
    print(f"\n🏆 Top-{top_k} Most Important Sentences:")
    top_indices = np.argsort(attn_weights)[-top_k:][::-1]
    for rank, idx in enumerate(top_indices, 1):
        print(f"  #{rank}: [{idx}] (w={attn_weights[idx]:.4f}) {sentences[idx][:120]}{'...' if len(sentences[idx]) > 120 else ''}")


def plot_attention_heatmap(sentences, attn_weights, title=None, figsize=None):
    """
    Plot attention weights as a horizontal bar chart.
    """
    if figsize is None:
        figsize = (12, max(4, len(sentences) * 0.3))
    plt.figure(figsize=figsize)
    
    # Clip long sentence labels
    short_sents = [s[:60] + '...' if len(s) > 60 else s for s in sentences]
    
    colors = plt.cm.Blues(attn_weights / (attn_weights.max() + 1e-8))
    bars = plt.barh(range(len(sentences)), attn_weights, color=colors, edgecolor='gray', alpha=0.8)
    
    plt.yticks(range(len(sentences)), short_sents, fontsize=9)
    plt.xlabel('Attention Weight', fontsize=12)
    plt.title(title or 'Sentence-Level Attention Weights', fontsize=14, fontweight='bold')
    plt.gca().invert_yaxis()  # Most important at top
    plt.tight_layout()
    plt.show()


def plot_attention_sentence_distribution(attn_weights_all, save_path=None):
    """
    Plot distribution of attention across sentence positions (aggregated over all samples).
    Useful to see if model biases certain positions (first/last sentences).
    """
    # attn_weights_all: (num_samples, max_sentences) — already padded with zeros
    avg_attn = attn_weights_all.mean(axis=0)
    std_attn = attn_weights_all.std(axis=0)
    
    plt.figure(figsize=(10, 5))
    x = np.arange(len(avg_attn))
    plt.bar(x, avg_attn, yerr=std_attn, capsize=3, color='steelblue', alpha=0.7)
    plt.xlabel('Sentence Position (index)', fontsize=12)
    plt.ylabel('Average Attention Weight', fontsize=12)
    plt.title('Average Attention vs. Sentence Position', fontsize=14, fontweight='bold')
    plt.grid(axis='y', alpha=0.3)
    
    if save_path:
        plt.savefig(save_path, dpi=150, bbox_inches='tight')
    plt.show()


def plot_confusion_with_attention(val_true, val_preds, attn_weights_all, label_encoder):
    """
    Confusion matrix colored by average attention — shows which classes
    the model found 'easiest' (high attention differentiation).
    """
    from sklearn.metrics import confusion_matrix
    
    cm = confusion_matrix(val_true, val_preds)
    
    # Per-class average attention (not perfect but illustrative)
    class_names = label_encoder.classes_
    
    fig, ax = plt.subplots(figsize=(10, 8))
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues',
                xticklabels=class_names, yticklabels=class_names,
                ax=ax)
    ax.set_title('Confusion Matrix', fontsize=14)
    ax.set_ylabel('True Label')
    ax.set_xlabel('Predicted Label')
    plt.tight_layout()
    plt.show()

def analyze_cumulative_attention(val_attentions, val_masks, save_dir=None):
    """
    Analyze cumulative attention coverage.
    
    Shows: "How many sentences do you actually need?"
    Generates: Cumulative coverage plot + Attention distribution plot
    
    Args:
        val_attentions: (n_docs, max_sentences) attention weights
        val_masks: (n_docs, max_sentences) mask (1=real sentence, 0=padding)
        save_dir: If set, save plot to this directory
    """
    # Mask out padding
    masked_attentions = val_attentions * val_masks
    
    # Per-document: cumulative attention by sentence POSITION
    cumulative_by_position = np.cumsum(masked_attentions, axis=1)
    avg_cumulative = np.mean(cumulative_by_position, axis=0)
    
    print(f"\n{'='*60}")
    print(f"📊 CUMULATIVE ATTENTION COVERAGE")
    print(f"{'='*60}")
    print(f"\nBy reading first N sentences (in order), you capture:\n")
    
    for n in [1, 2, 3, 5, 8, 10, 12, 15, 20, 25, 30, val_attentions.shape[1]]:
        if n <= len(avg_cumulative):
            coverage = avg_cumulative[n-1] * 100
            bar = '█' * int(coverage // 4)
            print(f"   First {n:2d} sentences: {coverage:5.1f}% {bar}")
    
    # Find thresholds
    print(f"\n💡 Optimal max_sentences recommendations:")
    for target in [0.90, 0.95, 0.99]:
        n_needed = int(np.argmax(avg_cumulative >= target) + 1)
        print(f"   To capture {target:.0%} of decision: max_sentences = {n_needed}")
    
    # Diminishing returns point (where marginal gain < 0.5%)
    diffs = np.diff(avg_cumulative)
    elbow = None
    for i in range(len(diffs)):
        if diffs[i] < 0.005:
            elbow = i + 1
            break
    
    if elbow:
        print(f"\n   ⚠️  Diminishing returns after {elbow} sentences")
        print(f"      (each additional sentence adds <0.5% to decision)")
        print(f"      → Consider reducing max_sentences from {val_attentions.shape[1]} to {elbow}")
        print(f"      → Saves {(1 - elbow/val_attentions.shape[1])*100:.0f}% compute")
    
    # ---- Plot: Cumulative coverage + Attention distribution ----
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    
    # Plot 1: Cumulative coverage curve
    x = np.arange(1, len(avg_cumulative) + 1)
    axes[0].plot(x, avg_cumulative * 100, 'b-o', linewidth=2, markersize=4)
    axes[0].axhline(y=90, color='green', linestyle='--', alpha=0.7, label='90%')
    axes[0].axhline(y=95, color='orange', linestyle='--', alpha=0.7, label='95%')
    axes[0].axhline(y=99, color='red', linestyle='--', alpha=0.7, label='99%')
    axes[0].set_xlabel('Number of Sentences (first N)')
    axes[0].set_ylabel('Cumulative Decision Coverage (%)')
    axes[0].set_title('How Many Sentences Do You Need?')
    axes[0].legend()
    axes[0].grid(True, alpha=0.3)
    
    # Plot 2: Attention per position (bar chart)
    avg_attn = np.mean(masked_attentions, axis=0)
    std_attn = np.std(masked_attentions, axis=0)
    axes[1].bar(x, avg_attn * 100, yerr=std_attn * 100, capsize=3,
                color='steelblue', alpha=0.7, edgecolor='white')
    axes[1].set_xlabel('Sentence Position')
    axes[1].set_ylabel('Average Attention Weight (%)')
    axes[1].set_title('Attention Distribution by Position')
    axes[1].grid(True, alpha=0.3, axis='y')
    
    plt.tight_layout()
    if save_dir:
        plt.savefig(os.path.join(save_dir, 'cumulative_attention.png'), dpi=150, bbox_inches='tight')
    plt.show()
    
    # ---- Importance-sorted cumulative (bonus analysis) ----
    sorted_attentions = np.sort(masked_attentions, axis=1)[:, ::-1]
    cumulative_sorted = np.cumsum(sorted_attentions, axis=1)
    avg_cumulative_sorted = np.mean(cumulative_sorted, axis=0)
    
    print(f"\n{'─'*60}")
    print(f"📊 IMPORTANCE-SORTED (If you could pick best sentences)")
    print(f"{'─'*60}")
    print(f"\n   {'N':<5} {'By Position':<18} {'By Importance':<18} {'Gap':<10}")
    print(f"   {'─'*5} {'─'*18} {'─'*18} {'─'*10}")
    
    for n in [1, 2, 3, 5, 8, 10]:
        if n <= len(avg_cumulative):
            pos = avg_cumulative[n-1] * 100
            imp = avg_cumulative_sorted[n-1] * 100
            gap = imp - pos
            print(f"   {n:<5} {pos:<18.1f}% {imp:<18.1f}% {gap:<+10.1f}%")
    
    gap_5 = avg_cumulative_sorted[4] - avg_cumulative[4]
    if gap_5 > 0.10:
        print(f"\n   ⚠️  Large gap ({gap_5*100:.1f}%) at 5 sentences")
        print(f"      → HAN attention is NOT finding the best sentences")
        print(f"      → Model may need more training or different architecture")
    else:
        print(f"\n   ✅ Small gap → HAN is correctly identifying important sentences")
    
    return avg_cumulative

# ============================================================
# MAIN TRAINING SCRIPT
# ============================================================
def main():
    # --- Configuration ---
    DATA_DIR = 'data'
    MODEL_SAVE_DIR = 'models'
    BATCH_SIZE = 32
    LEARNING_RATE = 1e-3
    N_EPOCHS = 50
    PATIENCE = 7
    DROPOUT = 0.2
    
    device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
    print(f"\n{'='*60}")
    print(f"🚀 HAN Training")
    print(f"{'='*60}")
    print(f"💻 Device: {device}")
    
    os.makedirs(MODEL_SAVE_DIR, exist_ok=True)
    
    # --- Load embeddings ---
    print(f"\n📂 Loading embeddings from '{DATA_DIR}/'...")
    train_data = np.load(os.path.join(DATA_DIR, 'train_embeddings.npz'))
    val_data = np.load(os.path.join(DATA_DIR, 'val_embeddings.npz'))
    label_encoder = joblib.load(os.path.join(DATA_DIR, 'label_encoder.joblib'))
    
    train_embeddings = train_data['embeddings']
    train_masks = train_data['masks']
    train_labels = train_data['labels']
    
    val_embeddings = val_data['embeddings']
    val_masks = val_data['masks']
    val_labels = val_data['labels']
    
    num_classes = len(label_encoder.classes_)
    hidden_size = train_embeddings.shape[2]
    
    print(f"   Train: {train_embeddings.shape}")
    print(f"   Val:   {val_embeddings.shape}")
    print(f"   Classes: {num_classes} → {list(label_encoder.classes_)}")
    print(f"   Hidden size: {hidden_size}")
    
    # --- Create dataloaders ---
    train_dataset = EmbeddingDataset(train_embeddings, train_masks, train_labels)
    val_dataset = EmbeddingDataset(val_embeddings, val_masks, val_labels)
    
    train_loader = DataLoader(train_dataset, batch_size=BATCH_SIZE, shuffle=True)
    val_loader = DataLoader(val_dataset, batch_size=BATCH_SIZE, shuffle=False)
    
    print(f"\n📊 DataLoaders:")
    print(f"   Train: {len(train_loader)} batches")
    print(f"   Val:   {len(val_loader)} batches")
    
    # --- Initialize model ---
    model = HANClassifier(
        hidden_size=hidden_size,
        num_classes=num_classes,
        dropout=DROPOUT
    ).to(device)
    
    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(model.parameters(), lr=LEARNING_RATE)
    scheduler = optim.lr_scheduler.ReduceLROnPlateau(
        optimizer, mode='max', factor=0.5, patience=3
    )
    
    print(f"\n🤖 Model: HANClassifier")
    print(f"   Parameters: {sum(p.numel() for p in model.parameters()):,}")
    print(f"   Dropout: {DROPOUT}")
    print(f"   LR: {LEARNING_RATE}")
    
    # --- Training loop ---
    print(f"\n{'='*60}")
    print(f"🎯 Training")
    print(f"{'='*60}")
    
    best_val_acc = 0
    patience_counter = 0
    history = {'train_loss': [], 'train_acc': [], 'val_loss': [], 'val_acc': []}
    
    for epoch in range(1, N_EPOCHS + 1):
        train_loss, train_acc = train_epoch(model, train_loader, optimizer, criterion, device)
        val_loss, val_acc, val_preds, val_true, _ = evaluate(model, val_loader, criterion, device)
        
        scheduler.step(val_acc)
        
        history['train_loss'].append(train_loss)
        history['train_acc'].append(train_acc)
        history['val_loss'].append(val_loss)
        history['val_acc'].append(val_acc)
        
        print(f"Epoch {epoch:2d}/{N_EPOCHS} | "
              f"Train Loss: {train_loss:.4f} | Train Acc: {train_acc:.4f} | "
              f"Val Loss: {val_loss:.4f} | Val Acc: {val_acc:.4f}", end='')
        
        if val_acc > best_val_acc:
            best_val_acc = val_acc
            torch.save({
                'epoch': epoch,
                'model_state_dict': model.state_dict(),
                'optimizer_state_dict': optimizer.state_dict(),
                'val_acc': val_acc,
                'hidden_size': hidden_size,
                'num_classes': num_classes,
            }, os.path.join(MODEL_SAVE_DIR, 'han_best.pt'))
            print(f" ✅ (saved, new best)")
            patience_counter = 0
        else:
            patience_counter += 1
            print(f" (patience {patience_counter}/{PATIENCE})")
        
        if patience_counter >= PATIENCE:
            print(f"\n⏹️  Early stopping triggered at epoch {epoch}")
            break
    
    # ================================================================
    # FINAL EVALUATION (single call, no duplicate)
    # ================================================================
    print(f"\n{'='*60}")
    print(f"📊 Final Evaluation (Best Model)")
    print(f"{'='*60}")
    
    # Load best model
    checkpoint = torch.load(os.path.join(MODEL_SAVE_DIR, 'han_best.pt'), weights_only=False)
    model.load_state_dict(checkpoint['model_state_dict'])
    
    # Single evaluation — captures everything we need
    val_loss, val_acc, val_preds, val_true, val_attentions = evaluate(
        model, val_loader, criterion, device
    )
    # val_attentions shape: (num_val_samples, max_sentences)
    
    print(f"\n📈 Best Validation Accuracy: {best_val_acc:.4f}")
    print(f"\n📋 Classification Report:")
    print(classification_report(
        val_true, val_preds,
        target_names=label_encoder.classes_,
        digits=4
    ))
    
    # --- Confusion matrix ---
    cm = confusion_matrix(val_true, val_preds)
    plt.figure(figsize=(10, 8))
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues',
                xticklabels=label_encoder.classes_,
                yticklabels=label_encoder.classes_)
    plt.title('Confusion Matrix')
    plt.ylabel('True Label')
    plt.xlabel('Predicted Label')
    plt.tight_layout()
    plt.savefig(os.path.join(MODEL_SAVE_DIR, 'confusion_matrix.png'))
    plt.show()
    
    # ================================================================
    # ATTENTION ANALYSIS (fixed)
    # ================================================================
    print(f"\n{'='*60}")
    print(f"🔍 Attention Analysis")
    print(f"{'='*60}")
    
    # 1. Aggregate attention distribution across sentence positions
    plot_attention_sentence_distribution(
        val_attentions,
        save_path=os.path.join(MODEL_SAVE_DIR, 'attention_position_distribution.png')
    )
    
    # 2. Show top-3 correctly classified samples with highest confidence
    #    Since evaluate() doesn't return logits, we approximate by re-running
    #    on a subset with logit capture (or just show random correct examples)
    correct_mask = np.array(val_preds) == np.array(val_true)
    correct_indices = np.where(correct_mask)[0]
    
    if len(correct_indices) > 0:
        # Pick up to 3 random correct samples
        np.random.seed(42)
        sample_indices = np.random.choice(correct_indices, size=min(3, len(correct_indices)), replace=False)
        
        print(f"\n📌 Attention Examples (correctly classified validation samples):")
        for idx in sample_indices:
            # Extract attention weights for this sample (remove padding)
            attn = val_attentions[idx]
            valid_len = int(val_masks[idx].sum())  # number of actual sentences
            attn_valid = attn[:valid_len]
            
            true_label = label_encoder.classes_[val_true[idx]]
            pred_label = label_encoder.classes_[val_preds[idx]]
            
            print(f"\n   Sample #{idx}: True={true_label}, Pred={pred_label}")
            top3 = np.argsort(attn_valid)[-3:][::-1]
            for rank, sent_idx in enumerate(top3, 1):
                print(f"     Top-{rank}: sent[{sent_idx}] (w={attn_valid[sent_idx]:.4f})")
    
    # 3. Attention per class (optional)
    print(f"\n   📊 Aggregate attention by sentence position saved to 'attention_position_distribution.png'")

    # ================================================================
    # CUMULATIVE ATTENTION ANALYSIS (NEW)
    # ================================================================
    print(f"\n{'='*60}")
    print(f"🔍 Cumulative Attention Analysis")
    print(f"{'='*60}")
    
    avg_cumulative = analyze_cumulative_attention(
        val_attentions,
        val_masks,
        save_dir=MODEL_SAVE_DIR
    )
    
    print(f"\n   📊 Cumulative attention chart saved to 'cumulative_attention.png'")

    # --- Training history plots ---
    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    
    axes[0].plot(history['train_loss'], label='Train Loss')
    axes[0].plot(history['val_loss'], label='Val Loss')
    axes[0].set_xlabel('Epoch')
    axes[0].set_ylabel('Loss')
    axes[0].legend()
    axes[0].set_title('Loss Curves')
    axes[0].grid(True, alpha=0.3)
    
    axes[1].plot(history['train_acc'], label='Train Acc')
    axes[1].plot(history['val_acc'], label='Val Acc')
    axes[1].set_xlabel('Epoch')
    axes[1].set_ylabel('Accuracy')
    axes[1].legend()
    axes[1].set_title('Accuracy Curves')
    axes[1].grid(True, alpha=0.3)
    
    plt.tight_layout()
    plt.savefig(os.path.join(MODEL_SAVE_DIR, 'training_history.png'))
    plt.show()
    
    print(f"\n✅ Training complete!")
    print(f"   Best model: {os.path.join(MODEL_SAVE_DIR, 'han_best.pt')}")
    print(f"   Confusion matrix: {os.path.join(MODEL_SAVE_DIR, 'confusion_matrix.png')}")
    print(f"   Training history: {os.path.join(MODEL_SAVE_DIR, 'training_history.png')}")
    print(f"   Attention distribution: {os.path.join(MODEL_SAVE_DIR, 'attention_position_distribution.png')}")
    print(f"   Cumulative attention:  {os.path.join(MODEL_SAVE_DIR, 'cumulative_attention.png')}")

# ============================================================
# INFERENCE EXAMPLE
# ============================================================
def predict_text(text, model, embedder, label_encoder, max_sentences=40, device='cpu'):
    """
    Predict domain for a new text.
    """
    from pipeline import split_and_segment  # Or import from your module
    
    # Split and segment
    sentences = split_and_segment(text)[:max_sentences]
    
    if len(sentences) == 0:
        return "unknown", 0.0
    
    # Create embedding matrix
    sent_embs = embedder.encode_document(sentences, max_sentences=max_sentences)
    sent_embs = torch.FloatTensor(sent_embs).unsqueeze(0).to(device)  # (1, max_sentences, hidden)
    
    mask = torch.zeros(1, max_sentences, device=device)
    mask[0, :len(sentences)] = 1
    
    model.eval()
    with torch.no_grad():
        logits, attn_weights = model(sent_embs, mask)
        probs = torch.softmax(logits, dim=1)
        pred_idx = torch.argmax(logits, dim=1).item()
        confidence = probs[0, pred_idx].item()
    
    predicted_domain = label_encoder.classes_[pred_idx]
    return predicted_domain, confidence


# ============================================================
# RUN
# ============================================================
if __name__ == "__main__":
    main()