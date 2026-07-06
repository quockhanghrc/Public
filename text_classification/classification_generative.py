import openai
import pandas as pd
import numpy as np
from tqdm import tqdm
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report, confusion_matrix
import matplotlib.pyplot as plt
import seaborn as sns
import os
import json
import time
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
warnings.filterwarnings('ignore')

# ============================================================
# YOUR ACTUAL 25 CATEGORIES
# ============================================================
CATEGORIES = sorted([
    "Arts_and_Entertainment", "Sensitive_Subjects", "Health",
    "People_and_Society", "Sports", "News",
    "Food_and_Drink", "Business_and_Industrial", "Travel_and_Transportation",
    "Jobs_and_Education", "Beauty_and_Fitness", "Home_and_Garden",
    "Computers_and_Electronics", "Law_and_Government", "Internet_and_Telecom",
    "Books_and_Literature", "Games", "Autos_and_Vehicles",
    "Finance", "Real_Estate", "Pets_and_Animals",
    "Science", "Shopping", "Hobbies_and_Leisure",
    "Online_Communities"
])

CATEGORY_DESCRIPTIONS = {
    "Arts_and_Entertainment": "nghệ thuật, giải trí, phim ảnh, âm nhạc",
    "Sensitive_Subjects": "chủ đề nhạy cảm, tội phạm, bạo lực",
    "Health": "sức khỏe, y tế, bệnh viện, thuốc men",
    "People_and_Society": "con người, xã hội, cộng đồng",
    "Sports": "thể thao, bóng đá, cầu thủ, giải đấu",
    "News": "tin tức, thời sự, sự kiện hiện tại",
    "Food_and_Drink": "ẩm thực, đồ ăn, thức uống, nhà hàng",
    "Business_and_Industrial": "kinh doanh, công nghiệp, doanh nghiệp",
    "Travel_and_Transportation": "du lịch, giao thông, vận tải",
    "Jobs_and_Education": "việc làm, giáo dục, trường học",
    "Beauty_and_Fitness": "làm đẹp, thể hình, chăm sóc da",
    "Home_and_Garden": "nhà cửa, vườn tược, nội thất",
    "Computers_and_Electronics": "máy tính, điện tử, phần cứng",
    "Law_and_Government": "luật pháp, chính phủ, chính sách",
    "Internet_and_Telecom": "internet, viễn thông, mạng xã hội",
    "Books_and_Literature": "sách, văn học, tiểu thuyết",
    "Games": "trò chơi, game, esports",
    "Autos_and_Vehicles": "ô tô, xe cộ, phương tiện",
    "Finance": "tài chính, ngân hàng, chứng khoán",
    "Real_Estate": "bất động sản, nhà đất, căn hộ",
    "Pets_and_Animals": "thú cưng, động vật, chó mèo",
    "Science": "khoa học, nghiên cứu, vật lý, hóa học",
    "Shopping": "mua sắm, thương mại, sản phẩm",
    "Hobbies_and_Leisure": "sở thích, giải trí cá nhân",
    "Online_Communities": "cộng đồng trực tuyến, diễn đàn"
}

SYSTEM_PROMPT = f"""You are a Vietnamese news classifier. Classify each article into EXACTLY ONE category.

Categories: {', '.join(CATEGORIES)}

Respond with a JSON array of category names, one per article in order.
Example: ["Sports", "Health", "News"]
"""


# ============================================================
# NORMALIZATION (optimized with dict lookup)
# ============================================================
# Pre-build normalization lookup
CATEGORY_LOWER = {cat.lower(): cat for cat in CATEGORIES}
CATEGORY_SPACE = {cat.lower().replace('_', ' '): cat for cat in CATEGORIES}

def normalize_category(pred: str) -> str:
    """Fast normalization using pre-built dicts."""
    pred = pred.strip().replace('"', '').replace("'", '').replace('.', '').replace(',', '')
    
    # Direct match
    pred_lower = pred.lower()
    if pred_lower in CATEGORY_LOWER:
        return CATEGORY_LOWER[pred_lower]
    
    # Space version
    pred_space = pred_lower.replace('_', ' ').replace('-', ' ')
    if pred_space in CATEGORY_SPACE:
        return CATEGORY_SPACE[pred_space]
    
    # Contains check
    for key, val in CATEGORY_SPACE.items():
        if key in pred_space or pred_space in key:
            return val
    
    return "UNKNOWN"


# ============================================================
# PARALLEL BATCH CLASSIFICATION
# ============================================================
progress_lock = threading.Lock()
completed_count = 0
total_count = 0

def classify_batch(articles, model="gpt-4o-mini", max_chars=2000, api_key=None):
    """Classify a batch of articles in one API call."""
    global completed_count
    
    # Truncate
    truncated = [art[:max_chars] for art in articles]
    
    # Build batch prompt
    batch_text = ""
    for i, art in enumerate(truncated):
        batch_text += f"\n--- Article {i+1} ---\n{art}\n"
    
    user_prompt = f"""Classify these {len(articles)} Vietnamese news articles.
Respond with a JSON array of {len(articles)} category names, one per article in order.

Articles:{batch_text}

JSON array of categories:"""
    
    for attempt in range(3):
        try:
            response = openai.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": user_prompt}
                ],
                temperature=0.0,
                max_tokens=50 * len(articles),
                response_format={"type": "json_object"}
            )
            
            raw = response.choices[0].message.content.strip()
            
            # Parse JSON
            import re
            json_match = re.search(r'\[.*?\]', raw, re.DOTALL)
            if json_match:
                predictions = json.loads(json_match.group())
            else:
                data = json.loads(raw)
                predictions = data.get('categories', data.get('predictions', list(data.values())))
            
            # Ensure correct length
            if len(predictions) != len(articles):
                predictions = predictions[:len(articles)]
                while len(predictions) < len(articles):
                    predictions.append("UNKNOWN")
            
            with progress_lock:
                completed_count += len(articles)
            
            return [normalize_category(p) for p in predictions]
            
        except Exception as e:
            if attempt < 2:
                time.sleep(2 ** attempt)
            else:
                with progress_lock:
                    completed_count += len(articles)
                return ["UNKNOWN"] * len(articles)


# ============================================================
# PROGRESS THREAD
# ============================================================
def show_progress():
    """Display progress every 5 seconds."""
    while completed_count < total_count:
        with progress_lock:
            done = completed_count
        pct = done / total_count * 100 if total_count > 0 else 0
        elapsed = time.time() - start_time
        eta = (elapsed / (done + 1)) * (total_count - done) if done > 0 else 0
        print(f"\r⏳ Progress: {done}/{total_count} ({pct:.1f}%) | "
              f"Elapsed: {elapsed:.0f}s | ETA: {eta:.0f}s ({eta/60:.1f} min)", end="")
        time.sleep(5)


# ============================================================
# FAST EVALUATION WITH PARALLEL BATCHES
# ============================================================
def evaluate_zero_shot_fast(
    df: pd.DataFrame,
    text_col: str = 'text',
    label_col: str = 'domain',
    model: str = "gpt-4o-mini",
    test_size: float = 0.2,
    sample_size: int = None,
    max_chars: int = 2000,
    batch_size: int = 20,
    max_workers: int = 5,  # Parallel API calls
    random_state: int = 42,
    output_dir: str = 'generative_results',
    api_key: str = None
) -> dict:
    """
    Fast zero-shot evaluation using PARALLEL batch API calls.
    5x faster than sequential batches = 50-100x faster than single article.
    """
    global total_count, completed_count, start_time
    completed_count = 0
    
    print(f"\n{'='*70}")
    print(f"🔮 PARALLEL ZERO-SHOT GENERATIVE CLASSIFICATION")
    print(f"{'='*70}")
    print(f"📦 Model: {model}")
    print(f"⚡ Parallel workers: {max_workers}")
    print(f"📦 Batch size: {batch_size}")
    print(f"📊 Dataset: {len(df):,} articles")
    
    if api_key:
        openai.api_key = api_key
    
    if sample_size and sample_size < len(df):
        df = df.sample(sample_size, random_state=random_state)
        print(f"   Sampled to: {sample_size}")
    
    # Split
    train_df, val_df = train_test_split(
        df, test_size=test_size, random_state=random_state,
        stratify=df[label_col]
    )
    
    print(f"\n📊 Train: {len(train_df):,} | Val: {len(val_df):,}")
    
    # Create batches
    val_texts = val_df[text_col].tolist()
    val_labels = val_df[label_col].tolist()
    total_count = len(val_texts)
    
    batches = []
    for i in range(0, len(val_texts), batch_size):
        batches.append(val_texts[i:i+batch_size])
    
    n_batches = len(batches)
    print(f"📦 {n_batches} batches of {batch_size}")
    
    # Start progress thread
    global start_time
    start_time = time.time()
    progress_thread = threading.Thread(target=show_progress, daemon=True)
    progress_thread.start()
    
    # Run batches in parallel
    all_predictions = []
    all_true = []
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all batches
        futures = {
            executor.submit(classify_batch, batch_texts, model, max_chars, api_key): batch_labels
            for batch_texts, batch_labels in zip(batches, 
                [val_labels[i:i+batch_size] for i in range(0, len(val_labels), batch_size)])
        }
        
        # Collect results as they complete
        for future in as_completed(futures):
            batch_labels = futures[future]
            predictions = future.result()
            all_predictions.extend(predictions)
            all_true.extend(batch_labels)
    
    print(f"\n\n✅ Classification complete in {time.time() - start_time:.1f}s")
    
    # Convert to numpy arrays
    predictions = np.array(all_predictions)
    true_labels = np.array(all_true)
    
    # Filter valid
    valid_mask = (predictions != "UNKNOWN") & (predictions != "ERROR")
    valid_preds = predictions[valid_mask]
    valid_true = true_labels[valid_mask]
    
    n_valid = valid_mask.sum()
    n_total = len(predictions)
    
    print(f"\n📊 Results:")
    print(f"   Valid: {n_valid}/{n_total} ({n_valid/n_total*100:.1f}%)")
    print(f"   Unknown: {(predictions == 'UNKNOWN').sum()}")
    
    if n_valid == 0:
        print("❌ No valid predictions!")
        return {"error": "no valid predictions"}
    
    # Accuracy
    accuracy = accuracy_score(valid_true, valid_preds)
    print(f"\n📈 Accuracy: {accuracy:.4f} ({accuracy*100:.2f}%)")
    
    # Classification Report
    unique_labels = sorted(set(valid_true) | set(valid_preds))
    print(f"\n📋 Classification Report:")
    print(classification_report(valid_true, valid_preds, labels=unique_labels, digits=4, zero_division=0))
    
    # Confusion Matrix
    os.makedirs(output_dir, exist_ok=True)
    cm = confusion_matrix(valid_true, valid_preds, labels=unique_labels)
    
    plt.figure(figsize=(20, 18))
    annot = np.where(cm > 0, cm.astype(str), '')
    sns.heatmap(cm, annot=annot, fmt='', cmap='Blues',
                xticklabels=unique_labels, yticklabels=unique_labels,
                cbar_kws={'label': 'Count'}, linewidths=0.5, linecolor='gray')
    plt.title(f'Confusion Matrix — {model}\nAccuracy: {accuracy:.2%} on {n_valid} samples',
              fontsize=16, fontweight='bold')
    plt.ylabel('True Label', fontsize=12)
    plt.xlabel('Predicted Label', fontsize=12)
    plt.xticks(rotation=45, ha='right', fontsize=7)
    plt.yticks(rotation=0, fontsize=7)
    plt.tight_layout()
    
    cm_path = os.path.join(output_dir, f'confusion_matrix_{model.replace("/", "_")}.png')
    plt.savefig(cm_path, dpi=150, bbox_inches='tight')
    plt.show()
    print(f"💾 Confusion matrix: {cm_path}")
    
    # FAST per-class accuracy (vectorized, no loop)
    print(f"\n📊 Per-Class Accuracy:")
    for label in unique_labels:
        mask = valid_true == label
        n = mask.sum()
        if n > 0:
            acc = (valid_preds[mask] == label).mean()
            correct = (valid_preds[mask] == label).sum()
            bar = '█' * int(acc * 20) + '░' * (20 - int(acc * 20))
            print(f"   {label:35s} {bar} {acc:.4f} ({correct}/{n})")
    
    # Save results
    results_df = pd.DataFrame({
        'true_label': true_labels,
        'predicted': predictions,
        'correct': predictions == true_labels,
        'valid': valid_mask
    })
    results_path = os.path.join(output_dir, f'results_parallel_{model.replace("/", "_")}.csv')
    results_df.to_csv(results_path, index=False)
    print(f"💾 Results saved: {results_path}")
    
    print(f"\n✅ Complete! Time: {time.time() - start_time:.1f}s")
    
    return {
        'model': model,
        'n_val': n_total,
        'n_valid': int(n_valid),
        'accuracy': float(accuracy),
        'time_seconds': time.time() - start_time
    }


# ============================================================
# RUN
# ============================================================
if __name__ == "__main__":
    # Load your data
    df = pd.read_parquet("data/raw/train-00000-of-00132.parquet")
    print(f"Loaded {len(df):,} articles")
    print(f"Categories: {df['domain'].nunique()}")
    
    # Set your API key
    # openai.api_key = 'your-key'
    
    # Run with 5 parallel workers
    # 18,439 articles / 20 batch / 5 workers = ~184 API calls per worker
    # Estimated time: ~3-5 minutes (vs 15 min sequential, vs 4.75 hours single)
    summary = evaluate_zero_shot_fast(
        df=df,
        text_col='text',
        label_col='domain',
        model="gpt-4o",
        test_size=0.2,
        sample_size=10000,        # 10000 total → 2000 validation
        max_chars=2000,          # Shorter for speed
        batch_size=20,           # 20 articles per call
        max_workers=5,           # 5 parallel API calls
        output_dir='generative_results'
    )