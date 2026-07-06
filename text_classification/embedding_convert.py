import torch
import numpy as np
import pandas as pd
from tqdm import tqdm
from typing import List, Tuple, Optional
import os
import warnings
import logging
warnings.filterwarnings('ignore')
logging.getLogger().setLevel(logging.ERROR)

# Global flag for pyvi warning
_PYVI_WARNING_SHOWN = False

# ============================================================
# SENTENCE SPLITTER (robust — minor fixes applied)
# ============================================================
def split_into_sentences(text: str, min_sentence_length: int = 5, max_sentence_length: int = 1000) -> List[str]:
    """Robust sentence splitter with abbreviation protection."""
    import re
    if not text or not isinstance(text, str):
        return []
    text = re.sub(r'\s+', ' ', text.strip())
    if not text:
        return []

    protected = {}
    counter = [0]

    def protect(pattern: str, text: str) -> str:
        def _replace(match):
            placeholder = f"__PROT_{counter[0]}__"
            protected[placeholder] = match.group()
            counter[0] += 1
            return placeholder
        return re.sub(pattern, _replace, text)

    abbr_patterns = [
        r'\b(?:Mr|Mrs|Ms|Miss|Dr|Prof|Sr|Jr|St|Ave|Blvd|Dept|Univ|Corp|Inc|Ltd|Co|Govt|Est|Assn|Bros)\.',
        r'\b(?:etc|vs|approx|dept|est|govt|natl|orig|temp|vol|ed|pp)\.',
        r'\b(?:Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\.',
        r'\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun)\.',
        r'\b[A-Z]\.(?:\s?[A-Z]\.)+',
    ]

    for pat in abbr_patterns:
        text = protect(pat, text)

    text = protect(r'\d+\.\d+', text)
    text = protect(r'https?://[^\s<>"]+|www\.[^\s<>"]+', text)
    text = protect(r'[\w.+-]+@[\w-]+\.[\w.-]+', text)
    text = text.replace('...', '__ELLIPSIS__')

    sentences = re.split(r'(?<=[.!?])\s+(?=[A-Z"\'«»„“])|(?<=[.!?])$', text)
    final_sentences = []
    for sent in sentences:
        parts = re.split(r'(?<=[.!?])\s+(?=[«»""''""])', sent)
        final_sentences.extend(parts)

    restored = []
    for sent in final_sentences:
        if not sent.strip():
            continue
        sent = sent.replace('__ELLIPSIS__', '...')
        for placeholder, original in protected.items():
            sent = sent.replace(placeholder, original)
        restored.append(sent)

    cleaned = []
    for sent in restored:
        sent = sent.strip()
        sent = sent.strip('"\'«»')               # ✅ fixed: single call strips all
        sent = re.sub(r'\s+', ' ', sent)
        if len(sent) >= min_sentence_length:
            if len(sent) > max_sentence_length:
                subs = re.split(r'(?<=[,;])\s+', sent)
                cleaned.extend([s.strip() for s in subs if len(s.strip()) >= min_sentence_length])
            else:
                cleaned.append(sent)

    return cleaned if cleaned else [text]


# ============================================================
# WORD SEGMENTATION FOR VIETNAMESE
# ============================================================
def word_segment_vietnamese(text: str) -> str:
    """Word segmentation for Vietnamese."""
    global _PYVI_WARNING_SHOWN
    try:
        from pyvi import ViTokenizer
        return ViTokenizer.tokenize(text)
    except ImportError:
        if not _PYVI_WARNING_SHOWN:
            print("⚠️  pyvi not installed. Run: pip install pyvi")
            _PYVI_WARNING_SHOWN = True
        return text


def split_and_segment(text: str) -> List[str]:
    """Split text into sentences and word-segment each sentence."""
    sentences = split_into_sentences(text)
    return [word_segment_vietnamese(sent) for sent in sentences]


# ============================================================
# STRATIFIED TRAIN-VAL SPLIT (80-20 preserving domain ratio)
# ============================================================
def stratified_train_val_split(
    df: pd.DataFrame,
    test_size: float = 0.2,
    random_state: int = 42
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split dataset preserving domain (label) distribution.
    """
    from sklearn.model_selection import train_test_split

    train_df, val_df = train_test_split(
        df,
        test_size=test_size,
        random_state=random_state,
        stratify=df['domain']
    )

    print(f"\n{'='*60}")
    print(f"📊 TRAIN-VAL SPLIT (80-20, Stratified by Domain)")
    print(f"{'='*60}")
    print(f"\n📈 Overall:")
    print(f"   Train: {len(train_df):,} samples ({len(train_df)/len(df)*100:.1f}%)")
    print(f"   Val:   {len(val_df):,} samples ({len(val_df)/len(df)*100:.1f}%)")

    print(f"\n📈 Domain Distribution:")
    domain_dist = pd.DataFrame({
        'Train': train_df['domain'].value_counts(),
        'Val': val_df['domain'].value_counts(),
        'Total': df['domain'].value_counts()
    }).fillna(0).astype(int)
    domain_dist['Train %'] = (domain_dist['Train'] / domain_dist['Total'] * 100).round(1)
    domain_dist['Val %'] = (domain_dist['Val'] / domain_dist['Total'] * 100).round(1)
    print(domain_dist)

    # Verify stratification
    print(f"\n✅ Stratification check:")
    train_ratio = len(train_df) / len(df)
    for domain in df['domain'].unique():
        domain_train_ratio = len(train_df[train_df['domain'] == domain]) / len(df[df['domain'] == domain])
        print(f"   {domain}: train ratio = {domain_train_ratio:.3f} (target: {train_ratio:.3f})")

    return train_df, val_df


# ============================================================
# SentenceTransformerEmbedder (cleaned up)
# ============================================================
class SentenceTransformerEmbedder:
    """
    Convert sentences to embedding vectors using Sentence Transformers.
    Model is cached in the specified cache_dir.
    """

    def __init__(
        self,
        model_name: str = 'vinai/phobert-base-v2',
        batch_size: int = 64,
        device: str = None,
        cache_dir: str = 'artifacts',
        max_seq_length: int = 256
    ):
        from sentence_transformers import SentenceTransformer

        self.batch_size = batch_size
        self.cache_dir = cache_dir
        self.device = device if device else ('cuda' if torch.cuda.is_available() else 'cpu')

        # Create cache directory
        os.makedirs(self.cache_dir, exist_ok=True)

        print(f"\n{'='*60}")
        print(f"📦 Loading Sentence Transformer: {model_name}")
        print(f"{'='*60}")
        print(f"💻 Device: {self.device}")
        print(f"🗂️  Cache:  {os.path.abspath(self.cache_dir)}")

        # Set cache folder BEFORE loading model
        os.environ['TRANSFORMERS_CACHE'] = self.cache_dir   # ✅ fixed: no redundant import
        os.environ['HF_HOME'] = self.cache_dir

        # Load model
        self.model = SentenceTransformer(model_name, cache_folder=self.cache_dir)

        # Set max sequence length
        self.model.max_seq_length = max_seq_length
        if hasattr(self.model, 'tokenizer'):
            self.model.tokenizer.model_max_length = max_seq_length

        if self.device == 'cuda':
            self.model = self.model.to('cuda')

        self.hidden_size = self.model.get_sentence_embedding_dimension()

        print(f"✅ Model loaded. Hidden size: {self.hidden_size}")
        print(f"📐 Max sequence length: {max_seq_length}")
        print(f"⚡ Batch size: {self.batch_size}\n")

        # ✅ Show cache status on init
        self._show_cache_status()

    def _show_cache_status(self):
        """Show what's cached in the artifacts folder."""
        cache_items = []
        for root, dirs, files in os.walk(self.cache_dir):
            for d in dirs:
                cache_items.append(os.path.join(root, d))
            for f in files:
                if f.endswith('.json') or f.endswith('.bin') or f.endswith('.h5'):
                    cache_items.append(os.path.join(root, f))

        if cache_items:
            print(f"   📦 Using cached model files ({len(cache_items)} items)")
        else:
            print(f"   🌐 Downloading model for the first time (will cache here)...")

    def truncate_by_tokens(self, sentence: str, max_tokens: int = 250) -> str:
        """✅ NEW: Truncate sentence to fit within max_tokens (subword-aware)."""
        tokens = self.model.tokenizer.encode(sentence)
        if len(tokens) <= max_tokens:
            return sentence
        # Decode truncated tokens back to string
        return self.model.tokenizer.decode(tokens[:max_tokens], skip_special_tokens=True)

    def encode_batch(self, sentences: List[str]) -> np.ndarray:
        """Encode a batch of sentences with token-aware truncation."""
        truncated = [self.truncate_by_tokens(sent) for sent in sentences]
        return self.model.encode(
            truncated,
            batch_size=self.batch_size,
            show_progress_bar=False,
            convert_to_numpy=True
        )

    def encode_document(
        self,
        sentences: List[str],
        max_sentences: int = 40,
        return_mask: bool = False
    ):
        """Encode a document into embedding matrix with truncation."""
        sent_count = min(len(sentences), max_sentences)
        sentences = sentences[:max_sentences]

        if len(sentences) == 0:
            embeddings = np.zeros((max_sentences, self.hidden_size))
            mask = np.zeros(max_sentences)
            return (embeddings, mask) if return_mask else embeddings

        # Token-aware truncation
        truncated_sentences = [self.truncate_by_tokens(sent) for sent in sentences]

        sentence_embeddings = self.model.encode(
            truncated_sentences,
            batch_size=self.batch_size,
            show_progress_bar=False,
            convert_to_numpy=True
        )

        embeddings = np.zeros((max_sentences, self.hidden_size))
        embeddings[:sent_count] = sentence_embeddings

        mask = np.zeros(max_sentences)
        mask[:sent_count] = 1

        if return_mask:
            return embeddings, mask
        return embeddings


# ============================================================
# CONVERT DATAFRAME TO EMBEDDINGS (ALL BUGS FIXED)
# ============================================================
def convert_and_save_embeddings(
    df: pd.DataFrame,
    split_name: str,
    data_dir: str = 'data',
    max_sentences: int = 40,
    batch_size: int = 64,
    embedder: SentenceTransformerEmbedder = None,
    chunk_size: int = 1000,
    label_encoder = None                         # ✅ NEW: accept pre-fitted encoder
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    """
    Convert dataframe to embeddings using chunked encoding.

    Returns:
        embeddings: (n_docs, max_sentences, hidden_size)
        masks:      (n_docs, max_sentences)
        labels:     (n_docs,)
    """
    import joblib

    # ✅ FIXED: use pre-fitted encoder if provided
    if label_encoder is None:
        from sklearn.preprocessing import LabelEncoder
        label_encoder = LabelEncoder()
        label_encoder.fit(df['domain'])

    encoded_labels = label_encoder.transform(df['domain'])  # transform, not fit_transform

    n_docs = len(df)
    embeddings = np.zeros((n_docs, max_sentences, embedder.hidden_size), dtype=np.float32)
    masks = np.zeros((n_docs, max_sentences), dtype=np.float32)

    # Segment all texts first
    all_segmented = []
    for text in tqdm(df['text'], desc=f"Segmenting {split_name}"):
        all_segmented.append(split_and_segment(text))

    # Encode in chunks
    for chunk_start in tqdm(range(0, n_docs, chunk_size), desc=f"Encoding {split_name}"):
        chunk_end = min(chunk_start + chunk_size, n_docs)
        chunk_sentences = []
        chunk_boundaries = []

        for i in range(chunk_start, chunk_end):
            truncated = all_segmented[i][:max_sentences]
            # ✅ FIXED: apply token-aware truncation to each sentence
            truncated = [embedder.truncate_by_tokens(sent) for sent in truncated]
            chunk_boundaries.append(len(truncated))
            chunk_sentences.extend(truncated)

        if chunk_sentences:
            chunk_emb = embedder.model.encode(
                chunk_sentences, batch_size=batch_size,
                show_progress_bar=False, convert_to_numpy=True
            )
            sent_idx = 0
            for j, n_sents in enumerate(chunk_boundaries):
                doc_idx = chunk_start + j
                if n_sents > 0:
                    embeddings[doc_idx, :n_sents] = chunk_emb[sent_idx:sent_idx + n_sents]
                    masks[doc_idx, :n_sents] = 1
                    sent_idx += n_sents

    # ✅ FIXED: labels assigned ONCE, outside the loop
    labels = np.array(encoded_labels)

    # Save to data/
    os.makedirs(data_dir, exist_ok=True)
    save_path = os.path.join(data_dir, f'{split_name}_embeddings.npz')
    np.savez_compressed(save_path, embeddings=embeddings, masks=masks, labels=labels)

    # ✅ NOTE: encoder is saved by full_pipeline (once), not here

    print(f"\n✅ {split_name.upper()} set saved:")
    print(f"   Embeddings: {embeddings.shape}")
    print(f"   Masks:      {masks.shape}")
    print(f"   Labels:     {labels.shape} ({len(label_encoder.classes_)} classes)")
    print(f"   Classes:    {list(label_encoder.classes_)}")
    print(f"   Location:   {save_path}")

    return embeddings, masks, labels


# ============================================================
# FULL PIPELINE (ALL BUGS FIXED)
# ============================================================
def full_pipeline(
    folder_path: str,
    data_dir: str = 'data',
    cache_dir: str = 'artifacts',
    max_sentences: int = 40,
    batch_size: int = 64,
    test_size: float = 0.2,
    sample: int = None,
    chunk_size: int = 1000                     # ✅ NEW: exposed kwarg
):
    """
    Complete pipeline:
    1. Load parquet files
    2. Stratified 80-20 train-val split
    3. Fit label encoder ONCE on all data
    4. Convert both splits to Sentence Transformer embeddings (chunked)
    5. Save everything to data/ directory
    """
    import glob
    from sklearn.preprocessing import LabelEncoder
    import joblib

    print(f"\n{'='*70}")
    print(f"🚀 STARTING FULL PIPELINE (Sentence Transformer)")
    print(f"{'='*70}")

    # --- Step 1: Load data ---
    print(f"\n📂 Step 1: Loading parquet files from {folder_path}")

    if folder_path.endswith('.parquet'):
        parquet_files = [folder_path]
    else:
        parquet_files = glob.glob(os.path.join(folder_path, "*.parquet"))

    print(f"Found {len(parquet_files)} parquet files")

    df_list = []
    for f in tqdm(parquet_files, desc="Loading"):
        try:                                      # ✅ NEW: error handling
            df_list.append(pd.read_parquet(f))
        except Exception as e:
            print(f"⚠️  Skipping corrupted file {f}: {e}")

    if not df_list:
        raise RuntimeError("No valid parquet files loaded!")

    df = pd.concat(df_list, ignore_index=True)

    if sample and sample < len(df):
        df = df.sample(sample, random_state=42)
        print(f"Sampled to {sample} rows")

    print(f"\n✅ Total: {len(df):,} rows")
    print(f"   Columns: {df.columns.tolist()}")

    # --- Step 2: Stratified split ---
    print(f"\n✂️ Step 2: Stratified 80-20 train-val split")
    train_df, val_df = stratified_train_val_split(df, test_size=test_size)

    # --- Step 3: Fit label encoder ONCE on ALL data ---
    print(f"\n🏷️  Step 3: Fitting label encoder on all {len(df):,} samples")
    label_encoder = LabelEncoder()
    label_encoder.fit(df['domain'])
    encoder_path = os.path.join(data_dir, 'label_encoder.joblib')
    os.makedirs(data_dir, exist_ok=True)
    joblib.dump(label_encoder, encoder_path)
    print(f"   {len(label_encoder.classes_)} classes: {list(label_encoder.classes_)}")
    print(f"   Saved to: {encoder_path}")

    # --- Step 4: Initialize embedder ---
    print(f"\n🤖 Step 4: Initializing Sentence Transformer embedder")
    embedder = SentenceTransformerEmbedder(
        batch_size=batch_size,
        cache_dir=cache_dir
    )

    # --- Step 5: Convert and save train set ---
    print(f"\n📀 Step 5: Converting and saving train embeddings")
    train_emb, train_mask, train_labels = convert_and_save_embeddings(
        df=train_df,
        split_name='train',
        data_dir=data_dir,
        max_sentences=max_sentences,
        batch_size=batch_size,
        embedder=embedder,
        chunk_size=chunk_size,                # ✅ pass through
        label_encoder=label_encoder            # ✅ use pre-fitted encoder
    )

    # --- Step 6: Convert and save validation set ---
    print(f"\n📀 Step 6: Converting and saving validation embeddings")
    val_emb, val_mask, val_labels = convert_and_save_embeddings(
        df=val_df,
        split_name='val',
        data_dir=data_dir,
        max_sentences=max_sentences,
        batch_size=batch_size,
        embedder=embedder,
        chunk_size=chunk_size,                # ✅ pass through
        label_encoder=label_encoder            # ✅ same encoder — no overwrite!
    )

    # --- Summary ---
    print(f"\n{'='*70}")
    print(f"✅ PIPELINE COMPLETE!")
    print(f"{'='*70}")
    print(f"\n📁 Files saved in '{data_dir}/':")
    for f in sorted(os.listdir(data_dir)):
        fpath = os.path.join(data_dir, f)
        size = os.path.getsize(fpath)
        print(f"   ├── {f} ({size/1024/1024:.2f} MB)")

    print(f"\n📊 Summary:")
    print(f"   Train: {len(train_df):,} samples → {train_emb.shape}")
    print(f"   Val:   {len(val_df):,} samples → {val_emb.shape}")
    print(f"   Classes: {len(np.unique(train_labels))}")

    # Show cached model
    print(f"\n📦 Model cached in '{cache_dir}/':")
    if os.path.exists(cache_dir):
        for f in sorted(os.listdir(cache_dir)):
            fpath = os.path.join(cache_dir, f)
            try:
                size = os.path.getsize(fpath) if os.path.isfile(fpath) else sum(
                    os.path.getsize(os.path.join(dirpath, fn))
                    for dirpath, _, fns in os.walk(fpath) for fn in fns
                )
                print(f"   ├── {f}")
            except Exception:
                print(f"   ├── {f}")
        print(f"   📍 {os.path.abspath(cache_dir)}")

    return train_emb, train_mask, train_labels, val_emb, val_mask, val_labels


# ============================================================
# LOAD SAVED EMBEDDINGS
# ============================================================
def load_saved_embeddings(data_dir: str = 'data'):
    """Load previously saved embeddings from data/ directory."""
    import joblib

    train_path = os.path.join(data_dir, 'train_embeddings.npz')
    val_path = os.path.join(data_dir, 'val_embeddings.npz')
    encoder_path = os.path.join(data_dir, 'label_encoder.joblib')

    for path in [train_path, val_path, encoder_path]:
        if not os.path.exists(path):
            raise FileNotFoundError(f"Missing file: {path}")

    train_data = np.load(train_path)
    val_data = np.load(val_path)
    label_encoder = joblib.load(encoder_path)

    print(f"\n📂 Loaded embeddings from '{data_dir}/':")
    print(f"   Train: {train_data['embeddings'].shape}")
    print(f"   Val:   {val_data['embeddings'].shape}")
    print(f"   Classes: {list(label_encoder.classes_)}")

    return train_data, val_data, label_encoder


# ============================================================
# USAGE
# ============================================================
if __name__ == "__main__":
    PARQUET_PATH = r"data/raw"
    DATA_DIR = 'data'
    CACHE_DIR = 'artifacts'
    MAX_SENTENCES = 75         ## Subset part of max sentence     
    BATCH_SIZE = 128
    CHUNK_SIZE = 10000          # ✅ tune based on GPU memory
    SAMPLE_SIZE = 100000         # set to None for full 368k

    train_emb, train_mask, train_labels, val_emb, val_mask, val_labels = full_pipeline(
        folder_path=PARQUET_PATH,
        data_dir=DATA_DIR,
        cache_dir=CACHE_DIR,
        max_sentences=MAX_SENTENCES,
        batch_size=BATCH_SIZE,
        test_size=0.2,
        sample=SAMPLE_SIZE,
        chunk_size=CHUNK_SIZE   # ✅ now passed through
    )