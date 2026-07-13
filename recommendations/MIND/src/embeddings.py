"""
Pretrained word embeddings from HuggingFace transformers.

Loads any HF model's input embeddings (BERT, RoBERTa, DistilBERT, ELECTRA,
MiniLM, etc.) and aligns them to the training vocabulary. The embedding
dimension is auto-detected from the model config, so the model's --embed_dim
is overridden automatically. Models are cached locally (cache-first) by
HuggingFace, so reruns need no re-download.

Swap models freely with --embed_model <hf-id>, e.g.:
    bert-base-uncased
    roberta-base
    distilbert-base-uncased
    google/electra-base-discriminator
    sentence-transformers/all-MiniLM-L6-v2
"""

import os

import numpy as np
import torch
from transformers import AutoModel, AutoTokenizer


def _infer_hidden_size(model, tokenizer) -> int:
    """Best-effort detection of the model's hidden/embedding dimension."""
    cfg = getattr(model, "config", None)
    for attr in ("hidden_size", "d_model", "n_embd", "dim", "hidden_dim"):
        if cfg is not None and hasattr(cfg, attr):
            try:
                return int(getattr(cfg, attr))
            except (TypeError, ValueError):
                pass
    # Fallback: inspect the input embedding layer shape
    emb = model.get_input_embeddings()
    return int(emb.weight.shape[1])


def load_hf_embeddings(
    vocab: dict,
    model_name: str,
    cache_dir: str = "cache",
    device: str = "cpu",
    pool: str = "mean",
) -> tuple:
    """
    Load a HuggingFace model's input embeddings aligned to `vocab`.

    Args:
        vocab: token -> index dict (0=PAD, 1=UNK, ...).
        model_name: HF model id, e.g. 'bert-base-uncased'.
        cache_dir: local HF cache folder (cache-first; no re-download).
        device: device for the HF model ('cpu'/'cuda').
        pool: how to combine subword pieces for a multi-piece word
              ('mean' = average of pieces, 'first' = first piece only).

    Returns:
        (matrix, num_found, embed_dim) where matrix is a
        (vocab_size, embed_dim) np.ndarray (PAD/UNK/OOV rows = 0),
        num_found is the number of vocab tokens covered, and embed_dim is the
        model's hidden size (used to auto-set the NRMS embedding dim).
    """
    os.makedirs(cache_dir, exist_ok=True)
    print(f"  Loading HuggingFace model '{model_name}' (cache_dir={cache_dir}) ...")
    tokenizer = AutoTokenizer.from_pretrained(model_name, cache_dir=cache_dir)
    model = AutoModel.from_pretrained(model_name, cache_dir=cache_dir)
    model = model.to(device)
    model.eval()

    # Auto-detect embedding dimension from the model config
    embed_dim = _infer_hidden_size(model, tokenizer)
    print(f"  Detected embedding dim = {embed_dim}")

    # Get the input embedding matrix (word -> vector)
    emb_layer = model.get_input_embeddings()
    emb_weight = emb_layer.weight  # (hf_vocab_size, embed_dim)
    hf_vocab_size = emb_weight.shape[0]

    # Batch-tokenize all vocab tokens at once (fast)
    tokens = list(vocab.keys())
    encodings = tokenizer(tokens, add_special_tokens=False)
    input_ids_list = encodings["input_ids"]

    matrix = np.zeros((len(vocab), embed_dim), dtype=np.float32)
    num_found = 0
    with torch.no_grad():
        for token, ids in zip(tokens, input_ids_list):
            idx = vocab[token]
            # Keep only in-range subword ids
            ids = [i for i in ids if 0 <= i < hf_vocab_size]
            if not ids:
                continue  # OOV -> leave zero (random init fallback)
            vecs = emb_weight[ids]  # (n_pieces, embed_dim)
            vec = vecs.mean(dim=0).cpu().numpy() if pool == "mean" else vecs[0].cpu().numpy()
            matrix[idx] = vec
            num_found += 1

    coverage = 100.0 * num_found / max(1, len(vocab))
    print(f"  HF embedding coverage: {num_found}/{len(vocab)} tokens ({coverage:.1f}%)")
    return matrix, num_found, embed_dim


def apply_hf_to_model(
    model: torch.nn.Module,
    hf_matrix: np.ndarray,
    device: torch.device,
    freeze: bool = False,
) -> bool:
    """
    Copy a preloaded HF embedding matrix into model.news_encoder.word_embedding.
    Returns True on success.

    Args:
        model: NRMSModel (must expose news_encoder.word_embedding).
        hf_matrix: (vocab_size, embed_dim) array from load_hf_embeddings.
        device: target torch device.
        freeze: if True, set word_embedding.weight.requires_grad = False.
    """
    with torch.no_grad():
        model.news_encoder.word_embedding.weight.copy_(
            torch.tensor(hf_matrix, device=device)
        )
    print(f"  HF initialized word_embedding ({hf_matrix.shape[0]} rows).")
    if freeze:
        model.news_encoder.word_embedding.weight.requires_grad = False
        print("  Word embeddings FROZEN (pretrained only).")
    return True
