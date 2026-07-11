"""
Vocabulary builder and tokenizer for news titles.
"""

from collections import Counter
from typing import List, Optional

# Special tokens
PAD_TOKEN = "<pad>"
UNK_TOKEN = "<unk>"
PAD_IDX = 0
UNK_IDX = 1


def tokenize(text: str) -> List[str]:
    """Simple whitespace tokenizer with lowercasing."""
    if not isinstance(text, str) or not text.strip():
        return []
    return text.lower().split()


def build_vocab(
    texts: List[str],
    min_freq: int = 2,
    max_size: Optional[int] = None,
) -> dict:
    """
    Build word-to-index vocabulary from a list of texts.

    Args:
        texts: List of raw text strings.
        min_freq: Minimum frequency to include a token.
        max_size: Max vocabulary size (excluding special tokens).

    Returns:
        Dictionary mapping token -> index (0=PAD, 1=UNK, then by freq).
    """
    counter: Counter = Counter()
    for t in texts:
        counter.update(tokenize(t))

    # Filter by min_freq and sort by frequency (desc), then alphabetically for ties
    sorted_tokens = sorted(
        [(tok, freq) for tok, freq in counter.items() if freq >= min_freq],
        key=lambda x: (-x[1], x[0]),
    )

    vocab = {PAD_TOKEN: PAD_IDX, UNK_TOKEN: UNK_IDX}
    for i, (tok, _) in enumerate(sorted_tokens):
        if max_size is not None and (i + 2) > max_size:  # +2 for PAD + UNK
            break
        vocab[tok] = i + 2  # 0 and 1 are reserved

    return vocab


def encode(
    text: str,
    vocab: dict,
    max_len: int,
    pad: bool = True,
) -> List[int]:
    """
    Tokenize text and convert to indices, with optional padding/truncation.

    Args:
        text: Raw text string.
        vocab: Token-to-index mapping.
        max_len: Maximum sequence length (truncate if longer).
        pad: Whether to pad to max_len.

    Returns:
        List of token indices.
    """
    tokens = tokenize(text)
    indices = [vocab.get(t, UNK_IDX) for t in tokens[:max_len]]

    if pad and len(indices) < max_len:
        indices += [PAD_IDX] * (max_len - len(indices))

    return indices


def vocab_size(vocab: dict) -> int:
    """Return the vocabulary size (number of unique tokens)."""
    return len(vocab)