"""Itemic-token (OpenOneRec-style) helpers for SLM-TIGER.

Items are quantized to 4-token semantic IDs (from our RQ-KMeans index) and rendered
as special tokens appended to a decoder-only LM's vocabulary:
    <s_a_{c0}><s_b_{c1}><s_c_{c2}><s_d_{c3}>
The LM is fine-tuned to GENERATE the next item's 4 SID tokens end-to-end.
"""
import torch
from transformers import LogitsProcessor


class ItemicTokenizer:
    """Adds 4 codebook-groups of SID special tokens to an HF tokenizer & maps codes<->token ids."""

    def __init__(self, base_tokenizer, num_codebooks=4, codebook_size=256):
        self._tok = base_tokenizer
        self._nc = num_codebooks
        self._cs = codebook_size
        self._base = None          # vocab len BEFORE adding (set in add_tokens)
        self._names = [f"<s_{chr(97 + l)}_{c}>" for l in range(num_codebooks) for c in range(codebook_size)]
        self._rating_names = [f"<rating_{i}>" for i in range(1, 6)]
        self._names.extend(self._rating_names)

    @property
    def base(self):
        return self._base

    @property
    def num_codebooks(self):
        return self._nc

    @property
    def codebook_size(self):
        return self._cs

    def add_tokens(self):
        """Register the SID special tokens; returns the updated tokenizer."""
        self._base = len(self._tok)
        self._tok.add_special_tokens({"additional_special_tokens": list(self._names)})
        return self._tok

    def new_vocab_size(self):
        return self._base + self._nc * self._cs + 5

    def sid_id(self, level, code):
        """Extended-vocab token id for a single (level, code)."""
        assert 0 <= level < self._nc and 0 <= code < self._cs
        return self._base + level * self._cs + code

    def sid_range(self, level):
        """[lo, hi) extended-vocab id range for a whole codebook level."""
        lo = self._base + level * self._cs
        return lo, lo + self._cs

    def rating_token_id(self, rating):
        """Extended-vocab token id for rating 1..5."""
        assert 1 <= rating <= 5
        return self._base + self._nc * self._cs + (rating - 1)

    def encode_codes(self, codes):
        """codes: (n, 4) raw per-level codes -> list of (n*4) extended token ids."""
        ids = []
        for row in codes:
            for l in range(self._nc):
                ids.append(self.sid_id(l, int(row[l])))
        return ids

    def decode_codes(self, ext_ids):
        """ext_ids: (4) extended SID token ids -> (4) raw per-level codes (composed=code_space*l+code)."""
        return [(int(t) - self._base) for t in ext_ids]

    def composed_prefix(self, ext_ids):
        """Map extended ids to the 'composed' 256*l+code form the SemanticMetric expects."""
        return [int(t) - self._base for t in ext_ids]


class SIDLogitsProcessor(LogitsProcessor):
    """Forces generation to stay inside valid SID token ids for each codebook position.

    Position p (0-based within the emitted sequence) maps to codebook level p % num_codebooks;
    at that position only that level's 256 token ids are allowed (others -> -inf).
    """

    def __init__(self, itemic: ItemicTokenizer):
        self._itemic = itemic

    def __call__(self, input_ids, scores):
        step = input_ids.shape[-1]                      # tokens emitted so far
        level = step % self._itemic.num_codebooks
        lo, hi = self._itemic.sid_range(level)
        mask = torch.ones_like(scores, dtype=torch.bool)
        mask[:, lo:hi] = False
        return scores.masked_fill(mask, float("-inf"))


class TrieLogitsProcessor(LogitsProcessor):
    """Extends SIDLogitsProcessor with a valid-item-prefix trie.

    Only *raw codes* that continue a valid item SID (from the index) are kept open,
    enforcing the beam stays inside the ~12k real item SIDs instead of 4.3 B possible 4-tuples.

    **Must receive `history_length` (number of tokens in the prompt/context) so it
    can distinguish generated SID tokens from history SID tokens.**  The first generated
    token corresponds to the first (level‑0) code of the predicted item, etc.
    """

    def __init__(self, itemic: ItemicTokenizer, index: dict, history_length: int):
        self._itemic = itemic
        self._hist_len = history_length
        self._level_fallback = SIDLogitsProcessor(itemic)
        trie: dict = {}
        for codes_str in index.values():
            codes = tuple(int(c) for c in codes_str)
            for lvl in range(itemic.num_codebooks):
                prefix = codes[:lvl]
                if prefix not in trie:
                    trie[prefix] = set()
                trie[prefix].add(codes[lvl])
        self._trie = trie
        _first = len(trie.get((), set()))
        print(f"[TrieLogitsProcessor] trie sizes: {len(trie)} raw-code prefixes, "
              f"{_first} valid first-level codes (total items={len(index)})", flush=True)

    def __call__(self, input_ids, scores):
        """Only look at the TAIL of the sequence (the generated part)."""
        itemic = self._itemic
        base = itemic.base
        n = itemic.num_codebooks
        cs = itemic.codebook_size
        # generated tokens are those at positions ≥ history_length
        gen = input_ids[0, self._hist_len:].tolist()
        raw = []
        for t in gen:
            if base <= t < base + n * cs:
                raw.append((t - base) % cs)
        prefix = tuple(raw)

        valid = self._trie.get(prefix)
        if valid and len(valid) > 0:
            level = len(prefix) % n
            lo, hi = itemic.sid_range(level)
            mask = torch.ones_like(scores, dtype=torch.bool)
            for c in valid:
                mask[:, lo + c] = False
            return scores.masked_fill(mask, float("-inf"))
        return self._level_fallback(input_ids, scores)