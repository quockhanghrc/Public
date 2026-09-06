"""SLM-TIGER (OpenOneRec-style): LoRA fine-tune Qwen2.5-1.5B to generate next-item 4-token SIDs.

Items -> itemic tokens (SID special tokens added to the LM vocab); SFT = causal-LM loss on
the 4 target SID tokens; eval = constrained beam search -> Recall/NDCG SemanticMetric.

Self-contained (does NOT use the custom BatchProcessor/Trainer) so a 1.5B LoRA SFT is
straightforward and robust. Reads inter.json + index_rqkmeans_s512.json directly.
Run: python train_slm_tiger.py --params configs/expS1_slm_rqkmeans_s512.json
"""
import argparse
import itertools
import json
import os
import time
os.environ.setdefault("PYTORCH_CUDA_ALLOC_CONF", "expandable_segments:True")  # reduce T4 OOM from fragmentation
import numpy as np
import torch
from torch.utils.data import DataLoader, Dataset
from transformers import AutoModelForCausalLM, AutoTokenizer
from peft import LoraConfig, get_peft_model

from modeling.models.slm_tiger import ItemicTokenizer, SIDLogitsProcessor, TrieLogitsProcessor
from modeling.metric import NDCGSemanticMetric, RecallSemanticMetric
from modeling.utils import DEVICE, fix_random_seed


def load_config():
    ap = argparse.ArgumentParser()
    ap.add_argument("--params", required=True)
    args = ap.parse_args()
    with open(args.params) as f:
        return json.load(f)


class SftDataset(Dataset):
    """items: list of (history_codes, target_codes) both lists of 4-code lists."""

    def __init__(self, samples):
        self._samples = samples

    def __len__(self):
        return len(self._samples)

    def __getitem__(self, i):
        return self._samples[i]


def build_samples(inter, index, kind, max_len):
    """kind in {'train','val','test'}. Returns list of (history_codes[list], target_codes)."""
    samples = []
    for uid, items in inter.items():
        codes = [index[str(i)] for i in items]
        if kind == "train":
            for t in range(1, len(codes) - 2):          # predict items[1 .. len-3]
                history = codes[:t][-max_len:]
                samples.append((history, codes[t]))
        elif kind == "val":                             # GT = 2nd-last
            samples.append((codes[:-2][-max_len:], codes[-2]))
        else:                                           # test: GT = last
            samples.append((codes[:-1][-max_len:], codes[-1]))
    return samples


def collate(itemic, tok):
    def _collate(batch):
        # seq = [history itemic tokens] + [4 target SID tokens] ; right-pad to max_len.
        seqs = [itemic.encode_codes(h) + itemic.encode_codes([t]) for h, t in batch]
        max_len = max(len(s) for s in seqs)
        pad_id = tok.pad_token_id
        inp_ids, attn, labels = [], [], []
        for seq in seqs:
            pad = max_len - len(seq)
            L = len(seq)
            inp_ids.append(seq + [pad_id] * pad)
            attn.append([1] * L + [0] * pad)
            # HF CausalLM does shift_logits=logits[...,:-1], shift_labels=labels[...,1:],
            # so labels must equal input_ids on targets (mask history/pad with -100).
            # Logit at L-5 (last history) then predicts seq[L-4] (first SID), etc.
            lab = [-100] * (L - 4) + seq[L - 4:L]
            labels.append(lab + [-100] * pad)
        return {
            "input_ids": torch.tensor(inp_ids, dtype=torch.long),
            "attention_mask": torch.tensor(attn, dtype=torch.long),
            "labels": torch.tensor(labels, dtype=torch.long),
        }
    return _collate


def main():
    config = load_config()
    fix_random_seed(42)

    dataset = config["dataset"]
    ds = config.get("dataloader", {})
    mcfg = config["model"]
    lcfg = config.get("lora", {})
    num_codebooks = dataset.get("num_codebooks", 4)
    codebook_size = mcfg.get("codebook_size", 256)
    max_len = dataset.get("max_sequence_length", 20)
    num_beams = mcfg.get("num_beams", 50)
    top_k = mcfg.get("top_k", 20)
    batch_size = ds.get("train_batch_size", 8)
    lr = config["optimizer"]["lr"]
    train_steps = config.get("train_steps_num")
    eval_subset = config.get("eval_subset", 600)
    early_stop_period = config.get("early_stop_period", 2000)
    early_stop_patience = config.get("early_stop_patience", 3)
    early_stop_es_check = config.get("early_stop_check", 64)

    with open(dataset["index_json_path"]) as f:
        index = json.load(f)
    with open(dataset["inter_json_path"]) as f:
        inter = json.load(f)

    # ---- itemic tokenizer + model (fp16) + LoRA ----
    tok = AutoTokenizer.from_pretrained(config["slm_id"])
    if tok.pad_token is None:
        tok.pad_token = tok.eos_token
    itemic = ItemicTokenizer(tok, num_codebooks, codebook_size)
    itemic.add_tokens()

    model = AutoModelForCausalLM.from_pretrained(
        config["slm_id"], torch_dtype=torch.float16,   # MUST be fp16: fp32 1.5B + 152k-vocab logits blows a 14.5GiB T4
    ).to(DEVICE)
    model.resize_token_embeddings(itemic.new_vocab_size())
    # The new SID rows MUST be initialized to small RANDOM values (not the mean of
    # existing rows). Mean-init makes all 1024 rows identical => softmax over a
    # 152,689-way vocab with 1024 tied input/lm_head rows produces ~uniform
    # distribution => loss = log(152689) ≈ 11.94 and gradients through softmax are
    # ~0 everywhere, so the model cannot learn. Small random init breaks the tie
    # and lets SFT actually move probability mass onto the correct SID codes.
    with torch.no_grad():
        torch.manual_seed(42)
        e_in  = model.get_input_embeddings().weight
        e_out = model.get_output_embeddings().weight
        # std of existing rows (small, since LM-head rows have low norm)
        std_in  = e_in[:itemic.base].std().item()  or 1e-3
        std_out = e_out[:itemic.base].std().item() or 1e-3
        e_in[itemic.base:]  = torch.randn(itemic._nc * itemic._cs, e_in.shape[1],  device=e_in.device,  dtype=e_in.dtype)  * std_in
        e_out[itemic.base:] = torch.randn(itemic._nc * itemic._cs, e_out.shape[1], device=e_out.device, dtype=e_out.dtype) * std_out
    # Train embed_tokens + lm_head (needed for the 1024 new SID rows) + LoRA on
    # attention projections. NOTE: get_peft_model() freezes ALL base weights, so
    # any requires_grad=True set BEFORE it is wiped. Per-row unfreeze is impossible
    # (requires_grad is per-Tensor), so we keep the whole embedding/LM-head trainable
    # via modules_to_save AND re-enable grad AFTER wrapping. Optimizer is created
    # after this, so it picks these params up.
    lora = LoraConfig(
        r=lcfg.get("r", 16), lora_alpha=lcfg.get("alpha", 32),
        lora_dropout=lcfg.get("dropout", 0.05),
        target_modules=["q_proj", "k_proj", "v_proj", "o_proj"],
        modules_to_save=["embed_tokens", "lm_head"],
        task_type="CAUSAL_LM",
    )
    model = get_peft_model(model, lora)
    # Must be AFTER get_peft_model (which resets requires_grad=False on base):
    try:
        model.get_input_embeddings().weight.requires_grad = True
    except Exception:
        model.get_base_model().get_input_embeddings().weight.requires_grad = True
    try:
        model.get_output_embeddings().weight.requires_grad = True
    except Exception:
        model.get_base_model().get_output_embeddings().weight.requires_grad = True
    model.print_trainable_parameters()

    # ---- data ----
    train_samples = build_samples(inter, index, "train", max_len)
    val_samples = build_samples(inter, index, "val", max_len)
    test_samples = build_samples(inter, index, "test", max_len)
    train_loader = DataLoader(SftDataset(train_samples), batch_size=batch_size, shuffle=True,
                              collate_fn=collate(itemic, tok), drop_last=True)
    print(f"samples: train={len(train_samples)} val={len(val_samples)} test={len(test_samples)}", flush=True)

    # ---- optimizer ----
    opt = torch.optim.AdamW([p for p in model.parameters() if p.requires_grad], lr=lr)

    # ---- metrics ----
    ndcg5, ndcg10, ndcg20 = NDCGSemanticMetric(5, codebook_size, num_codebooks), NDCGSemanticMetric(10, codebook_size, num_codebooks), NDCGSemanticMetric(20, codebook_size, num_codebooks)
    rec5, rec10, rec20 = RecallSemanticMetric(5, codebook_size, num_codebooks), RecallSemanticMetric(10, codebook_size, num_codebooks), RecallSemanticMetric(20, codebook_size, num_codebooks)
    metrics = [("ndcg@5", ndcg5), ("ndcg@10", ndcg10), ("ndcg@20", ndcg20),
               ("recall@5", rec5), ("recall@10", rec10), ("recall@20", rec20)]

    def evaluate(prefix, samples, n):
        model.eval()
        agg = {k: 0.0 for k, _ in metrics}
        sub = samples[:n]
        # The SemanticMetric(s) require predictions shape (batch, k_max, sid_len) where
        # k_max=20 (max k in our metrics). Pad with -1 (a code that will never match any
        # real SID) when the beam returns fewer candidates.
        k_max = 20
        pad_code = -1
        with torch.no_grad():
            for history, target in sub:
                hid = torch.tensor([itemic.encode_codes(history)], dtype=torch.long, device=DEVICE)
                am = torch.ones_like(hid)
                out = model.generate(
                    input_ids=hid, attention_mask=am,
                    max_new_tokens=num_codebooks, min_new_tokens=num_codebooks,
                    num_beams=min(num_beams, top_k), num_return_sequences=top_k, do_sample=False,
                    logits_processor=[TrieLogitsProcessor(itemic, index, history_length=hid.shape[1])],
                    eos_token_id=tok.eos_token_id, pad_token_id=tok.pad_token_id,
                )
                rows = out[:, hid.shape[1]:].tolist()
                pred = []
                for row in rows:
                    tail = row[-num_codebooks:]
                    if len(tail) < num_codebooks:
                        tail = tail + [itemic.base - 1] * (num_codebooks - len(tail))
                    pred.append(tail)  # (k, 4) ext ids
                composed = [itemic.composed_prefix(p) for p in pred]                     # (k,4) 256*l+code
                # right-pad to k_max candidates with pad_code so the metric can slice
                if len(composed) < k_max:
                    pad = [[pad_code] * num_codebooks] * (k_max - len(composed))
                    composed = composed + pad
                else:
                    composed = composed[:k_max]
                inputs = {
                    "predictions": torch.tensor([composed], dtype=torch.long),
                    "semantic_labels.ids": torch.tensor([target], dtype=torch.long),
                }
                for name, m in metrics:
                    agg[name] += float(m(inputs, "predictions", "labels")[0])
        n = len(sub)
        line = " ".join(f"{name} {agg[name]/n:.6f}" for name, _ in metrics)
        print(f"[eval:{prefix}] ({n} users) " + line, flush=True)

    # ---- training ----
    step = 0
    epoch = 0
    sw = time.time()
    best_val, no_improve, stopped = float("inf"), 0, False

    def val_loss(n):
        model.eval()
        batch = collate(itemic, tok)(val_samples[:n])
        b = {k: v.to(DEVICE) for k, v in batch.items()}
        with torch.no_grad():
            return model(**b).loss.item()

    while train_steps is None or step < train_steps:
        model.train()
        for batch in train_loader:
            b = {k: v.to(DEVICE) for k, v in batch.items()}
            out = model(**b)
            loss = out.loss
            opt.zero_grad(); loss.backward()
            torch.nn.utils.clip_grad_norm_([p for p in model.parameters() if p.requires_grad], 1.0)
            opt.step()
            step += 1
            if step % 50 == 0:
                print(f"[slm] epoch {epoch} step {step}/{train_steps} loss {loss.item():.4f} | {time.time()-sw:.0f}s", flush=True)
            if step % early_stop_period == 0:
                vl = val_loss(early_stop_es_check)
                if vl < best_val - 1e-4:
                    best_val, no_improve = vl, 0
                else:
                    no_improve += 1
                print(f"[es] step {step} val_loss {vl:.4f} best {best_val:.4f} no_improve {no_improve}/{early_stop_patience}", flush=True)
                if no_improve >= early_stop_patience:
                    print("EARLY STOP triggered", flush=True)
                    stopped = True
                    break
            if train_steps is not None and step >= train_steps:
                break
        if stopped:
            break
        epoch += 1

    # ---- final eval ----
    print("Saving model...", flush=True)
    model.save_pretrained(f"checkpoints/{config['experiment_name']}_lora")
    print("Final evaluation...", flush=True)
    evaluate("val", val_samples, eval_subset)          # quick sanity on val subset
    evaluate("test", test_samples, eval_subset)


if __name__ == "__main__":
    main()