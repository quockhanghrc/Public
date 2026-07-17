"""Diagnostic: find why val/test logits are NaN in eval mode but not train."""
import sys, numpy as np, torch
sys.path.insert(0, "d:/OneDrive/python-code/Public/recommendations/tenrec/CTR/scripts")
import config
config.NUM_WORKERS = 0  # avoid Windows spawn re-import issues
from dataset import load_stats, get_dataloader
from model import build_model

stats = load_stats()
model = build_model("cpu")

def check_running_stats(tag):
    bad = []
    for name, m in model.named_modules():
        for bname in ("running_mean", "running_var"):
            if hasattr(m, bname):
                t = getattr(m, bname)
                if not torch.isfinite(t).all():
                    bad.append(f"{name}.{bname} -> {t}")
    print(f"[{tag}] non-finite running stats: {bad if bad else 'NONE'}")

def run_eval(split, n=20):
    dl = get_dataloader(split, stats)
    model.eval()
    nan_rows = 0; tot = 0; any_nan = False
    with torch.no_grad():
        for i, batch in enumerate(dl):
            if i >= n: break
            feats = {k: v for k, v in batch.items() if k != "click"}
            out = model(feats)
            if not torch.isfinite(out).all():
                any_nan = True
                nan_rows += 1
            tot += 1
    print(f"[{split} eval] nan_batches={nan_rows}/{tot} any_nan={any_nan}")

if __name__ == "__main__":
    print("=== BEFORE TRAINING ===")
    check_running_stats("init")
    run_eval("val", n=20)

# Train a few steps
print("\n=== TRAIN 30 STEPS ===")
dl = get_dataloader("train", stats, shuffle_files=True)
model.train()
opt = torch.optim.AdamW(model.parameters(), lr=1e-3)
from train import FocalLoss
crit = FocalLoss(pos_weight=(1-stats['pos_rate'])/stats['pos_rate'])
for i, batch in enumerate(dl):
    if i >= 30: break
    labels = batch["click"]
    feats = {k: v for k, v in batch.items() if k != "click"}
    logits = model(feats)
    loss = crit(logits, labels)
    opt.zero_grad(); loss.backward(); opt.step()

check_running_stats("after 30 steps")
run_eval("val", n=20)
run_eval("test", n=20)
