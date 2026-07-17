"""Track finiteness of fused/mlp/weights through a 30-step training loop."""
import sys, numpy as np, torch
sys.path.insert(0, "d:/OneDrive/python-code/Public/recommendations/tenrec/CTR/scripts")
import config
config.NUM_WORKERS = 0
from dataset import load_stats, get_dataloader
from model import build_model
from train import FocalLoss

stats = load_stats()
model = build_model("cpu")
model.train()
opt = torch.optim.AdamW(model.parameters(), lr=1e-3)
crit = FocalLoss(pos_weight=(1-stats['pos_rate'])/stats['pos_rate'])

dl = get_dataloader("train", stats, shuffle_files=True)
for i, batch in enumerate(dl):
    if i >= 30: break
    labels = batch["click"]
    feats = {k: v for k, v in batch.items() if k != "click"}
    logits = model(feats)
    loss = crit(logits, labels)
    opt.zero_grad(); loss.backward(); opt.step()
    # check
    fused_finite = torch.isfinite(model._last_fused).all().item() if hasattr(model, "_last_fused") else "n/a"
    w = model.head.weight
    print(f"step {i:2d} loss={loss.item():.4f} logits_finite={torch.isfinite(logits).all().item()} "
          f"head_w_finite={torch.isfinite(w).all().item()} grad_norm={torch.nn.utils.clip_grad_norm_(model.parameters(), float('inf')).item():.1f}")
