"""Find which branch produces NaN in train mode."""
import sys, numpy as np, torch
sys.path.insert(0, "d:/OneDrive/python-code/Public/recommendations/tenrec/CTR/scripts")
import config
config.NUM_WORKERS = 0
from dataset import load_stats, get_dataloader
from model import build_model, CTRModel

stats = load_stats()
model = CTRModel()
model.train()

dl = get_dataloader("train", stats, shuffle_files=True)
for i, batch in enumerate(dl):
    if i >= 5: break
    item_id = batch["item_id"]; hist = batch["hist"]
    cand_emb = model.item_emb(item_id)
    hist_emb = model.item_emb(hist)
    cat_emb = model.cat_emb(batch["video_category"])
    gender_emb = model.gender_emb(batch["gender"])
    age_emb = model.age_emb(batch["age"])
    eng = model.engagement(torch.stack([batch["follow"], batch["like"], batch["share"]], dim=1))
    watch = model.watch(batch["watching_times"].unsqueeze(1))
    interest = model.history_attn(cand_emb, hist_emb, batch["hist_mask"])
    print(f"step {i}: cand {torch.isfinite(cand_emb).all().item()} hist {torch.isfinite(hist_emb).all().item()} "
          f"cat {torch.isfinite(cat_emb).all().item()} gender {torch.isfinite(gender_emb).all().item()} "
          f"age {torch.isfinite(age_emb).all().item()} eng {torch.isfinite(eng).all().item()} "
          f"watch {torch.isfinite(watch).all().item()} interest {torch.isfinite(interest).all().item()}")
    if not torch.isfinite(interest).all():
        # find rows where interest is nan
        bad = ~torch.isfinite(interest).all(dim=1)
        print("  bad interest rows:", int(bad.sum()))
        print("  their hist_mask.all():", batch["hist_mask"][bad].all(dim=1).tolist()[:5])
        print("  their hist sample:", hist[bad][:3].tolist())
