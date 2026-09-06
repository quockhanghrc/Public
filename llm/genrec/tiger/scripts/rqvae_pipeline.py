"""RQ-VAE tokenization for Sentence-T5 S512 embeddings -> data/index_rqvae_s512.json.

Port of ref/nonameuntitled_tiger/notebooks/RQVaePipeline.ipynb adapted to our
embeddings (Sentence-T5, 768-dim => input_dim=768, not the reference's 4096).
3 codebooks x 256 + 1 collision-solver code => 4-token SID per item.

ENHANCEMENT (collapse fix, faithful to reference):
  * FixDeadCentroids dead-codebook reset (was MISSING in the original port -> the
    reason coverage fell to 4-13%). Re-inits unused codebook rows from encoder outputs.
  * Optional EMA codebook updates (--ema-decay>0 freezes codebooks and EMA-updates them).
  * Defaults raised: hidden_dim 64 (was 32), epochs 100 (was 10).

All knobs are CLI args (R1):
  --emb --out --state --epochs --beta --hidden --lr --codebook-size --num-codebooks
  --batch-size --seed --reset-period --ema-decay --log-every
Runs from tiger/ cwd (paths relative to the mounted volume APP_DIR).
"""
import argparse
import json
import os
from collections import defaultdict

import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader
from torch.utils.data import Dataset

DEVICE = torch.device("cuda") if torch.cuda.is_available() else torch.device("cpu")


class EmbDataset(Dataset):
    def __init__(self, data):
        self.ids = data["item_id"]
        self.embs = data["embedding"]

    def __len__(self):
        return len(self.ids)

    def __getitem__(self, idx):
        return {"item_id": self.ids[idx], "embedding": self.embs[idx]}


def make_collate(device):
    def collate(samples):
        return {
            "item_id": torch.LongTensor([s["item_id"] for s in samples]),
            "embedding": torch.FloatTensor(np.stack([s["embedding"] for s in samples])).to(device),
        }
    return collate


class RQVAE(nn.Module):
    def __init__(self, input_dim, hidden_dim, beta, codebook_sizes):
        super().__init__()
        self.register_buffer("beta", torch.tensor(beta))
        self.encoder = self._tower(input_dim, hidden_dim)
        self.decoder = self._tower(hidden_dim, input_dim)
        self.codebook_sizes = codebook_sizes
        self.codebooks = nn.ParameterList()
        for size in codebook_sizes:
            cb = torch.FloatTensor(size, hidden_dim)
            with torch.no_grad():
                nn.init.trunc_normal_(cb, std=0.02, a=-0.04, b=0.04)
            self.codebooks.append(cb)

    @staticmethod
    def _tower(d1, d2, bias=False):
        return nn.Sequential(nn.Linear(d1, d1), nn.ReLU(), nn.Linear(d1, d2), nn.ReLU(), nn.Linear(d2, d2, bias=bias))

    @staticmethod
    def _indices(remainder, codebook):
        return torch.cdist(remainder, codebook).argmin(dim=-1)

    def forward(self, inputs):
        latent = self.encoder(inputs["embedding"])
        latent_restored = 0
        rqvae_loss = 0
        clusters = []
        remainder = latent
        for cb in self.codebooks:
            idx = self._indices(remainder, cb)
            clusters.append(idx)
            quantized = cb[idx]
            codebook_vectors = remainder + (quantized - remainder).detach()
            rqvae_loss += self.beta * nn.functional.mse_loss(remainder, quantized.detach())
            rqvae_loss += nn.functional.mse_loss(quantized, remainder.detach())
            latent_restored += codebook_vectors
            remainder = remainder - codebook_vectors
        emb_hat = self.decoder(latent_restored)
        recon = nn.functional.mse_loss(emb_hat, inputs["embedding"])
        total = (recon + rqvae_loss).mean()
        return {
            "loss": total,
            "clusters": torch.stack(clusters).T,       # (N, num_codebooks)
            "embedding_hat": emb_hat,
            "metrics": dict(loss=total.detach(), recon_loss=recon.mean().item(), rqvae_loss=rqvae_loss.detach()),
        }


@torch.no_grad()
def init_codebooks(model, loader):
    it = iter(loader)
    for i in range(len(model.codebooks)):
        batch = next(it)["embedding"]
        rem = model.encoder(batch)
        for j in range(i):
            idx = model._indices(rem, model.codebooks[j])
            rem = rem - model.codebooks[j][idx]
        # reference-style: random subset of a batch's encoder anchors
        perm = torch.randperm(rem.shape[0], device=rem.device)[: model.codebooks[i].shape[0]]
        model.codebooks[i].data = rem[perm].detach()


@torch.no_grad()
def collect_usage_and_anchors(model, loader):
    """One pass over the full set: per-codebook usage counts + anchor remainder per codebook."""
    counts = [torch.zeros(cb.shape[0], dtype=torch.long, device=DEVICE) for cb in model.codebooks]
    anchors = [[] for _ in model.codebooks]   # anchor = remainder input to each codebook before lookup
    for batch in loader:
        x = batch["embedding"]
        rem = model.encoder(x)
        for l, cb in enumerate(model.codebooks):
            idx = model._indices(rem, cb)            # (n,)
            anchors[l].append(rem.detach())
            counts[l].scatter_add_(0, idx, torch.ones_like(idx))
            q = cb[idx]
            codebook_vectors = rem + (q - rem).detach()
            rem = rem - codebook_vectors             # walk the residual for the next level
    counts = [c.cpu().numpy() for c in counts]
    anchors = [torch.cat(a, dim=0).cpu() for a in anchors]
    return counts, anchors


def fix_dead_centroids(model, counts, anchors):
    """Re-init unused codebook rows from random example anchors (FixDeadCentroids)."""
    fixed = []
    for l, cb in enumerate(model.codebooks):
        dead = counts[l] == 0
        n = int(dead.sum())
        fixed.append(n)
        if n == 0:
            continue
        pool = anchors[l]
        sel = pool[torch.randperm(pool.shape[0])[:n]].to(DEVICE)
        with torch.no_grad():
            cb.data[dead] = sel
    return fixed


def ema_update_codebooks(model, codes, anchors, decay):
    """EMA codebook update (VQ-VAE-2 style) using per-example codes+anchors. Codebooks frozen."""
    with torch.no_grad():
        for l, cb in enumerate(model.codebooks):
            idx = torch.LongTensor(codes[:, l]).to(DEVICE)
            anchor_l = anchors[l].to(DEVICE)
            # per-codebook EMA mean via counts
            counts = torch.zeros(cb.shape[0], device=DEVICE)
            counts.scatter_add_(0, idx, torch.ones_like(idx, dtype=torch.float))
            sums = torch.zeros_like(cb)
            sums.scatter_add_(0, idx[:, None].expand(-1, cb.shape[1]), anchor_l)
            denom = counts.unsqueeze(1).clamp(min=1)
            new = sums / denom
            cb.data.mul_(decay).add_(new, alpha=1 - decay)


def main():
    def get(name, default):
        return getattr(args, name, default)

    p = argparse.ArgumentParser()
    p.add_argument("--emb", default="data/content_embeddings_s512.pkl")
    p.add_argument("--input-dim", type=int, default=768)
    p.add_argument("--out", default="data/index_rqvae_s512.json")
    p.add_argument("--state", default="data/rqvae_s512.pt")
    p.add_argument("--epochs", type=int, default=100)
    p.add_argument("--beta", type=float, default=0.25)
    p.add_argument("--hidden", type=int, default=64)
    p.add_argument("--lr", type=float, default=3e-4)
    p.add_argument("--codebook-size", type=int, default=256)
    p.add_argument("--num-codebooks", type=int, default=3)
    p.add_argument("--batch-size", type=int, default=256)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--reset-period", type=int, default=1, help="epochs between dead-centroid resets; 0 disables")
    p.add_argument("--ema-decay", type=float, default=0.0, help=">0 enables frozen-EMA codebook update")
    p.add_argument("--log-every", type=int, default=10)
    args = p.parse_args()

    torch.manual_seed(args.seed)
    np.random.seed(args.seed)
    torch.set_float32_matmul_precision("high")
    if torch.cuda.is_available():
        torch.cuda.manual_seed(args.seed)
        torch.backends.cuda.matmul.allow_tf32 = True
        torch.backends.cudnn.allow_tf32 = True

    with open(args.emb, "rb") as f:
        import pickle
        data = pickle.load(f)
    ds = EmbDataset(data)
    collate = make_collate(DEVICE)
    train_loader = DataLoader(ds, batch_size=args.batch_size, shuffle=True, drop_last=True, collate_fn=collate)
    full_loader = DataLoader(ds, batch_size=args.batch_size, shuffle=False, drop_last=False, collate_fn=collate)

    model = RQVAE(args.input_dim, args.hidden, args.beta, [args.codebook_size] * args.num_codebooks).to(DEVICE)
    if args.ema_decay > 0:
        # freeze codebooks -> EMA owns them
        for cb in model.codebooks:
            cb.requires_grad_(False)
    init_codebooks(model, train_loader)
    opt = torch.optim.Adam([p for p in model.parameters() if p.requires_grad], lr=args.lr)

    step = 0
    for epoch in range(args.epochs):
        model.train()
        for batch in train_loader:
            opt.zero_grad()
            out = model(batch)
            out["loss"].backward()
            nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            opt.step()
            step += 1
            if step % args.log_every == 0:
                print(f"[rqvae] epoch {epoch} step {step} recon_loss {out['metrics']['recon_loss']:.5f}", flush=True)

        # per-epoch collect: usage counts + anchors
        counts, anchors = collect_usage_and_anchors(model, full_loader)
        dead_tot = 0
        if args.reset_period > 0 and (epoch + 1) % args.reset_period == 0:
            dead_tot = sum(fix_dead_centroids(model, counts, anchors))
        if args.ema_decay > 0:
            ema_update_codebooks(model, _codes_for(model, anchors), anchors, args.ema_decay)
        print(f"[rqvae] epoch {epoch} done: dead_fixed={dead_tot}", flush=True)

    torch.save(model.state_dict(), args.state)
    print(f"saved rqvae weights -> {args.state}")

    # index generation (identical to before)
    inter = {}
    sem2 = defaultdict(list)
    model.eval()
    with torch.inference_mode():
        for batch in full_loader:
            out = model(batch)
            for item_id, sids in zip(batch["item_id"].tolist(), out["clusters"].cpu().tolist()):
                inter[item_id] = sids
                sem2[tuple(sids)].append(item_id)
    for sem, ids in sem2.items():
        assert len(ids) <= args.codebook_size, f"collision group too large: {len(ids)}"
        solvers = np.random.permutation(args.codebook_size)[: len(ids)].tolist()
        for item_id, sv in zip(ids, solvers):
            inter[item_id].append(sv)
    with open(args.out, "w") as f:
        json.dump(inter, f)
    lens = set(len(v) for v in inter.values())
    print(f"wrote {args.out}: {len(inter)} items, SID lengths {lens}")


@torch.no_grad()
def _codes_for(model, anchors):
    cols = []
    for l, cb in enumerate(model.codebooks):
        cols.append(model._indices(anchors[l].to(DEVICE), cb).cpu())
    return torch.stack(cols, dim=1).numpy()


if __name__ == "__main__":
    main()