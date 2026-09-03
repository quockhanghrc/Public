"""RQ-VAE tokenization for Sentence-T5 S512 embeddings -> data/index_rqvae_s512.json.

Port of ref/nonameuntitled_tiger/notebooks/RQVaePipeline.ipynb adapted to our
embeddings (Sentence-T5, 768-dim => input_dim=768, not the reference's 4096).
3 codebooks x 256 + 1 collision-solver code => 4-token SID per item.
Runs from tiger/ cwd (so paths are relative to the mounted volume APP_DIR).
"""
import json
import os
from collections import defaultdict

import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader
from torch.utils.data import Dataset

DEVICE = torch.device("cuda") if torch.cuda.is_available() else torch.device("cpu")
EMB_PATH = "data/content_embeddings_s512.pkl"
OUT_PATH = "data/index_rqvae_s512.json"
CKPT_PATH = "data/rqvae_s512.pt"

INPUT_DIM = 768     # Sentence-T5 dim (NOT 4096)
HIDDEN_DIM = 32
CODEBOOK_SIZE = 256
NUM_CODEBOOKS = 3
BETA = 0.25
LR = 3e-4
NUM_EPOCHS = 10
BATCH_SIZE = 256
SEED = 42


class EmbDataset(Dataset):
    def __init__(self, data):
        self.ids = data["item_id"]
        self.embs = data["embedding"]

    def __len__(self):
        return len(self.ids)

    def __getitem__(self, idx):
        return {"item_id": self.ids[idx], "embedding": self.embs[idx]}


def collate(samples):
    return {
        "item_id": torch.LongTensor([s["item_id"] for s in samples]),
        "embedding": torch.FloatTensor(np.stack([s["embedding"] for s in samples])).to(DEVICE),
    }


class RQVAE(nn.Module):
    def __init__(self, input_dim, hidden_dim, beta, codebook_sizes):
        super().__init__()
        self.register_buffer("beta", torch.tensor(beta))
        self.mse_loss = nn.MSELoss()
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
        model.codebooks[i].data = rem[: model.codebooks[i].shape[0]].detach()


def main():
    torch.manual_seed(SEED)
    np.random.seed(SEED)
    if torch.cuda.is_available():
        torch.cuda.manual_seed_all(SEED)

    with open(EMB_PATH, "rb") as f:
        data = pickle_load(f)
    ds = EmbDataset(data)
    train_loader = DataLoader(ds, batch_size=BATCH_SIZE, shuffle=True, drop_last=True, collate_fn=collate)
    full_loader = DataLoader(ds, batch_size=BATCH_SIZE, shuffle=False, drop_last=False, collate_fn=collate)

    model = RQVAE(INPUT_DIM, HIDDEN_DIM, BETA, [CODEBOOK_SIZE] * NUM_CODEBOOKS).to(DEVICE)
    init_codebooks(model, train_loader)
    opt = torch.optim.Adam(model.parameters(), lr=LR)

    step = 0
    for epoch in range(NUM_EPOCHS):
        model.train()
        for batch in train_loader:
            opt.zero_grad()
            out = model(batch)
            out["loss"].backward()
            nn.utils.clip_grad_norm_(model.parameters(), 1.0)
            opt.step()
            step += 1
            if step % 10 == 0:
                print(f"[rqvae] epoch {epoch} step {step} recon_loss {out['metrics']['recon_loss']:.5f}", flush=True)

    # save weights
    torch.save(model.state_dict(), CKPT_PATH)
    print(f"saved rqvae weights -> {CKPT_PATH}")

    # index generation
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
        assert len(ids) <= CODEBOOK_SIZE, f"collision group too large: {len(ids)}"
        solvers = np.random.permutation(CODEBOOK_SIZE)[:len(ids)].tolist()
        for item_id, sv in zip(ids, solvers):
            inter[item_id].append(sv)
    with open(OUT_PATH, "w") as f:
        json.dump(inter, f)
    lens = set(len(v) for v in inter.values())
    print(f"wrote {OUT_PATH}: {len(inter)} items, SID lengths {lens}")


# local import helper (pickle)
def pickle_load(f):
    import pickle
    return pickle.load(f)


if __name__ == "__main__":
    main()