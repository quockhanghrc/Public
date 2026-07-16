"""
Data Parallelism demo - CPU only, NO torchrun / NO gloo needed.

This is the SAME data-parallelism concept as ddp_train.py, but implemented with
a *manual* gradient all-reduce so it runs on any machine (including Windows CPU,
where PyTorch's gloo/nccl backends are often broken). It is fully GPU-ready:
flip USE_CUDA and the math is identical, just on tensors placed on cuda devices.

WHY THIS AVOIDS "1 BUSY, REST FREE":
We split the dataset into N disjoint shards. Each process trains on its OWN
shard (so all ranks are busy at the same time on different data), computes
gradients locally, and then we AVERAGE the gradients across all processes (the
all-reduce step). Every replica then applies the same averaged update ->
identical to DistributedDataParallel, but you can see exactly how it works.

HOW TO RUN (CPU, 2 processes via multiprocessing):
    python ddp_spawn.py --nproc 2

HOW TO RUN (single process, quick demo):
    python ddp_spawn.py --nproc 1

To use GPUs instead: set USE_CUDA = True below (requires a CUDA build of torch).
"""

import argparse
import os
import tempfile
import numpy as np
import torch
import torch.multiprocessing as mp
import torch.nn as nn
from torch.utils.data import DataLoader, Dataset


# ---- GPU readiness -------------------------------------------------------
# Set to True to run on CUDA GPUs (needs a CUDA torch build). The training
# logic below is device-agnostic, so nothing else changes.
USE_CUDA = False


class ToyDataset(Dataset):
    """A trivial dataset: y = 2*x1 + 3*x2 + 1 + noise. We fit a linear model."""

    def __init__(self, n_samples: int = 100000, seed: int = 0):
        g = torch.Generator().manual_seed(seed)
        self.x = torch.randn(n_samples, 2, generator=g)
        self.y = (2.0 * self.x[:, 0] + 3.0 * self.x[:, 1] + 1.0
                  + 0.01 * torch.randn(n_samples, generator=g))

    def __len__(self):
        return len(self.x)

    def __getitem__(self, idx):
        return self.x[idx], self.y[idx]


class LinearModel(nn.Module):
    def __init__(self):
        super().__init__()
        self.linear = nn.Linear(2, 1)

    def forward(self, x):
        return self.linear(x).squeeze(-1)


def fmt(t):
    """Compact one-line formatting of a tensor/array for logging."""
    return np.array(t.detach().cpu() if hasattr(t, "detach") else t).ravel().round(4)


def manual_allreduce_gradients(model, rank, world_size, tmpdir, step):
    """Average every parameter's .grad across all ranks via local files.

    This is the core of data parallelism: each rank computed grads on its OWN
    data shard; averaging them is mathematically equivalent to training on the
    combined batch. (DDP does this with a fast all-reduce; here we use temp
    files so it works without gloo/nccl and without fragile shared-memory
    proxies that misbehave on Windows.)

    Synchronization uses STEP-SPECIFIC files: each rank writes its grads to
    g_{rank}_{step}.npy (atomic: tmp -> rename) and a ready_{rank}_{step} marker,
    then waits until every rank's marker for THIS step exists. Keying by step
    means a fast rank can never read a slow rank's in-flight or future grads.

    Returns the local (pre-sync) and averaged (post-sync) gradient of the
    weight matrix, so the caller can PROVE the all-reduce actually happened.
    """
    if world_size == 1:
        return None, None

    # 1) Publish grads (atomic write) + raise this rank's ready marker.
    grads = [p.grad.detach().cpu().numpy() for p in model.parameters()
             if p.grad is not None]
    local_w_grad = grads[0].copy()  # weight grad before averaging
    tmp = os.path.join(tmpdir, f"g_{rank}_{step}.tmp.npy")
    final = os.path.join(tmpdir, f"g_{rank}_{step}.npy")
    # Save the list of gradient arrays as a single object entry.
    buf = np.empty((), dtype=object)
    buf[()] = grads
    np.save(tmp, buf, allow_pickle=True)
    os.replace(tmp, final)
    with open(os.path.join(tmpdir, f"ready_{rank}_{step}.txt"), "w") as f:
        f.write("1")

    # 2) Wait until every rank has published its gradients for THIS step.
    while not all(os.path.exists(os.path.join(tmpdir, f"ready_{r}_{step}.txt"))
                  for r in range(world_size)):
        pass

    # 3) Average across ranks and write the result back into each .grad.
    all_grads = [np.load(os.path.join(tmpdir, f"g_{r}_{step}.npy"), allow_pickle=True)[()]
                 for r in range(world_size)]
    for i, p in enumerate(model.parameters()):
        if p.grad is None:
            continue
        stacked = torch.stack(
            [torch.as_tensor(all_grads[r][i]) for r in range(world_size)], dim=0)
        p.grad.copy_(stacked.mean(dim=0).to(p.grad.device))

    avg_w_grad = next(model.parameters()).grad.detach().cpu().numpy().copy()
    return local_w_grad, avg_w_grad


def worker(rank, world_size, epochs, tmpdir):
    device = torch.device("cuda", rank) if USE_CUDA else torch.device("cpu")

    # Seed identically on every rank so all replicas start from the SAME
    # weights (exactly what DistributedDataParallel does via broadcast at init).
    torch.manual_seed(42)
    np.random.seed(42)

    # ---- Model ----------------------------------------------------------
    model = LinearModel().to(device)
    optimizer = torch.optim.SGD(model.parameters(), lr=0.1)
    loss_fn = nn.MSELoss()

    # ---- Data: each rank gets its OWN disjoint shard --------------------
    # To make DP provably correct (all ranks bit-identical, like real DDP), ALL
    # ranks must process the SAME batch at the SAME step. We build one global
    # shuffled index order (same seed on every rank), split into batches, and
    # each rank iterates batches in the SAME global order, contributing only its
    # strided slice of samples within each batch. So at step t every rank sees
    # batch t (with its own samples); after the gradient all-reduce every rank
    # applies the IDENTICAL averaged update -> weights stay bit-equal.
    full = ToyDataset()
    g = torch.Generator().manual_seed(1234)
    perm = torch.randperm(len(full), generator=g).tolist()
    batch_size = 64*2
    n_batches_total = len(perm) // batch_size
    # Precompute, for each global batch, the sample indices this rank owns.
    # Rank r owns samples [r::world_size] inside batch b.
    my_batches = []
    for b in range(n_batches_total):
        base = b * batch_size
        my_idx = [perm[base + j] for j in range(rank, batch_size, world_size)]
        my_batches.append(my_idx)
    # Loader yields one batch per global step, in order; each batch already
    # contains only THIS rank's samples for that step.
    loader = DataLoader(
        torch.utils.data.Subset(full, [i for batch in my_batches for i in batch]),
        batch_size=len(my_batches[0]), shuffle=False)

    # ---- DP PROOF (only on the first step of epoch 0) ------------------
    # We demonstrate that: (a) each rank sees DIFFERENT data,
    # (b) each rank computes a DIFFERENT local gradient,
    # (c) after all-reduce every rank holds the SAME averaged gradient,
    # (d) therefore all ranks apply the SAME weight update.
    proved = False
    step = 0

    for epoch in range(epochs):
        model.train()
        total_loss, n_batches = 0.0, 0
        for x, y in loader:
            x, y = x.to(device), y.to(device)
            optimizer.zero_grad()
            pred = model(x)
            loss = loss_fn(pred, y)
            loss.backward()           # local grad on THIS rank's shard
            local_g, avg_g = manual_allreduce_gradients(
                model, rank, world_size, tmpdir, step)  # avg
            optimizer.step()          # every rank applies the SAME update
            total_loss += loss.item()
            n_batches += 1
            step += 1

            # Emit the proof once, from every rank, on the very first step.
            if not proved:
                proved = True
                if world_size > 1:
                    n_my = sum(len(b) for b in my_batches)
                    print(f"[rank {rank}] owns {n_my} samples "
                          f"({len(my_batches)} batches x {len(my_batches[0])}/batch)")
                    print(f"[rank {rank}] LOCAL  weight-grad (before sync) = {fmt(local_g)}")
                    print(f"[rank {rank}] SYNCED weight-grad (after all-reduce) = {fmt(avg_g)}")
                else:
                    print(f"[rank {rank}] single process: shard size = {len(full)} "
                          f"(no all-reduce needed)")

        avg_loss = total_loss / n_batches
        if rank == 0:
            print(f"[rank {rank}/{world_size}] epoch {epoch+1}/{epochs} "
                  f"loss={avg_loss:.4f}")

    # ---- Final DP PROOF: all ranks must hold IDENTICAL weights ----------
    w = model.linear.weight.detach().cpu().numpy().copy()
    b = model.linear.bias.detach().cpu().numpy().copy()
    # Publish final weights to files + a done marker, then wait for all ranks.
    np.save(os.path.join(tmpdir, f"w_{rank}.npy"), w)
    np.save(os.path.join(tmpdir, f"b_{rank}.npy"), b)
    with open(os.path.join(tmpdir, f"done_{rank}.txt"), "w") as f:
        f.write("1")
    while not all(os.path.exists(os.path.join(tmpdir, f"done_{r}.txt"))
                  for r in range(world_size)):
        pass
    if rank == 0:
        print("\n=== FINAL DP CHECK: are all ranks identical? ===")
        ref_w = np.load(os.path.join(tmpdir, "w_0.npy"))
        ref_b = np.load(os.path.join(tmpdir, "b_0.npy"))
        all_same = all(
            np.allclose(np.load(os.path.join(tmpdir, f"w_{r}.npy")), ref_w) and
            np.allclose(np.load(os.path.join(tmpdir, f"b_{r}.npy")), ref_b)
            for r in range(world_size))
        for r in range(world_size):
            rw = np.load(os.path.join(tmpdir, f"w_{r}.npy")).ravel()
            rb = np.load(os.path.join(tmpdir, f"b_{r}.npy")).ravel()
            print(f"  rank {r}: weights={rw.round(4)} bias={rb.round(4)}")
        print(f"  >>> all ranks identical after sync: {all_same}")
        print(f"Learned weights: {ref_w.ravel()}  bias: {ref_b.ravel()}")
        print("(target: weights ~[2, 3], bias ~1)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--nproc", type=int, default=2,
                        help="Number of processes (ranks) to spawn.")
    parser.add_argument("--epochs", type=int, default=3)
    args = parser.parse_args()

    global USE_CUDA
    if USE_CUDA and not torch.cuda.is_available():
        print("USE_CUDA=True but no CUDA available -> using CPU.")
        USE_CUDA = False

    # A shared temp directory used to exchange gradients and ready markers
    # between ranks (robust on Windows, unlike gloo/nccl or shared-memory
    # proxies). Each rank writes g_{rank}_{step}.npy + ready_{rank}_{step}.txt.
    with tempfile.TemporaryDirectory() as tmpdir:
        mp.spawn(worker, args=(args.nproc, args.epochs, tmpdir),
                 nprocs=args.nproc, join=True)


if __name__ == "__main__":
    main()
