"""
Data Parallelism demo with PyTorch DistributedDataParallel (DDP).

DEFAULT MODE = CPU ONLY (works everywhere, no GPU needed).
The GPU code path is fully wired up and ready: pass --device cuda and run on a
machine with CUDA GPUs to use it. Nothing else in the training logic changes.

WHY THIS AVOIDS "1 BUSY, REST FREE":
In a naive setup you might put the whole dataset on a single process/GPU and
leave the others idle. Here we use `DistributedSampler`, which splits the
dataset into N disjoint shards (one per process). Every process:
  1. loads its OWN shard of the data,
  2. runs the forward/backward pass on that shard,
  3. averages gradients with the other processes via DDP's all-reduce.
So all ranks are busy at the same time, each on different data -> true data
parallelism.

HOW TO RUN (multi-process, CPU-only by default):
    torchrun --nproc_per_node=2 ddp_train.py --device cpu
Replace 2 with the number of processes you want.

HOW TO RUN ON GPU (when CUDA is available):
    torchrun --nproc_per_node=2 ddp_train.py --device cuda

HOW TO RUN ON A SINGLE CPU FOR A QUICK DEMO (no distributed):
    python ddp_train.py --single
"""

import argparse
import os
import torch
import torch.distributed as dist
import torch.nn as nn
from torch.nn.parallel import DistributedDataParallel as DDP
from torch.utils.data import DataLoader, Dataset, DistributedSampler


class ToyDataset(Dataset):
    """A trivial dataset: y = 2*x1 + 3*x2 + 1 + noise. We fit a linear model."""

    def __init__(self, n_samples: int = 10_000):
        torch.manual_seed(0)
        self.x = torch.randn(n_samples, 2)
        self.y = (2.0 * self.x[:, 0] + 3.0 * self.x[:, 1] + 1.0
                  + 0.01 * torch.randn(n_samples))

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


def setup_distributed(device: str):
    """Initialize the process group from torchrun-provided env vars.

    `device` is "cuda" or "cpu". The backend is chosen accordingly:
      - cuda -> nccl (fast GPU all-reduce)
      - cpu  -> gloo (works on any machine, no GPU needed)
    """
    backend = "nccl" if device == "cuda" else "gloo"
    dist.init_process_group(backend=backend)
    rank = dist.get_rank()
    world_size = dist.get_world_size()
    local_rank = int(os.environ["LOCAL_RANK"])
    # Each process pins to its own GPU when using CUDA.
    if device == "cuda":
        torch.cuda.set_device(local_rank)
    return rank, world_size, local_rank


def cleanup_distributed():
    if dist.is_initialized():
        dist.destroy_process_group()


def train(rank, world_size, local_rank, epochs, device="cpu", single=False):
    # ---- Model ----------------------------------------------------------
    # Resolve the torch device for this process.
    # CPU-only by default; CUDA path is ready when --device cuda is passed.
    if device == "cuda":
        device = torch.device(f"cuda:{local_rank}")
    else:
        device = torch.device("cpu")

    model = LinearModel().to(device)

    if single:
        # Single-process path: plain model, no DDP, whole dataset on one process.
        sampler = None
        loader = DataLoader(ToyDataset(), batch_size=64, shuffle=True)
    else:
        # DDP path: wrap the model so gradients are all-reduced across ranks.
        # device_ids is only set for CUDA; for CPU it is left as None.
        ddp_kwargs = {"device_ids": [local_rank]} if device.type == "cuda" else {}
        model = DDP(model, **ddp_kwargs)
        # DistributedSampler splits the data into `world_size` disjoint shards.
        # Rank r only ever sees shard r -> every process is busy on its own data.
        sampler = DistributedSampler(ToyDataset(), num_replicas=world_size, rank=rank)
        loader = DataLoader(ToyDataset(), batch_size=64, sampler=sampler)

    optimizer = torch.optim.SGD(model.parameters(), lr=0.1)
    loss_fn = nn.MSELoss()

    for epoch in range(epochs):
        if sampler is not None:
            sampler.set_epoch(epoch)  # reshuffle shards differently each epoch
        model.train()
        total_loss, n_batches = 0.0, 0
        for x, y in loader:
            x, y = x.to(device), y.to(device)
            optimizer.zero_grad()
            pred = model(x)
            loss = loss_fn(pred, y)
            loss.backward()      # DDP all-reduces grads automatically here
            optimizer.step()
            total_loss += loss.item()
            n_batches += 1

        avg_loss = total_loss / n_batches
        # Only rank 0 prints to avoid 4x duplicated logs.
        if single or rank == 0:
            print(f"[{'single' if single else f'rank {rank}/{world_size}'}] "
                  f"epoch {epoch+1}/{epochs}  loss={avg_loss:.4f}")

    # Show the learned weights on rank 0 (or single) to confirm convergence.
    if single or rank == 0:
        w = model.module.linear.weight if hasattr(model, "module") else model.linear.weight
        b = model.module.linear.bias if hasattr(model, "module") else model.linear.bias
        print(f"Learned weights: {w.detach().cpu().numpy().ravel()}  "
              f"bias: {b.detach().cpu().numpy().ravel()}")
        print("(target: weights ~[2, 3], bias ~1)")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--epochs", type=int, default=3)
    parser.add_argument("--device", choices=["cpu", "cuda"], default="cpu",
                        help="Device to train on. CPU-only by default; "
                             "use 'cuda' when GPUs are available.")
    parser.add_argument("--single", action="store_true",
                        help="Run a single-process demo (no distributed).")
    args = parser.parse_args()

    if args.single:
        train(rank=0, world_size=1, local_rank=0, epochs=args.epochs,
              device=args.device, single=True)
        return

    rank, world_size, local_rank = setup_distributed(args.device)
    try:
        train(rank, world_size, local_rank, args.epochs, device=args.device)
    finally:
        cleanup_distributed()


if __name__ == "__main__":
    main()
