# Data Parallelism — Distributed Learning Demo

This folder contains a minimal but complete example of **data parallelism**.
**Default mode is CPU-only** (runs anywhere, no GPU needed). The GPU code path
is fully wired up and ready — flip one flag and it runs on CUDA.

## The problem it solves

A common mistake is to load the *entire* dataset on one process/GPU and leave the
others idle ("1 busy, the rest free"). This example avoids that by sharding the
data so **every** process trains on its own slice of data at the same time.

## How it works

1. `N` identical worker processes are launched (one per "rank").
2. The dataset is split into `N` disjoint shards. Rank `r` only ever sees
   shard `r` — so all ranks are busy simultaneously on different data.
3. Each rank runs forward + backward on its shard and computes gradients.
4. The gradients are **all-reduced** (averaged) across ranks, so every replica
   stays in sync and applies the same update.
5. This is mathematically equivalent to training on the combined batch — an
   effective batch size of `N × local_batch`.

```
        Rank 0          Rank 1          Rank 2   ...
        ------          ------          ------
  data: shard 0    |   shard 1    |   shard 2
  fwd/bwd  -->  all-reduce gradients  -->  synced update on every rank
```

## Files

- `ddp_spawn.py` — **start here.** CPU-only data-parallel training with a
  *manual* gradient all-reduce (via `multiprocessing`). Runs out of the box on
  Windows CPU, where PyTorch's `gloo`/`nccl` backends are often broken. Set
  `USE_CUDA = True` at the top to switch to GPUs. No `torchrun` needed.
- `ddp_train.py` — the "textbook" version using PyTorch's
  `DistributedDataParallel` + `DistributedSampler` + `torchrun`. Use this on
  Linux / GPU machines (the standard production approach).
- `README.md` — this file.

## Run it (CPU, no torchrun)

```bash
# 2 processes, each on its own data shard (no idle workers)
python ddp_spawn.py --nproc 2 --epochs 3

# single process quick demo
python ddp_spawn.py --nproc 1 --epochs 3
```

## Run it on GPU (when CUDA is available)

In `ddp_spawn.py` set `USE_CUDA = True`, or use the standard DDP version:

```bash
torchrun --nproc_per_node=2 ddp_train.py --device cuda --epochs 3
```

## What you should see (proof that DP actually works)

The demo prints concrete evidence at every step. With `--nproc 2` you get:

```
[rank 0] owns 4992 samples (156 batches x 32/batch)
[rank 1] owns 4992 samples (156 batches x 32/batch)
[rank 0] LOCAL  weight-grad (before sync) = [ 1.3133 -4.3556]   <- rank 0's own data
[rank 1] LOCAL  weight-grad (before sync) = [-3.0496 -7.124 ]   <- rank 1's own data
[rank 0] SYNCED weight-grad (after all-reduce) = [-0.8682 -5.7398]  <- identical on both
[rank 1] SYNCED weight-grad (after all-reduce) = [-0.8682 -5.7398]  <- identical on both
...
=== FINAL DP CHECK: are all ranks identical? ===
  rank 0: weights=[2.0002 2.9998] bias=[1.0002]
  rank 1: weights=[2.0002 2.9998] bias=[1.0002]
  >>> all ranks identical after sync: True
Learned weights: [2.0001626 2.9997718]  bias: [1.0002077]
(target: weights ~[2, 3], bias ~1)
```

Three things prove data parallelism is really happening:

1. **Different shards** — each rank owns a disjoint slice of the data
   (`owns 4992 samples`), so no rank is idle.
2. **Divergent local gradients** — before sync, rank 0 and rank 1 compute
   *different* gradients because they saw different data. If they were identical
   it would mean the sharding failed.
3. **Identical synced gradients + identical final weights** — after the
   all-reduce every rank holds the *same* averaged gradient and ends with
   *bit-identical* weights (`all ranks identical after sync: True`). That is the
   defining property of data parallelism: many replicas, one synchronized model.

The learned weights converge to the true function `y = 2·x1 + 3·x2 + 1`
(weights ≈ `[2, 3]`, bias ≈ `1`), confirming the parallel training is correct.

## Requirements

```bash
pip install torch
```

