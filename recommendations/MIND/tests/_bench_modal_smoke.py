"""
Smoke test for run_nrms_mind.py (Modal entrypoint).

We cannot launch a real Modal GPU run here (no credentials / GPU), but we can
verify the parts that are most likely to break:
  1. The module imports cleanly (catches NameErrors like a missing `Optional`).
  2. `_build_main_args` forwards the hard-negative mining flags correctly for
     `train_mode='listwise_hn'` and omits them for other modes.
  3. The argv produced for `listwise_hn` is actually accepted by `main.py`
     (via `python main.py --help` style parse check using argparse-free import
     of the real flag set is overkill; instead we assert the exact flag strings
     that main.py expects are present).
"""
import sys

sys.path.insert(0, '.')

import run_nrms_mind as m


def test_import_ok():
    assert m is not None
    print("[ok] module imports cleanly")


def test_build_args_listwise_hn():
    params = dict(
        epochs=1,
        train_mode="listwise_hn",
        max_train_impressions=200,
        max_dev_impressions=100,
        mine_num_hn=4,
        mine_model="sentence-transformers/all-MiniLM-L6-v2",
        mine_cache_dir="/data/model_cache",
        mine_max_news=2000,
        batch_size=32,
        eval_batch_size=16,
    )
    argv = m._build_main_args("t_hn", "train", params)
    s = " ".join(argv)
    # Hard-negative flags must be present and correctly ordered.
    assert "--train_mode listwise_hn" in s, s
    assert "--mine_num_hn 4" in s, s
    assert "--mine_model sentence-transformers/all-MiniLM-L6-v2" in s, s
    assert "--mine_cache_dir /data/model_cache" in s, s
    assert "--mine_max_news 2000" in s, s
    assert "--checkpoint_dir /data/checkpoints" in s, s
    assert "--hf_cache /data/model_cache" in s, s
    print("[ok] listwise_hn argv:", s)


def test_build_args_baseline_omits_mine_flags():
    params = dict(
        epochs=5,
        train_mode="listwise",
        max_train_impressions=5000,
        max_dev_impressions=2000,
        batch_size=128,
        eval_batch_size=256,
    )
    argv = m._build_main_args("t_base", "train", params)
    s = " ".join(argv)
    assert "--mine_num_hn" not in s, s
    assert "--mine_max_news" not in s, s
    assert "--train_mode listwise" in s, s
    print("[ok] baseline argv omits mine flags:", s)


def test_main_py_accepts_mine_flags():
    # Confirm main.py actually defines the mine flags that _build_main_args emits,
    # so the forwarded argv will not be rejected at parse time on Modal.
    import importlib.util
    spec = importlib.util.spec_from_file_location("main_mod", "main.py")
    main_mod = importlib.util.module_from_spec(spec)
    # parse_args() reads sys.argv; stub it so import doesn't trigger CLI parsing.
    import argparse
    real_parse = argparse.ArgumentParser.parse_args
    argparse.ArgumentParser.parse_args = lambda self, *a, **k: self.parse_args_real(*a, **k) if hasattr(self, "parse_args_real") else self
    try:
        spec.loader.exec_module(main_mod)
    finally:
        argparse.ArgumentParser.parse_args = real_parse
    # Inspect the argument parser definition by re-parsing with --help suppressed.
    import io
    from contextlib import redirect_stdout
    p = main_mod.parse_args.__globals__  # not directly usable; rebuild a parser
    # Simpler: call parse_args with the exact mine flags to ensure they are accepted.
    test_argv = [
        "--train_mode", "listwise_hn",
        "--mine_num_hn", "4",
        "--mine_model", "sentence-transformers/all-MiniLM-L6-v2",
        "--mine_cache_dir", "/data/model_cache",
        "--mine_max_news", "2000",
        "--epochs", "1",
    ]
    import sys as _sys
    old = _sys.argv
    _sys.argv = ["main.py"] + test_argv
    try:
        ns = main_mod.parse_args()
    finally:
        _sys.argv = old
    assert ns.train_mode == "listwise_hn"
    assert ns.mine_num_hn == 4
    assert ns.mine_max_news == 2000
    assert ns.mine_cache_dir == "/data/model_cache"
    print("[ok] main.py accepts all mine flags forwarded by run_nrms_mind.py")


if __name__ == "__main__":
    test_import_ok()
    test_build_args_listwise_hn()
    test_build_args_baseline_omits_mine_flags()
    test_main_py_accepts_mine_flags()
    print("\nALL MODAL SMOKE TESTS PASSED")
