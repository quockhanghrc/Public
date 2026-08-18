import importlib
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "modal"))


def load_config():
    import config

    return importlib.reload(config)


def test_default_hardware_is_gpu(monkeypatch):
    monkeypatch.delenv("MODAL_HARDWARE", raising=False)
    cfg = load_config()
    assert cfg.HARDWARE == "gpu"
    assert cfg.VLLM_APP_NAME == "phase2-vllm-gpu"
    assert cfg.API_APP_NAME == "phase2-api"


def test_cpu_hardware_profile(monkeypatch):
    monkeypatch.setenv("MODAL_HARDWARE", "cpu")
    cfg = load_config()
    assert cfg.HARDWARE == "cpu"
    assert cfg.VLLM_APP_NAME == "phase2-vllm-cpu"
    assert cfg.gpu_spec() == "T4"
    assert cfg.cpu_spec() == 2


def test_gpu_serve_command(monkeypatch):
    monkeypatch.setenv("MODAL_HARDWARE", "gpu")
    cfg = load_config()
    cmd = cfg.vllm_serve_command()
    assert "--device" not in cmd
    assert "--enable-prefix-caching" in cmd
    assert "--cpu-kvcache-space" not in cmd
    assert cmd[cmd.index("--port") + 1] == str(cfg.VLLM_INTERNAL_PORT)


def test_cpu_serve_command(monkeypatch):
    monkeypatch.setenv("MODAL_HARDWARE", "cpu")
    cfg = load_config()
    cmd = cfg.vllm_serve_command()
    assert "--device" not in cmd
    assert "--cpu-kvcache-space" not in cmd
    assert cfg.ENABLE_PREFIX_CACHING


def test_model_name_override(monkeypatch):
    monkeypatch.setenv("MODEL_NAME", "Qwen/Qwen2.5-7B-Instruct")
    cfg = load_config()
    assert cfg.MODEL_NAME == "Qwen/Qwen2.5-7B-Instruct"


def test_memory_knob(monkeypatch):
    monkeypatch.setenv("MODAL_HARDWARE", "cpu")
    monkeypatch.setenv("MODAL_CPU_MEMORY", "16384")
    cfg = load_config()
    assert cfg.CPU_MEMORY_MIB == 16384


def test_kvcache_knob(monkeypatch):
    monkeypatch.setenv("MODAL_HARDWARE", "cpu")
    monkeypatch.setenv("MODAL_CPU_KVCACHE_GB", "2")
    cfg = load_config()
    assert cfg.CPU_KVCACHE_GB == 2


def test_invalid_hardware_raises(monkeypatch):
    monkeypatch.setenv("MODAL_HARDWARE", "banana")
    with pytest.raises(ValueError):
        load_config()
