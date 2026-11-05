import pytest
import os
import shutil
import tempfile
import asyncio
from aidars.adapters.llm.adapter import LLMAdapter
from aidars.distributed.models import WorkloadSpec
from aidars.distributed.cas_adapter import LocalCASAdapter
from aidars.distributed.worker import DistributedWorker
from aidars.core.assets.manager import AssetManager

def is_llama_installed():
    return shutil.which("llama-cli") is not None

@pytest.fixture
def mock_model_file():
    fd, path = tempfile.mkstemp(suffix=".gguf")
    with os.fdopen(fd, 'w') as f:
        f.write("mock_model_data")
    yield path
    os.remove(path)

@pytest.mark.asyncio
async def test_m19_6_llm_distributed_orchestration(mock_model_file):
    if not is_llama_installed():
        pytest.skip("ENVIRONMENT-GATED: llama-cli not found")
        
    coord_cas_dir = tempfile.mkdtemp()
    coord_cas = LocalCASAdapter(coord_cas_dir)
    coord_asset_manager = AssetManager(coord_cas)
    
    adapter = LLMAdapter(asset_manager=coord_asset_manager)
    
    # 1. Submit multiple independent prompts (Distributed Orchestration)
    prompts = ["Tell me a joke", "What is AI?", "Translate hello"]
    all_specs = []
    
    for prompt in prompts:
        req = {
            "model_path": mock_model_file,
            "prompt": prompt,
            "max_tokens": 10,
            "requires_gpu": False
        }
        assert adapter.validate(req)
        specs = adapter.evaluate_request(req)
        spec = specs[0]
        spec.execution_spec = adapter.build_execution_spec(spec)
        all_specs.append(spec)
        
    assert len(all_specs) == 3
    # Check that job_ids and workload_ids are distinct for independent workloads
    job_ids = {s.job_id for s in all_specs}
    assert len(job_ids) == 3
    
    # 2. Worker setup
    worker_cas_dir = tempfile.mkdtemp()
    worker_cas = LocalCASAdapter(worker_cas_dir)
    
    # Simulate single_flight_sync for Worker A
    spec_a = all_specs[0]
    for h in spec_a.input_asset_hashes:
        source_path = coord_cas.get_asset_path(h)
        worker_cas.store_file(source_path, expected_sha256=h)
        
    worker_a = DistributedWorker(
        coordinator_url="http://localhost:dummy",
        port=0,
        cas_adapter=worker_cas
    )
    
    # 3. Physical Execution Validation
    result = await worker_a.execute_workload(spec_a)
    
    assert result.success is True
    assert len(result.output_asset_hashes) == 1


def test_m19_6_llm_adapter_evaluation_parameters():
    adapter = LLMAdapter()
    req = {
        "model_identifier": "deepseek-r1:8b",
        "prompt": "What is the capital of France?",
        "max_tokens": 512,
        "temperature": 0.3,
        "requires_gpu": True,
    }
    assert adapter.validate(req)
    specs = adapter.evaluate_request(req)
    assert len(specs) == 1
    spec = specs[0]
    
    # Requirement 2: Ensure spec.parameters includes model, model_identifier, max_tokens, temperature
    assert spec.parameters["model"] == "deepseek-r1:8b"
    assert spec.parameters["model_identifier"] == "deepseek-r1:8b"
    assert spec.parameters["max_tokens"] == 512
    assert spec.parameters["temperature"] == 0.3
    
    # Requirement 2: Ensure spec.execution_spec is populated before returning
    assert spec.execution_spec is not None
    assert spec.execution_spec.command == "python"
    assert "-c" in spec.execution_spec.args
    assert spec.execution_spec.env["AIDAR_LLM_MODEL"] == "deepseek-r1:8b"
    assert spec.execution_spec.env["AIDAR_LLM_MAX_TOKENS"] == "512"
    assert spec.execution_spec.env["AIDAR_LLM_TEMPERATURE"] == "0.3"


def test_m19_6_llm_multi_prompt_chunking():
    adapter = LLMAdapter()
    req = {
        "model": "deepseek-r1:8b",
        "prompts": ["Prompt A", "Prompt B", "Prompt C"],
        "max_tokens": 256,
        "temperature": 0.2,
        "worker_count": 3,
    }
    specs = adapter.evaluate_request(req)
    assert len(specs) == 3
    for idx, spec in enumerate(specs):
        assert spec.parameters["model"] == "deepseek-r1:8b"
        assert spec.parameters["model_identifier"] == "deepseek-r1:8b"
        assert spec.parameters["max_tokens"] == 256
        assert spec.parameters["temperature"] == 0.2
        assert spec.parameters["chunk_index"] == idx
        assert spec.execution_spec is not None


def test_m19_6_llm_runner_simulated_execution(tmp_path):
    import subprocess
    import json
    from aidars.adapters.llm.adapter import _LLM_WORKER_RUNNER_CODE
    
    env = os.environ.copy()
    env["AIDAR_LLM_MODEL"] = "deepseek-r1:8b"
    env["AIDAR_LLM_PROMPTS_JSON"] = json.dumps(["Hello from test"])
    env["AIDAR_LLM_MAX_TOKENS"] = "256"
    env["AIDAR_LLM_TEMPERATURE"] = "0.3"
    env["AIDAR_LLM_CHUNK_INDEX"] = "0"
    env["AIDAR_LLM_LIVE"] = "0"
    
    res = subprocess.run(
        [shutil.which("python") or "python", "-c", _LLM_WORKER_RUNNER_CODE],
        cwd=str(tmp_path),
        env=env,
        capture_output=True,
        text=True,
    )
    assert res.returncode == 0, f"Runner failed:\nSTDOUT: {res.stdout}\nSTDERR: {res.stderr}"
    
    out_file = tmp_path / "outputs" / "llm_chunk_0.json"
    assert out_file.exists()
    data = json.loads(out_file.read_text(encoding="utf-8"))
    assert data["model"] == "deepseek-r1:8b"
    assert data["chunk_index"] == "0"
    assert len(data["results"]) == 1
    assert "simulated_response:deepseek-r1:8b:Hello from test" in data["results"][0]["response"]


def test_m19_6_llm_runner_live_streaming_fast_reasoning_and_fallback(tmp_path):
    import http.server
    import threading
    import subprocess
    import json
    from aidars.adapters.llm.adapter import _LLM_WORKER_RUNNER_CODE

    recorded_requests = []

    class MockOllamaHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            if self.path == "/api/tags":
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                # Return available models (missing deepseek-r1:8b to trigger fallback)
                self.wfile.write(json.dumps({
                    "models": [{"name": "qwen2.5:7b"}, {"name": "llama3:latest"}]
                }).encode("utf-8"))
            else:
                self.send_response(404)
                self.end_headers()

        def do_POST(self):
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length)
            payload = json.loads(body.decode("utf-8"))
            recorded_requests.append(payload)

            self.send_response(200)
            self.send_header("Content-Type", "application/x-ndjson")
            self.end_headers()
            # Stream line-by-line NDJSON chunks with thinking tags
            chunks = [
                {"response": "<think>\nStep-by-step thinking...\n</think>\n", "done": False},
                {"response": "Hello ", "done": False},
                {"response": "from ", "done": False},
                {"response": "stream!", "done": True},
            ]
            for c in chunks:
                self.wfile.write((json.dumps(c) + "\n").encode("utf-8"))
                self.wfile.flush()

        def log_message(self, format, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), MockOllamaHandler)
    port = server.server_port
    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()

    try:
        env = os.environ.copy()
        env["AIDAR_LLM_MODEL"] = "deepseek-r1:8b"
        env["AIDAR_LLM_PROMPTS_JSON"] = json.dumps(["Explain quantum computing in 5 words"])
        env["AIDAR_LLM_MAX_TOKENS"] = "128"
        env["AIDAR_LLM_NUM_CTX"] = "2048"
        env["AIDAR_LLM_TEMPERATURE"] = "0.2"
        env["AIDAR_LLM_CHUNK_INDEX"] = "0"
        env["AIDAR_LLM_LIVE"] = "1"
        env["AIDAR_LLM_FAST_REASONING"] = "1"
        env["AIDAR_OLLAMA_URL"] = f"http://127.0.0.1:{port}"

        res = subprocess.run(
            [shutil.which("python") or "python", "-c", _LLM_WORKER_RUNNER_CODE],
            cwd=str(tmp_path),
            env=env,
            capture_output=True,
            text=True,
        )
        assert res.returncode == 0, f"Runner failed:\nSTDOUT: {res.stdout}\nSTDERR: {res.stderr}"

        # 1. Verify fallback model was selected because deepseek-r1:8b was missing from /api/tags
        assert len(recorded_requests) == 1
        req = recorded_requests[0]
        assert req["model"] == "qwen2.5:7b"

        # 2. Verify options and keep_alive
        assert req["keep_alive"] == "15m"
        assert req["stream"] is True
        assert req["raw"] is True
        assert req["options"]["num_ctx"] == 2048
        assert req["options"]["num_predict"] == 128
        assert req["options"]["temperature"] == 0.2
        assert "num_thread" in req["options"]
        assert req["options"]["num_thread"] >= 1

        # 3. Verify fast reasoning prompt formatting
        assert "<｜User｜>" in req["prompt"]
        assert "<｜Assistant｜><think>\n</think>\n" in req["prompt"]

        # 4. Verify output file has response without <think> tags
        out_file = tmp_path / "outputs" / "llm_chunk_0.json"
        assert out_file.exists()
        data = json.loads(out_file.read_text(encoding="utf-8"))
        assert len(data["results"]) == 1
        resp = data["results"][0]["response"]
        assert "<think>" not in resp
        assert "</think>" not in resp
        assert "Hello from stream!" in resp
    finally:
        server.shutdown()
        server.server_close()


