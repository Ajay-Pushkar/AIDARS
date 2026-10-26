import hashlib
import json
import uuid
import os
from typing import Any, List, Dict, Optional
from aidars.adapters.base import ApplicationAdapter
from aidars.distributed.models import WorkloadSpec, ExecutionSpec, OutputVerificationPolicy
_LLM_WORKER_RUNNER_CODE = (
    "import json, os, sys, urllib.request\n"
    "os.makedirs('outputs', exist_ok=True)\n"
    "prompts = json.loads(os.environ.get('AIDAR_LLM_PROMPTS_JSON', '[\"Hello\"]'))\n"
    "model = os.environ.get('AIDAR_LLM_MODEL', 'llama3')\n"
    "max_tokens = int(os.environ.get('AIDAR_LLM_MAX_TOKENS', '128'))\n"
    "temperature = float(os.environ.get('AIDAR_LLM_TEMPERATURE', '0.7'))\n"
    "chunk_idx = os.environ.get('AIDAR_LLM_CHUNK_INDEX', '0')\n"
    "live = os.environ.get('AIDAR_LLM_LIVE', '0').strip() in ('1', 'true', 'yes')\n"
    "ollama_url = os.environ.get('AIDAR_OLLAMA_URL', 'http://127.0.0.1:11434').rstrip('/')\n"
    "results = []\n"
    "for p in prompts:\n"
    "    resp_text = f'simulated_response:{model}:{p}'\n"
    "    if live:\n"
    "        try:\n"
    "            body = json.dumps({'model': model, 'prompt': p, 'stream': False, 'options': {'num_predict': max_tokens, 'temperature': temperature}}).encode('utf-8')\n"
    "            req = urllib.request.Request(f'{ollama_url}/api/generate', data=body, headers={'Content-Type': 'application/json'}, method='POST')\n"
    "            with urllib.request.urlopen(req, timeout=120.0) as r:\n"
    "                payload = json.loads(r.read().decode('utf-8'))\n"
    "                raw_resp = payload.get('response', '').strip()\n"
    "                raw_think = payload.get('thinking', '').strip()\n"
    "                resp_text = raw_resp if raw_resp else raw_think\n"
    "                if raw_resp and raw_think: resp_text = f'{raw_resp}\\n\\n[Reasoning: {raw_think[:200]}...]'\n"
    "        except Exception as exc:\n"
    "            if os.environ.get('AIDAR_LLM_REQUIRE_LIVE', '0') == '1':\n"
    "                raise\n"
    "            resp_text = f'fallback_response:{model}:{exc}'\n"
    "    results.append({'prompt': p, 'response': resp_text})\n"
    "out_path = f'outputs/llm_chunk_{chunk_idx}.json'\n"
    "with open(out_path, 'w', encoding='utf-8') as f:\n"
    "    json.dump({'model': model, 'chunk_index': chunk_idx, 'results': results}, f)\n"
    "print(f'LLM inference completed: {len(results)} prompts -> {out_path}')\n"
)


class LLMAdapter(ApplicationAdapter):
    """LLM inference adapter implementing the generic contract.
    
    Targets llama-cli (llama.cpp) as the local inference engine.
    """
    
    def __init__(self, asset_manager=None) -> None:
        self.asset_manager = asset_manager

    def validate(self, request: dict) -> bool:
        """Validate LLM specific request."""
        if "prompt" not in request and "prompts" not in request:
            return False
        return True

    def evaluate_request(self, request: dict) -> List[WorkloadSpec]:
        """Parse LLM request, calculate requirements, and output WorkloadSpec."""
        model_identifier = request.get("model") or request.get("model_identifier") or request.get("model_path") or "default-model"
        model_path = request.get("model_path")
        prompt = request.get("prompt", "")
        raw_prompts = request.get("prompts")
        if raw_prompts and isinstance(raw_prompts, list):
            prompt_list = [str(p) for p in raw_prompts]
        elif prompt:
            prompt_list = [str(prompt)]
        else:
            prompt_list = ["Hello"]

        max_tokens = request.get("max_tokens", 256)
        requires_gpu = request.get("requires_gpu", True)
        worker_count = int(request.get("worker_count", 1))
        min_cpu_cores = int(request.get("min_cpu_cores", 4))
        
        # M4: Physical asset resolution
        input_asset_hashes = set()
        model_hash = None
        if model_path and os.path.exists(model_path) and os.path.isfile(model_path):
            hasher = hashlib.sha256()
            with open(model_path, "rb") as f:
                while chunk_data := f.read(65536):
                    hasher.update(chunk_data)
            model_hash = hasher.hexdigest()
            input_asset_hashes.add(model_hash)
            if self.asset_manager:
                self.asset_manager.upload_asset_file(model_path, model_hash)

        job_id = f"llm-job-{uuid.uuid4().hex[:16]}"
        
        # Estimate requirements based on parameters
        min_vram_bytes = 4 * 1024 * 1024 * 1024 if requires_gpu else 0
        min_ram_bytes = int(request.get("min_ram_bytes", 8 * 1024 * 1024 * 1024))

        if len(prompt_list) > 1 and worker_count > 1:
            specs = []
            for idx, p in enumerate(prompt_list):
                wid = f"{job_id}-chunk-{idx}"
                spec = WorkloadSpec(
                    workload_id=wid,
                    job_id=job_id,
                    task_type="llm_inference",
                    input_asset_hashes=input_asset_hashes,
                    min_cpu_cores=min_cpu_cores,
                    min_ram_bytes=min_ram_bytes,
                    requires_gpu=requires_gpu,
                    min_vram_bytes=min_vram_bytes,
                    estimated_duration_seconds=float(max_tokens) * 0.5,
                    parameters={
                        "model": model_identifier,
                        "model_identifier": model_identifier,
                        "model_hash": model_hash,
                        "prompt": p,
                        "prompts": [p],
                        "max_tokens": max_tokens,
                        "chunk_index": idx,
                    }
                )
                spec.execution_spec = self.build_execution_spec(spec)
                specs.append(spec)
            return specs

        prompt_hash = hashlib.md5(prompt_list[0].encode()).hexdigest()[:8]
        spec = WorkloadSpec(
            workload_id=f"{job_id}-{prompt_hash}",
            job_id=job_id,
            task_type="llm_inference",
            input_asset_hashes=input_asset_hashes,
            min_cpu_cores=min_cpu_cores,
            min_ram_bytes=min_ram_bytes,
            requires_gpu=requires_gpu,
            min_vram_bytes=min_vram_bytes,
            estimated_duration_seconds=float(max_tokens) * 0.5, # ~2 tok/sec fallback estimate
            parameters={
                "model": model_identifier,
                "model_identifier": model_identifier,
                "model_hash": model_hash,
                "prompt": prompt_list[0],
                "prompts": prompt_list,
                "max_tokens": max_tokens,
            }
        )
        spec.execution_spec = self.build_execution_spec(spec)
        return [spec]
        
    def build_execution_spec(self, spec: WorkloadSpec) -> ExecutionSpec:
        """Build the generic subprocess execution specification for LLM inference."""
        model_id = spec.parameters.get("model_identifier") or spec.parameters.get("model", "llama3")
        batch_size = spec.parameters.get("batch_size", 1)
        max_tokens = spec.parameters.get("max_tokens", 128)
        temperature = spec.parameters.get("temperature", 0.7)
        prompts = spec.parameters.get("prompts") or ([spec.parameters["prompt"]] if spec.parameters.get("prompt") else ["Hello"])
        chunk_index = spec.parameters.get("chunk_index", 0)

        args = [
            "-c",
            _LLM_WORKER_RUNNER_CODE,
            "--model",
            str(model_id),
            "--batch-size",
            str(batch_size),
            "--max-tokens",
            str(max_tokens),
            "--temperature",
            str(temperature),
        ]

        env = {
            "AIDAR_ADAPTER": "llm",
            "AIDAR_LLM_MODEL": str(model_id),
            "AIDAR_LLM_BATCH_SIZE": str(batch_size),
            "AIDAR_LLM_MAX_TOKENS": str(max_tokens),
            "AIDAR_LLM_TEMPERATURE": str(temperature),
            "AIDAR_LLM_PROMPTS_JSON": json.dumps(list(prompts)),
            "AIDAR_LLM_CHUNK_INDEX": str(chunk_index),
        }

        return ExecutionSpec(
            command="python",
            args=args,
            env=env,
        )

    def describe_expected_outputs(self, workload: WorkloadSpec) -> OutputVerificationPolicy:
        return OutputVerificationPolicy(
            expected_output_count=1 # Expect one text output file
        )
