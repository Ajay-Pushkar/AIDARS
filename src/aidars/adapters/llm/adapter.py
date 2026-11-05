import hashlib
import json
import uuid
import os
from typing import Any, List, Dict, Optional
from aidars.adapters.base import ApplicationAdapter
from aidars.distributed.models import WorkloadSpec, ExecutionSpec, OutputVerificationPolicy
_LLM_WORKER_RUNNER_CODE = (
    "import json, os, sys, urllib.request, re\n"
    "os.makedirs('outputs', exist_ok=True)\n"
    "params = json.loads(os.environ.get('AIDAR_LLM_PARAMS_JSON', '{}'))\n"
    "prompts = json.loads(os.environ.get('AIDAR_LLM_PROMPTS_JSON', '[\"Hello\"]'))\n"
    "model = str(params.get('model_identifier') or params.get('model') or os.environ.get('AIDAR_LLM_MODEL', 'llama3'))\n"
    "max_tokens = int(params.get('max_tokens', os.environ.get('AIDAR_LLM_MAX_TOKENS', '512')))\n"
    "num_ctx = int(params.get('num_ctx', os.environ.get('AIDAR_LLM_NUM_CTX', '1024')))\n"
    "temperature = float(params.get('temperature', os.environ.get('AIDAR_LLM_TEMPERATURE', '0.3')))\n"
    "chunk_idx = str(params.get('chunk_index', os.environ.get('AIDAR_LLM_CHUNK_INDEX', '0')))\n"
    "live = os.environ.get('AIDAR_LLM_LIVE', '0').strip() in ('1', 'true', 'yes')\n"
    "ollama_url = os.environ.get('AIDAR_OLLAMA_URL', 'http://127.0.0.1:11434').rstrip('/')\n"
    "fast_reasoning = bool(params.get('fast_reasoning', os.environ.get('AIDAR_LLM_FAST_REASONING', '1').strip() in ('1', 'true', 'yes')))\n"
    "cpus = os.cpu_count() or 4\n"
    "threads = max(1, cpus - 2) if cpus >= 8 else max(1, cpus - 1)\n"
    "options = {'num_ctx': num_ctx, 'num_predict': max_tokens, 'temperature': temperature, 'num_thread': threads}\n"
    "available_models = []\n"
    "if live:\n"
    "    try:\n"
    "        tags_req = urllib.request.Request(f'{ollama_url}/api/tags', method='GET')\n"
    "        with urllib.request.urlopen(tags_req, timeout=5.0) as r_tags:\n"
    "            tags_data = json.loads(r_tags.read().decode('utf-8'))\n"
    "            available_models = [m.get('name', '') for m in tags_data.get('models', []) if m.get('name')]\n"
    "    except Exception:\n"
    "        pass\n"
    "target_model = model\n"
    "if live and available_models:\n"
    "    matched = False\n"
    "    for m in available_models:\n"
    "        if model == m or model == m.split(':')[0] or f'{model}:latest' == m:\n"
    "            target_model = m\n"
    "            matched = True\n"
    "            break\n"
    "    if not matched:\n"
    "        target_model = available_models[0]\n"
    "results = []\n"
    "for p in prompts:\n"
    "    resp_text = f'simulated_response:{model}:{p}'\n"
    "    if live:\n"
    "        try:\n"
    "            is_deepseek_r1 = 'deepseek-r1' in model.lower() or 'deepseek-r1' in target_model.lower()\n"
    "            use_fast = is_deepseek_r1 or fast_reasoning\n"
    "            if use_fast:\n"
    "                raw_prompt = f'<\\uff5cUser\\uff5c>{p}<\\uff5cAssistant\\uff5c><think>\\n</think>\\n'\n"
    "                payload = {'model': target_model, 'prompt': raw_prompt, 'raw': True, 'stream': True, 'keep_alive': '15m', 'options': options}\n"
    "            else:\n"
    "                payload = {'model': target_model, 'prompt': p, 'stream': True, 'keep_alive': '15m', 'options': options}\n"
    "            req_body = json.dumps(payload).encode('utf-8')\n"
    "            req = urllib.request.Request(f'{ollama_url}/api/generate', data=req_body, headers={'Content-Type': 'application/json'}, method='POST')\n"
    "            chunks = []\n"
    "            with urllib.request.urlopen(req, timeout=120.0) as r:\n"
    "                for line in r:\n"
    "                    line_str = line.decode('utf-8').strip()\n"
    "                    if not line_str: continue\n"
    "                    try:\n"
    "                        chunk_json = json.loads(line_str)\n"
    "                        token = chunk_json.get('response', '')\n"
    "                        if token: chunks.append(token)\n"
    "                    except Exception: pass\n"
    "            resp_text = ''.join(chunks).strip()\n"
    "            if resp_text and '<think>' in resp_text:\n"
    "                resp_text = re.sub(r'<think>.*?</think>', '', resp_text, flags=re.DOTALL).strip()\n"
    "        except Exception as exc:\n"
    "            if os.environ.get('AIDAR_LLM_REQUIRE_LIVE', '0') == '1':\n"
    "                raise\n"
    "            resp_text = f'fallback_response:{model}:{exc}'\n"
    "    results.append({'prompt': p, 'response': resp_text})\n"
    "out_path = f'outputs/llm_chunk_{chunk_idx}.json'\n"
    "with open(out_path, 'w', encoding='utf-8') as f:\n"
    "    json.dump({'model': model, 'target_model': target_model, 'chunk_index': chunk_idx, 'results': results}, f)\n"
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

    def evaluate_request(self, request: Any) -> List[WorkloadSpec]:
        """Parse LLM request, calculate requirements, and output WorkloadSpec."""
        if hasattr(request, "get"):
            model_identifier = (
                request.get("model_identifier")
                or request.get("model")
                or request.get("model_path")
                or "default-model"
            )
            model_path = request.get("model_path")
            prompt = request.get("prompt", "")
            raw_prompts = request.get("prompts")
            max_tokens = int(request.get("max_tokens", 256))
            temperature = float(request.get("temperature", 0.3))
            num_ctx = int(request.get("num_ctx", 1024))
            fast_reasoning = bool(request.get("fast_reasoning", True))
            requires_gpu = request.get("requires_gpu", True)
            worker_count = int(request.get("worker_count", 1))
            min_cpu_cores = int(request.get("min_cpu_cores", 4))
            min_ram_bytes = int(request.get("min_ram_bytes", 8 * 1024 * 1024 * 1024))
        else:
            model_identifier = (
                getattr(request, "model_identifier", None)
                or getattr(request, "model", None)
                or getattr(request, "model_path", None)
                or "default-model"
            )
            model_path = getattr(request, "model_path", None)
            prompt = getattr(request, "prompt", "")
            raw_prompts = getattr(request, "prompts", None)
            max_tokens = int(getattr(request, "max_tokens", 256))
            temperature = float(getattr(request, "temperature", 0.3))
            num_ctx = int(getattr(request, "num_ctx", 1024))
            fast_reasoning = bool(getattr(request, "fast_reasoning", True))
            requires_gpu = getattr(request, "requires_gpu", True)
            worker_count = int(getattr(request, "worker_count", 1))
            min_cpu_cores = int(getattr(request, "min_cpu_cores", 4))
            min_ram_bytes = int(getattr(request, "min_ram_bytes", 8 * 1024 * 1024 * 1024))

        if raw_prompts and isinstance(raw_prompts, list):
            prompt_list = [str(p) for p in raw_prompts]
        elif prompt:
            prompt_list = [str(prompt)]
        else:
            prompt_list = ["Hello"]

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
                        "temperature": temperature,
                        "num_ctx": num_ctx,
                        "fast_reasoning": fast_reasoning,
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
                "temperature": temperature,
                "num_ctx": num_ctx,
                "fast_reasoning": fast_reasoning,
            }
        )
        spec.execution_spec = self.build_execution_spec(spec)
        return [spec]
        
    def build_execution_spec(self, spec: WorkloadSpec) -> ExecutionSpec:
        """Build the generic subprocess execution specification for LLM inference."""
        model_id = spec.parameters.get("model_identifier") or spec.parameters.get("model", "llama3")
        batch_size = spec.parameters.get("batch_size", 1)
        max_tokens = spec.parameters.get("max_tokens", 512)
        temperature = spec.parameters.get("temperature", 0.3)
        num_ctx = spec.parameters.get("num_ctx", 1024)
        fast_reasoning = spec.parameters.get("fast_reasoning", True)
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
            "AIDAR_LLM_NUM_CTX": str(num_ctx),
            "AIDAR_LLM_TEMPERATURE": str(temperature),
            "AIDAR_LLM_FAST_REASONING": "1" if fast_reasoning else "0",
            "AIDAR_LLM_PROMPTS_JSON": json.dumps(list(prompts)),
            "AIDAR_LLM_CHUNK_INDEX": str(chunk_index),
            "AIDAR_LLM_PARAMS_JSON": json.dumps(spec.parameters or {}),
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
