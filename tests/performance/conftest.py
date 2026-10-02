import os
import time
import httpx
import pytest
import tempfile
import subprocess
import yaml

@pytest.fixture(scope="function")
def scalable_docker_cluster(request):
    """
    Creates an isolated Docker Compose cluster with a dynamic number of workers.
    Does not modify the production deploy/docker-compose.yml.
    """
    num_workers = getattr(request, "param", 3)
    
    with tempfile.TemporaryDirectory() as temp_dir:
        # Create .env
        env_path = os.path.join(temp_dir, ".env")
        with open(env_path, "w") as f:
            f.write("AIDAR_ADMIN_TOKENS=test_token\n")
            f.write("AIDAR_WORKER_BOOTSTRAP_SECRET=test_secret\n")
            f.write("AIDAR_INSECURE_MODE=1\n")

        # Create docker-compose.yml
        compose_dict = {
            "services": {
                "coordinator": {
                    "build": {
                        "context": "C:/AIDAR",
                        "dockerfile": "deploy/Dockerfile"
                    },
                    "image": "aidars-perf:latest",
                    "command": ["aidars-coordinator"],
                    "environment": {
                        "AIDAR_COORDINATOR_HOST": "0.0.0.0",
                        "AIDAR_COORDINATOR_PORT": "8000",
                        "AIDAR_COORDINATOR_DB_PATH": "/app/data/coordinator_state.db",
                        "AIDAR_ADMIN_TOKENS": "${AIDAR_ADMIN_TOKENS:?set AIDAR_ADMIN_TOKENS in .env}",
                        "AIDAR_WORKER_BOOTSTRAP_SECRET": "${AIDAR_WORKER_BOOTSTRAP_SECRET:?set AIDAR_WORKER_BOOTSTRAP_SECRET in .env}",
                        "AIDAR_INSECURE_MODE": "${AIDAR_INSECURE_MODE:-0}"
                    },
                    "ports": ["8000:8000"],
                    "volumes": ["coordinator_data:/app/data"],
                    "healthcheck": {
                        "test": ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://127.0.0.1:8000/api/v1/ping')"],
                        "interval": "5s",
                        "timeout": "3s",
                        "retries": 10
                    },
                    "networks": {
                        "aidar_net": {
                            "ipv4_address": "172.29.0.10"
                        }
                    }
                }
            },
            "networks": {
                "aidar_net": {
                    "driver": "bridge",
                    "ipam": {
                        "config": [{"subnet": "172.29.0.0/16"}]
                    }
                }
            },
            "volumes": {
                "coordinator_data": {}
            }
        }

        # Add workers dynamically
        for i in range(num_workers):
            worker_id = f"worker-{i+1}"
            worker_ip = f"172.29.0.{11 + i}"
            compose_dict["volumes"][f"{worker_id}_data"] = {}
            compose_dict["services"][worker_id] = {
                "build": {
                    "context": "C:/AIDAR",
                    "dockerfile": "deploy/Dockerfile"
                },
                "image": "aidars-perf:latest",
                "command": ["aidars-worker"],
                "environment": {
                    "AIDAR_WORKER_BIND_HOST": "0.0.0.0",
                    "AIDAR_WORKER_PORT": "8001",
                    "AIDAR_COORDINATOR_URL": "http://172.29.0.10:8000",
                    "AIDAR_WORKER_CAS_DIR": "/app/data/cas",
                    "AIDAR_WORKER_BOOTSTRAP_SECRET": "${AIDAR_WORKER_BOOTSTRAP_SECRET:?set AIDAR_WORKER_BOOTSTRAP_SECRET in .env}",
                    "AIDAR_INSECURE_MODE": "${AIDAR_INSECURE_MODE:-0}",
                    "AIDAR_WORKER_ID": worker_id,
                    "AIDAR_WORKER_IP": worker_ip
                },
                "depends_on": {
                    "coordinator": {
                        "condition": "service_healthy"
                    }
                },
                "volumes": [f"{worker_id}_data:/app/data"],
                "networks": {
                    "aidar_net": {
                        "ipv4_address": worker_ip
                    }
                }
            }

        compose_path = os.path.join(temp_dir, "docker-compose.yml")
        with open(compose_path, "w") as f:
            yaml.dump(compose_dict, f)

        print(f"Starting isolated cluster with {num_workers} workers...")
        # Use project name to avoid colliding with other environments
        project_name = f"perf_run_{int(time.time())}"
        subprocess.run(["docker", "compose", "-p", project_name, "up", "-d", "--build"], cwd=temp_dir, check=True)

        print("Waiting for cluster to initialize...")
        max_retries = 90
        for _ in range(max_retries):
            time.sleep(1)
            try:
                resp = httpx.get("http://127.0.0.1:8000/api/v1/ping")
                if resp.status_code == 200:
                    resp = httpx.get("http://127.0.0.1:8000/api/v1/workers", headers={"Authorization": "Bearer test_token"})
                    workers = resp.json()
                    active = [w for w in workers if w["status"] == "active"]
                    if len(active) >= num_workers:
                        print(f"Cluster ready with {len(active)} active workers.")
                        break
            except Exception:
                pass
        else:
            subprocess.run(["docker", "compose", "-p", project_name, "logs"], cwd=temp_dir)
            subprocess.run(["docker", "compose", "-p", project_name, "down", "-v"], cwd=temp_dir)
            raise RuntimeError(f"Cluster failed to initialize {num_workers} workers in time.")

        yield project_name

        print("Tearing down cluster...")
        subprocess.run(["docker", "compose", "-p", project_name, "down", "-v"], cwd=temp_dir)
