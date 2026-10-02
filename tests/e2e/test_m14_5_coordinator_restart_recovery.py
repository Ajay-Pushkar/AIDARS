import os
import subprocess
import time
import httpx
import pytest
from aidars.distributed.models import WorkloadSpec

@pytest.fixture(scope="module", autouse=True)
def docker_cluster():
    deploy_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../deploy"))
    print("Bringing down existing cluster...")
    subprocess.run(["docker", "compose", "down", "-v"], cwd=deploy_dir)

    env_path = os.path.join(deploy_dir, ".env")
    with open(env_path, "w") as f:
        f.write("AIDAR_ADMIN_TOKENS=test_token\n")
        f.write("AIDAR_WORKER_BOOTSTRAP_SECRET=test_secret\n")
        f.write("AIDAR_INSECURE_MODE=1\n")

    print("Starting cluster...")
    subprocess.run(["docker", "compose", "up", "-d", "--build"], cwd=deploy_dir, check=True)

    print("Waiting for cluster to initialize...")
    max_retries = 30
    for i in range(max_retries):
        time.sleep(1)
        try:
            resp = httpx.get("http://127.0.0.1:8000/api/v1/ping")
            if resp.status_code == 200:
                resp = httpx.get("http://127.0.0.1:8000/api/v1/workers", headers={"Authorization": "Bearer test_token"})
                workers = resp.json()
                active = [w for w in workers if w["status"] == "active"]
                if len(active) == 3:
                    print(f"Cluster ready with 3 active workers.")
                    break
        except Exception:
            pass
    else:
        subprocess.run(["docker", "compose", "logs"], cwd=deploy_dir)
        subprocess.run(["docker", "compose", "down", "-v"], cwd=deploy_dir)
        raise RuntimeError("Cluster failed to initialize properly in time.")

    yield deploy_dir

    print("Tearing down cluster...")
    subprocess.run(["docker", "compose", "down", "-v"], cwd=deploy_dir)

def test_m14_5_coordinator_restart_recovery(docker_cluster):
    """
    M14.5 COORDINATOR RESTART AND RECOVERY
    Prove that AIDAR can recover correctly when the Coordinator process/container 
    is unexpectedly terminated while a workload is in flight.
    """
    deploy_dir = docker_cluster
    job_id = f"m14-5-coord-restart-{int(time.time())}"
    workload_id = f"{job_id}-chunk-0"
    
    # We use a command that sleeps for 15 seconds, then creates an output.
    # This gives us plenty of time to kill the coordinator container while it's executing.
    spec = WorkloadSpec(
        workload_id=workload_id,
        job_id=job_id,
        task_type="test",
        min_ram_bytes=1024,
        estimated_duration_seconds=10.0,
        parameters={
            "command": "python -c \"import time; import os; time.sleep(15); os.makedirs('outputs', exist_ok=True); open('outputs/recovery.txt', 'w').write('survived'); print('m14-5-success')\""
        }
    )
    
    headers = {"Authorization": "Bearer test_token"}
    payload = {
        "specs": [spec.model_dump(mode="json")]
    }
    
    resp = httpx.post("http://127.0.0.1:8000/api/v1/jobs/submit", json=payload, headers=headers)
    assert resp.status_code == 202, f"Failed to submit job: {resp.text}"
    
    # Wait until it is placed and running
    selected_worker_id = None
    max_wait = 15
    for _ in range(max_wait):
        time.sleep(1)
        resp = httpx.get(f"http://127.0.0.1:8000/api/v1/workloads/{workload_id}", headers=headers)
        if resp.status_code == 200:
            data = resp.json()
            if data["state"] == "placed" or data["state"] == "running":
                assert "placement" in data
                selected_worker_id = data["placement"]["selected_worker_id"]
                break
    else:
        pytest.fail("Workload did not transition to placed/running state in time.")
        
    print(f"Workload in-flight on: {selected_worker_id}. Killing coordinator now...")
    
    # Kill the coordinator container
    subprocess.run(["docker", "compose", "kill", "coordinator"], cwd=deploy_dir, check=True)
    
    # Verify coordinator is down
    try:
        httpx.get("http://127.0.0.1:8000/api/v1/ping", timeout=2.0)
        pytest.fail("Coordinator should be down, but it responded.")
    except httpx.RequestError:
        print("Coordinator successfully killed.")
        
    # Start it back up
    print("Restarting coordinator...")
    subprocess.run(["docker", "compose", "start", "coordinator"], cwd=deploy_dir, check=True)
    
    # Wait for coordinator to be healthy again
    max_retries = 30
    for i in range(max_retries):
        time.sleep(1)
        try:
            resp = httpx.get("http://127.0.0.1:8000/api/v1/ping")
            if resp.status_code == 200:
                print(f"Coordinator restarted successfully.")
                break
        except Exception:
            pass
    else:
        pytest.fail("Coordinator failed to restart properly in time.")
        
    # Wait for the workload to finish (it should be recovered and eventually complete)
    max_wait_completion = 60
    final_data = None
    for _ in range(max_wait_completion):
        time.sleep(2)
        resp = httpx.get(f"http://127.0.0.1:8000/api/v1/workloads/{workload_id}", headers=headers)
        if resp.status_code == 200:
            data = resp.json()
            if data["state"] == "completed":
                final_data = data
                break
            elif data["state"] == "failed":
                subprocess.run(["docker", "compose", "logs"], cwd=deploy_dir)
                pytest.fail(f"Workload failed completely instead of recovering: {data}")
    else:
        pytest.fail("Workload did not complete within the expected time after recovery.")
        
    # Verify the attempt history
    attempts = final_data.get("attempts", [])
    assert len(attempts) >= 2, "Expected at least 2 attempts (1 failed/lost due to coord restart + 1 redrive)"
    
    first_attempt = attempts[0]
    second_attempt = attempts[-1]
    
    assert first_attempt["worker_id"] == selected_worker_id
    assert first_attempt["status"] == "lost"
    assert "coordinator restarted" in first_attempt.get("failure_reason", "")
    
    assert second_attempt["status"] == "succeeded"
    
    # Verify result stdout
    assert "result" in final_data
    assert final_data["result"]["success"] is True
    assert "m14-5-success" in final_data["result"]["stdout_snippet"]
    
    # Verify job state
    job_resp = httpx.get(f"http://127.0.0.1:8000/api/v1/jobs/{job_id}", headers=headers)
    assert job_resp.status_code == 200
    job_data = job_resp.json()
    
    assert job_data["state"] == "completed"
    assert job_data["completed"] == 1
    assert len(job_data["output_asset_hashes"]) > 0, "Output artifact was not mapped to job"
