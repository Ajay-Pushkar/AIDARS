import asyncio
import httpx

async def main():
    spec = {
        "workload_id": "lan-fault-task-no-assets",
        "task_type": "python_script",
        "parameters": {"script_code": "import time; time.sleep(10)"},
        "min_cpu_cores": 1,
        "input_asset_hashes": []
    }
    
    print("Submitting workload to coordinator...")
    async with httpx.AsyncClient() as client:
        resp = await client.post("http://127.0.0.2:8000/api/v1/workloads/submit", json=spec)
        print("Submit response:", resp.json())
        
if __name__ == '__main__':
    asyncio.run(main())
