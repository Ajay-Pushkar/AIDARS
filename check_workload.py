import asyncio
import httpx

async def main():
    async with httpx.AsyncClient() as client:
        resp = await client.get("http://127.0.0.2:8000/api/v1/workloads/lan-fault-task-final")
        print("Status:", resp.json())
        
if __name__ == '__main__':
    asyncio.run(main())
