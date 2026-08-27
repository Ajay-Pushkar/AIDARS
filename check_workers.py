import asyncio
import httpx

async def main():
    async with httpx.AsyncClient() as client:
        resp = await client.get("http://127.0.0.1:8000/api/v1/workers")
        print("Workers:", resp.json())
        
if __name__ == '__main__':
    asyncio.run(main())
