
import asyncio
import aiohttp
import ssl

async def check_market_data():
    url = "https://gamma-api.polymarket.com/markets?active=true&limit=1"
    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE
    
    async with aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False)) as session:
        async with session.get(url) as response:
            data = await response.json()
            if data:
                print(data[0])

if __name__ == "__main__":
    asyncio.run(check_market_data())
