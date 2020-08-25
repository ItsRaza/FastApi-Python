from fastapi import FastAPI
import asyncio

app = FastAPI()


@app.get("/")
async def read_root():
    print('Started')
    await asyncio.sleep(5)
    print('Finished')
    return {"Hello": "World"}
