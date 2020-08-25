import asyncio
import uvicorn
from confluent_kafka import KafkaException
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from aio_producer import AIOProducer

app = FastAPI()


class Item(BaseModel):
    name: str


producer = None


@app.on_event("startup")
async def startup_event():
    global producer
    producer = AIOProducer(
        {"bootstrap.servers": "localhost:9092"})


@app.on_event("shutdown")
def shutdown_event():
    producer.close()


@app.post("/items")
async def create_item(item: Item):
    try:
        result = await producer.produce("items", item.name)
        return {"timestamp": result.timestamp()}
    except KafkaException as ex:
        raise HTTPException(status_code=500, detail=ex.args[0].str())

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=8000)
