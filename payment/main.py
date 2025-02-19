from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.background import BackgroundTasks
from http import HTTPStatus
from redis_om import get_redis_connection, HashModel
import requests
from time import sleep, perf_counter
import logging
from functools import wraps


app = FastAPI()

redis = get_redis_connection(
    host="redis-13870.c339.eu-west-3-1.ec2.redns.redis-cloud.com",
    port=13870,
    password="gLhgvQfeN1PgxLIpWj8hyuRGgnjObWWl",
    decode_responses= True
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S',
)

class Order(HashModel):
    product_id: str
    price: float
    fee: float
    total: float
    order_quantity: int
    status: str

    class Meta:
        database = redis


@app.get("/orders/{pk}")
def getOrderInfo(pk: str):
    return Order.get(pk)

@app.post("/orders")
async def order(request: Request, background_tasks: BackgroundTasks):
    body = await request.json()

    res = requests.get("http://127.0.0.1:8000/products/%s" % body["id"])
    product = res.json()

    product_id= product["pk"]
    price= product["price"]
    fee = 0.2 * price
    available_quantity = body["order_quantity"]
    
    order = Order(
        product_id = product_id,
        price = price,
        fee = fee,
        total = price+fee,
        order_quantity = available_quantity,
        status = "pending"
    )
    order.save()

    logging.info(f"order pending, order_id: {order.pk}")

    background_tasks.add_task(order_completed, order)

    return order

def order_completed(order: Order):
    sleep(10)
    redis.xadd("order_completed", order.dict(), "*")
    order = Order.get(order.pk)
    print(f"order status: {order.status}")
    if order.status != "refunded":
        order.status = "completed"
        order.save()
        logging.info(f"order completed, order_id: {order.pk}")
    elif order.status == "refunded":
        logging.info(f"order refunded, order_id: {order.pk}")
    # sleep(3)
    # order = Order.get(order.pk)
    # print(f"order status: {order.status}")

@app.middleware('http')
async def timing(request: Request, call_next):
    start = perf_counter()
    response = await call_next(request)
    duration = perf_counter() - start
    logging.info(
        '[metric:call.duration] %s %s %d - %.2fs',
        request.method,
        request.url,
        response.status_code,
        duration,
    )
    return response