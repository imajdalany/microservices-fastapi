from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from redis_om import get_redis_connection, HashModel
from http import HTTPStatus

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://127.0.0.1:3000"],
    allow_methods= ["*"],
    allow_headers=["*"]
)

redis = get_redis_connection(
    host="redis-13870.c339.eu-west-3-1.ec2.redns.redis-cloud.com",
    port=13870,
    password="gLhgvQfeN1PgxLIpWj8hyuRGgnjObWWl",
    decode_responses= True
)

class Product(HashModel):
    name: str
    price: float
    quantity: int

    class Meta:
        database = redis
 
def find_format_product(pk: str):
    product = Product.get(pk)

    return {
        "id":product.pk,
        "name":product.name,
        "price":product.price,
        "available_quantity": product.quantity
    }

@app.get("/products")
def getProducts():
    return [find_format_product(pk) for pk in Product.all_pks()]

@app.post("/products")
def addProduct(product: Product):
    return product.save()

@app.get("/products/{pk}")
def getProduct(pk: str):
    try:
        return Product.get(pk)
    except :
        raise HTTPException(
            status_code= HTTPStatus.BAD_REQUEST,
            detail="Product does not exist"
        )
    
@app.delete("/products/{pk}")
def deleteProduct(pk:str):
    try:
        deletedProduct = Product.get(pk)
        Product.delete(pk)

        return {
            "Deleted product": deletedProduct}
    except:
        raise HTTPException(
            status_code= HTTPStatus.BAD_REQUEST,
            detail="Product does not exist"
        )