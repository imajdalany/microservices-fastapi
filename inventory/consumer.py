from main import redis, Product
from time import sleep

key ="order_completed"
group= "inventory_group"

try:
    redis.xgroup_create(key, group, mkstream=True)
except Exception as e:
    print(e)

while True:
    try:
        results = redis.xreadgroup(group, key, {key: ">"}, None)
        
        if results != []:
            for result in results:
                order = result[1][0][1]

                try:
                    product = Product.get(order["product_id"])
                    product.quantity -= int(order["order_quantity"])
                    product.save()

                    # redis.xack(key, group)
                    print(f"order completed {product}")
                except:
                    # redis.xack(key, group)
                    print(f"refunding order {order['product_id']}")
                    redis.xadd("refund_order", order, "*")
                    
                    
    except Exception as e:
        print("Exception occured:")
        print(e)