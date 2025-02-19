from main import redis, Order
import logging

key ="refund_order"
group= "payment_group"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%S',
)

try:
    redis.xgroup_create(key, group, mkstream=True)
except Exception as e:
    print(e)

while True:
    try:
        results = redis.xreadgroup(group, key, {key: ">"}, None)
        
        if results != []:
            print(results)
            for result in results:
                order_to_refund = result[1][0][1]
                order = Order.get(order_to_refund["pk"])

                order.status = "refunded"
                order.save()
                
                logging.info(f"Order refunded, Order id: {order_to_refund['pk']}")
                
                # redis.xack(key, group, result[1][0][1])
                    
    except Exception as e:
        print("\tExeption occured: \n" + str(e))