#!/usr/bin/env python3
import pika
import sys
import random
import time

# Wait for rabbitmq to come up
time.sleep(10)

# Create RabbitMQ communication channel
connection = pika.BlockingConnection(pika.ConnectionParameters(host="localhost"))
channel = connection.channel()


channel.queue_declare(queue="HOLA", durable=True)

for i in range(100):
    message = "72,7d855b682bc73090e8b25dfd3a413a96,2022-04-16,2022-04-17,ATL,DEN,VAA0AKEN,PT3H25M,0,False,False,True,262.33,296.61,1,1207.0,1650216900,2022-04-17T13:35:00.000-04:00,1650229200,2022-04-17T15:00:00.000-06:00,DEN,ATL,United,UA,Embraer 175 (Enhanced Winglets),12300,1207,coach"
    channel.basic_publish(
        exchange="",
        routing_key="HOLA",
        body=message,
        properties=pika.BasicProperties(
            delivery_mode=2,  # make message persistent
        ),
    )
    print(" [x] Sent %r" % message)
    time.sleep(1)

connection.close()
