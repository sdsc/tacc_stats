#!/usr/bin/env python
import pika
import os, sys
import time
import tacc_stats.cfg as cfg
from fcntl import flock, LOCK_EX, LOCK_NB

def on_message(channel, method_frame, header_frame, body):

    try:
        message = body.decode()    
    except: 
        print("Unexpected error at decode:", sys.exc_info()[0])
        return

    if message[0] == '$': host = message.split('\n')[1].split()[1]       
    else: host = message.split()[2]

    host_dir = os.path.join(cfg.archive_dir, host)
    if not os.path.exists(host_dir):
        os.makedirs(host_dir)
    
    current_path = os.path.join(host_dir, "current")
    if message[0] == '$':
        if os.path.exists(current_path):
            os.unlink(current_path)

        with open(current_path, 'w') as fd:
            link_path = os.path.join(host_dir, str(int(time.time())))
            if os.path.exists(link_path):
                os.remove(link_path)
            os.link(current_path, link_path)

    with open(current_path, 'a') as fd:
        fd.write(message)

    channel.basic_ack(delivery_tag=method_frame.delivery_tag)

with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "listend_lock"), "w") as fd:
    try:
        flock(fd, LOCK_EX | LOCK_NB)
    except IOError:
        print("listend is already running")
        sys.exit()

    parameters = pika.ConnectionParameters(cfg.rmq_server)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()
    channel.basic_consume(cfg.rmq_queue, on_message)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
    connection.close()
