import pika
import threading

########################################
# ip
address = 'localhost'
# queue
queue_name = 'queue_test'
# 线程数，每个线程都会消费消息。
max_thread = 1
########################################

def readmq():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=address))
    channel = connection.channel()
    # 队列持久化，重启不消失。
    channel.queue_declare(queue=queue_name, durable=True)
    print('Waiting for message. To exit press CTRL + C')
    # 每次只处理一条消息
    channel.basic_qos(prefetch_count=1)
    # no_ack控制是否消息响应确认
    channel.basic_consume(callback, queue=queue_name, no_ack=True)s
    channel.start_consuming()
    # 该语句后面不再执行


def callback(ch, method, properties, body):
    print(body)


threads = []

for t in range(max_thread):
    target = threading.Thread(target=readmq, args=())
    threads.append(target)

# 线程调试
print('%d threads starts.' % max_thread)

if __name__ == '__main__':
    for t in threads:
        t.start()
    for t in threads:
        t.join()
