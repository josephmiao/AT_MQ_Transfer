import pika
import threading

########################################
# ip
address = 'localhost'
# queue
queue_name = 'queue_test'
# 插入的内容
message = 'hello_test'
# 插入多少条消息
max_insert = 1
# 线程数，每个线程跑max_insert数量的消息。
max_thread = 1
########################################


def insertmq(max_ins, msg):
    # 连接MQ
    connection = pika.BlockingConnection(pika.ConnectionParameters(host=address))
    channel = connection.channel()
    # 队列持久化，重启不消失。
    channel.queue_declare(queue=queue_name, durable=True)
    # 循环插入消息
    for m in range(max_ins):
        channel.basic_publish(exchange='',
                      routing_key=queue_name,
                      body=msg
                      )

    # print('Insert %d messages.' % max_ins)
    connection.close()

threads = []

for t in range(max_thread):
    target = threading.Thread(target=insertmq, args=(max_insert, message))
    threads.append(target)


if __name__ == '__main__':
    for t in threads:
        t.start()
    for t in threads:
        t.join()

print('Total: %d threads, %d messages published.' % (max_thread, max_insert*max_thread))
