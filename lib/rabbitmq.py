#-- coding:utf-8 --
import pika

class Rabbitmq():
    def __init__(self,user,password,host,port):
        self.user = user
        self.password = password
        self.host = host
        self.port = port
        self.__channel()


    def __channel(self):
        try:
            credentials = pika.PlainCredentials(self.user,self.password)
            parameters = pika.ConnectionParameters(host=self.host, port = self.port, credentials=credentials)
            self.connection = pika.BlockingConnection(parameters)
            self.channel = self.connection.channel()
            print('Rabbitmq connect success...')
        except Exception as e:
            print(e)

    def declare(self,queuename):
        self.channel.queue_declare(queue=queuename)

    def publish(self,content,queuename):
        self.channel.basic_publish(
                                    exchange='',
                                    routing_key=queuename,
                                    body = str(content),
                                            )
        print(" [x] Sent !")


    def close(self):
        self.connection.close()

    def consume(self,queuename,callbak):
        channel.basic_consume(
                        queuename,
                        callbak,
                        consumer_tag='test',
                        auto_ack=True)
        print(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()


    def get_channal(self):
        return self.channel

    def get_connection(self):
        return self.connection