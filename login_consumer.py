import pika
import json

# Conexão
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Exchange e fila
channel.exchange_declare(exchange='user_events', exchange_type='direct')
channel.queue_declare(queue='login_queue')
channel.queue_bind(exchange='user_events', queue='login_queue', routing_key='user.login')

# Callback
def callback(ch, method, properties, body):
    data = json.loads(body)
    print(f"[LOGIN] {data['user']} acabou de fazer login!")

channel.basic_consume(queue='login_queue', on_message_callback=callback, auto_ack=True)

print(" [*] Aguardando eventos de login...")
channel.start_consuming()
