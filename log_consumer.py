import pika
import json

# Conexão
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Exchange e fila
channel.exchange_declare(exchange='user_events', exchange_type='direct')
channel.queue_declare(queue='log_queue')

# Bind para todos os eventos
for key in ['user.login', 'user.upload', 'user.logout']:
    channel.queue_bind(exchange='user_events', queue='log_queue', routing_key=key)

# Callback
def callback(ch, method, properties, body):
    data = json.loads(body)
    print(f"[LOG] {data['user']} executou o evento: {data['event']}")

channel.basic_consume(queue='log_queue', on_message_callback=callback, auto_ack=True)

print(" [*] Aguardando todos os eventos...")
channel.start_consuming()
