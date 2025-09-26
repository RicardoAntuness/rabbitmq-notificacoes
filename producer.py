import pika
import json
from datetime import datetime

# Conexão
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Declaração da exchange
channel.exchange_declare(exchange='user_events', exchange_type='direct')

# Mensagens de exemplo
events = [
    {"user": "Ricardo", "event": "user.login"},
    {"user": "Ricardo", "event": "user.upload"},
    {"user": "Ricardo", "event": "user.logout"},
]

for e in events:
    message = {
        "user": e["user"],
        "event": e["event"],
        "timestamp": datetime.utcnow().isoformat() + "Z"
    }
    channel.basic_publish(
        exchange='user_events',
        routing_key=e["event"],
        body=json.dumps(message)
    )
    print(f"[x] Enviado: {message}")

connection.close()
