from kafka import KafkaConsumer
from collections import defaultdict
import json
from datetime import datetime, timedelta

consumer = KafkaConsumer(
    'transactions',
    bootstrap_servers='broker:9092',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    group_id='anomaly-detector'  
)

# przechowujemy zdarzenia per user
user_events = defaultdict(list)

print("Nasłuchuję na podejrzane zachowania użytkowników...")

for message in consumer:
    event = message.value
    
    user_id = event['user_id']
    timestamp = datetime.fromisoformat(event['timestamp'])
    
    # dodaj nowe zdarzenie
    user_events[user_id].append(timestamp)
    
    # usuń stare zdarzenia (>60s)
    one_minute_ago = timestamp - timedelta(seconds=60)
    user_events[user_id] = [
        t for t in user_events[user_id] if t >= one_minute_ago
    ]
    
    # sprawdź liczbę transakcji
    if len(user_events[user_id]) > 3:
        print(f"🚨 ALERT: {user_id} wykonał {len(user_events[user_id])} transakcji w 60s!")
        print(event)
