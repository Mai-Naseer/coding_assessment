import random
from faker import Faker
import psycopg2
import psycopg2.extras

fake = Faker()
print("Start Generating fake data")

# Connect to the PostgreSQL database
conn = psycopg2.connect(
    host="db",
    port="5432",
    database="messages_db",
    user="oxiqa",
    password="oxiqa"
)
cursor = conn.cursor()

# Generate and insert faker data into the messages table
print("Inserting 1000 messages")

for _ in range(1000):
    content = fake.sentence(nb_words=6)
    status = "scheduled"
    scheduled_date = fake.date_time_between(start_date='-1h', end_date='+1h')
    cursor.execute(
        "INSERT INTO messages (content, status, scheduled_date) VALUES (%s, %s, %s)",
        (content, status, scheduled_date)
    )

# Get the inserted message IDs from the messages table
cursor.execute("SELECT id FROM messages")
message_ids = [row[0] for row in cursor.fetchall()]

print("Fetched {} messages".format(len(message_ids)))

print("Inserting message recipients")
# Generate and insert faker data into the recipients table
print("Debugging statement")
for message_id in message_ids:

    num_recipients = random.randint(1, 1000)  # Generate a random number of recipients for each message
    recipients = []

    for _ in range(num_recipients):
        email_address = fake.email()
        recipients.append([message_id, email_address])

    psycopg2.extras.execute_batch(
        cursor,
        "INSERT INTO recipients (message_id, email_address) VALUES (%s, %s)",
        recipients,
        page_size=100
    )

conn.commit()
cursor.close()
conn.close()

print("Complete generating fake data")