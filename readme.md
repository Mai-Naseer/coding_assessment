# Problem
You are tasked with designing an algorithm to dispatch messages from a queue to a set of given recipients. Messages with varying numbers of recipients are added to the queue(messages table) concurrently by multiple clients. Your algorithm should ensure that all queued messages are attended to fairly avoiding longer wait time. Additionally, it should handle any conflicts that may arise due to concurrent modifications of the queued messages. To achieve this you are provided a database with following tables and sample data.

Table: messages
```
id: Integer
content: String
status: String
scheduled_date: DateTime
delivered_date: DateTime
```
Table: recipients
```
id: Integer
message_id: (Integer - Foreign Key to messages.id)
email_address: String
status: String
delivered_date: DateTime
```
# Constraints:
1- Each message can have a varying number of recipients.
2- The algorithm should handle concurrent modifications of the queue by multiple clients, ensuring fair dispatch to the given recipients.
3- No message should be modified while on dispatch.


- Focus on how to process the queue and ignore how the message is actually delivered.
- You can use any programming language to solve this problem. Good luck with your implementation!

# Setup

run to initialize database and seed data  
```docker-compose -f docker-compose.yml -f faker/faker-compose.yml up```  
consecutive runs should only run the database  
```docker-compose up```  

# Implementation reflections
- My implementation allows users to efficiently process the data in the message table considering the scheduled date.
- It takes into account the changes from multiple clients simultaneously
- It does not allow a user to make modifications to a message being dispatched.
- I run a dummy version of faker_data_generator.py called fake/test.py to create a smaller set of data. (Included in this repo)
- I then run fake/implementation.py to process message delivery.
- Please note that the concurrent processes were set to a minimum for the smaller subset and I have taken into account the scaling for the original data size.

# Limitations

- Can one recipient receive the same message? Given the database schema, only one unique recipient exists in the table.


