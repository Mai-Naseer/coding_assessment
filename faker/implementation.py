import random
from faker import Faker
import psycopg2
import psycopg2.extras
import sys
import pprint
import pandas as pd
import datetime
from multiprocessing import Process, Pool
import multiprocessing

#This function is designed to handle constraint 2 in the readme
#Handling concurrent modernisation of the queue
#This function is called to add a message into the queue
#It simulates the scenario where multiple messages are added to the queue concurrently in different pools

def add_messages(cursor):
    content = "THIS IS A TEST WORD I ADDED"
    status = "scheduled"
    scheduled_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute(
        "INSERT INTO messages (content, status, scheduled_date) VALUES (%s, %s, %s)",
        (content, status, scheduled_date)
    )

#Simulates constraint of not being able to modify messages while on dispatch

def modify_message(cursor, message_id):
    global dispatch_list
    #This method is invoked when user tries to modify a message
    #In dispatch_list we have the messages that are being dispatched at the moment.
    #We have some logic to check if the message that the user is wanting to update is not already in the ditpatch list
    #If it is, we do not allow the modificiation and otherwise update the message in the queue
    #This is happening concurrently on multiple processes to cater for the constraint

    if message_id in dispatch_list:
        print('message is being dispatched... we cannot modify it at the moment.') 
    else:
        cursor.execute("UPDATE messages SET content = 'MODIYING THE MESSAGE TO THIS' where id= %s", (message_id, )) 


#Printing the results in aa data frame (pandas) and returning the data frame
def print_table(cursor, result):
    columns = [desc[0] for desc in cursor.description]
    print_dataframe_of_results= pd.DataFrame(result, columns=columns)
    print(print_dataframe_of_results)
    return print_dataframe_of_results

#In this function, we will fetch the recipients from the recipients table who will receive the message with the given ID
#And dispatch the message to them
#Update the delivered for message sent and received by receiver. 

def dispatch_message(dispatch_msg_id):
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM recipients WHERE message_id = %s AND  status IS NULL ", (dispatch_msg_id, ))
    results=cursor.fetchall()
    print('sending message id: ' + str(dispatch_msg_id) +  ' to respective ' + str(len(results)) +' recipients ... ' )
    
    
    delivered_date=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    cursor.execute('UPDATE recipients SET status = %s, delivered_date = %s WHERE message_id= %s ',('delivered', delivered_date, dispatch_msg_id ))
    cursor.execute('UPDATE messages SET status = %s, delivered_date = %s WHERE id = %s',('delivered', delivered_date, dispatch_msg_id))
    
    print('completed dispatching of message ' + str(dispatch_msg_id))
    # dispatch_list.remove(dispatch_msg_id)
    conn.commit()
    return True

#Each time we want to dispatch messages, we do 3 at a time for efficiency and to accommodate for wait time
def modify_queue(send_queue):
    global dispatch_list
    dispatch_list= send_queue


#Initialise a connection for the dataabase.
conn = psycopg2.connect(
    host="localhost",
    port="5433",
    database="messages_db",
    user="oxiqa",
    password="oxiqa"
)
dispatch_list= None


if __name__ =='__main__':
    run_forever=True 
    while run_forever:
        cursor = conn.cursor()
        # The important task is to sort the messages basaed ob the scheduled date time stamp.
        # The lines below do this sorting.
        # We will continue to select messages which are not yet delivered and fetch this data
        cursor.execute("SELECT * FROM messages ORDER BY scheduled_date")
        cursor.execute("SELECT * FROM messages WHERE status != 'delivered'")

        results=cursor.fetchall()
        result_dataframe=print_table(cursor, results)

        if result_dataframe.empty:
            run_forever=False
    
        send_queue=result_dataframe.iloc[:3]['id'].to_list()
        modify_queue(send_queue)

        #Concurrent pooling to allow multiple processes to run
        with Pool() as pool:
            result=pool.map(dispatch_message, send_queue)
        
        #If we get a random floaat > 0.5, we will try to modify a message and validate that it does not allow modification of a message in a dispatch queue
        #Else we check that we can generate a new row of data to the queue and yet it is sorted and scheduled and processes in the correct order and adding a new line does not make a difference to this.
        random_check = random.random()
        if random_check > 0.5:
            message_id_modify= (result_dataframe['id'].to_list()) 
            if len(message_id_modify) > 1:
                print("THE LIST OF MESSAGE IDS WE WANT TO MODIFY", message_id_modify)
                for message_id in message_id_modify:
                    modify_message(cursor, message_id)
        else:
            add_messages(cursor)



        modify_queue([])
        if len(dispatch_list) > 1:
            print('messages ' + str(dispatch_list) + 'released from dispatch queue and can be used for modifications')

        else:
            print('Thank you. All messages delivered')   
        conn.commit()
        cursor.close()
        
        
        


    conn.close()     


