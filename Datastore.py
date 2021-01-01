import time
import json
import os
import threading
class Datastore:
    def __init__(self):
        self.name='\tFile Based Key-Value Datastore\t'
        self.lock = threading.Lock()
        print(self.name)
        
    # create() method is used to generate the File Based key-Value Datastore.
    # The data is stored in JSON format ( Key-value pairs ).
    # dump() and load() methods are used to store and retrieve the data in JSON format from a File.
    def create(self,key,value,time_to_live=0):
        self.lock.acquire()
        print('\n\t\t\tCreation Method Executing in Datastore\t\t\t')
        try:
            open('data.txt','a+')
            if os.stat('data.txt').st_size==0:
                with open('data.txt','w') as outfile:
                    json.dump({},outfile)
            with open('data.txt')as data_file:
                data=json.load(data_file) 
            
            if key in data:
                data_temp=data[key]
                if data_temp[1]!=0:
                    if data_temp[1]<time.time():
                        del data[key]
                        if key.isalpha():
                            if len(data)<(1024*1024*1024) and len(value)<=(16*1024*1024):
                                if time_to_live!=0:
                                    data_temp=[{key:value},time.time()+time_to_live]
                                else:
                                    data_temp=[{key:value},time_to_live]
                                if len(key)<=32:
                                    data[key]=data_temp
                                    print('Key created successfully')
                                else:
                                    print('Error: Key size must be  <=32 characters')
                                    print('Enter a valid key name')
                            else:
                                print('Error: Memory Limit Exceeded')
                        else:
                            print('Error: Invalid key')
        
                        with open('data.txt','w') as outfile:
                            json.dump(data,outfile)
                        
                    else:
                        print('Error: The key '+key+' is already exists in Datastore')
                else:
                    print('Error: The key '+key+' is already exists in Datastore')
            else:
                if key.isalpha():
                    if len(data)<(1024*1024*1024) and len(value)<=(16*1024*1024):
                        if time_to_live!=0:
                            data_temp=[{key:value},time.time()+time_to_live]
                        else:
                            data_temp=[{key:value},time_to_live]
                        if len(key)<=32:
                            data[key]=data_temp
                            print('Key created successfully')
                        else:
                            print('Error: Key size must be  <=32 characters')
                            print('Enter a valid key name')
                    else:
                        print('Error: Memory Limit Exceeded')
                else:
                    print('Error: Invalid key')
        
            with open('data.txt','w') as outfile:
                json.dump(data,outfile)
        finally:
            self.lock.release()
    # read() method is used to search the data in datastore with key.
    # Tt checks for key existance and Time-To-Live .if both are there then it returns the Data for the key.
    # If key and Time-To-Live are not active then an Error message was Displayed.
            
    def read(self,key):
        print('\n\t\t\tRead Method Executing in Datastore\t\t\t')
        self.lock.acquire()
        try:
            with open('data.txt')as data_file:
                data=json.load(data_file)
            if key not in data:
                print('Error: The key '+key+' doesn\'t exists')
            else:
                data_temp=data[key]
                if data_temp[1]!=0:
                    if data_temp[1]>time.time():
                        string=str(key)+':'+str(data_temp[0])
                        print(string)
                    else:
                        del data[key]
                        print('Error: Time-To-Live for '+key+' has expired')
                        with open('data.txt','w') as outfile:
                            json.dump(data,outfile)
                else:
                    string=str(key)+':'+str(data_temp[0])
                    print(string)
        finally:
            self.lock.release()
    # delete() method used to delete the data from datastore with its equivalent key.
    # If the key exists ,it checks for Time-To-Live. Time-To-Live is active then it deletes te key in the datastore otherwise it gives an error message
    # If the key doesn't exist in the Datastore then it returns an Error message as 'Key doesn't exist'
                
    def delete(self,key):
        self.lock.acquire()
        print('\n\t\t\tDeletion Method Executing in Datastore\t\t\t')
        try:
            with open('data.txt')as data_file:
                data=json.load(data_file)
            if key not in data:
                print('Error: this key '+key+' doesn\'t exists')
            else:
                data_temp=data[key]
                if data_temp[1]!=0:
                    if data_temp[1]>time.time():
                        del data[key]
                        print('The key '+key+' is successfully deleted')
                    else:
                        del data[key]
                        print('Error: Time-To-Live for '+key+' has expired')
                else:
                    del data[key]
                    print('The key '+key+' is successfully deleted')
            with open('data.txt','w') as data_file:
                json.dump(data,data_file)
        finally:
            self.lock.release()

# To run the code manually remove the comments for below code

'''obj=Datastore()
flag=True
while flag:
    print('\n\t**** MENU ****')
    print('1 Create\n2 Read\n3 Delete\n4 Exit')
    print('Enter Your Choice : ',end='')
    ip=input()
    if ip=='1':
        print('Enter Key : ',end='')
        key=input()
        print('\nEnter Key-Value : ',end='')
        value=input()
        print('Enter Time To Live : ',end='')
        t=int(input())
        obj.create(key,value,t)
    elif ip=='2':
        print('\nEnter Key to search : ',end='')
        key=input()
        obj.read(key)
    elif ip=='3':
        print('\nEnter Key to delete : ',end='')
        key=input()
        obj.delete(key)
    elif ip=='4':
        flag=False
    else:
        print('\nInvalid Choice')'''
