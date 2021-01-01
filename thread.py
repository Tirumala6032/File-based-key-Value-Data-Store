import threading
import Datastore as data
obj=data.Datastore()
threads=[]
for _ in range(1):
    t=threading.Thread(target=obj.create,args=['tirumala','{name:tirumala,age:21}',1000])
    t.start()
    threads.append(t)
for thread in threads:
    thread.join()  
threads=[]
for _ in range(1):
    t=threading.Thread(target=obj.read,args=['tirumala'])
    t.start()
    threads.append(t)
for thread in threads:
    thread.join()

threads=[]
for _ in range(3):
    t=threading.Thread(target=obj.delete,args=['tirumala'])
    t.start()
    threads.append(t)
for thread in threads:
    thread.join()
