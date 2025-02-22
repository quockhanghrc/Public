import random

from datetime import datetime,timedelta
def smallest_positive(input):
    randomlist = []
    for i in range(0,input):
        n = random.randint(-input,input)
        randomlist.append(n)
        
    randomlist.sort()
    flag_change=0
    flag_positive=0
    positive_random=[]

    for i in randomlist:
        if i>0:
            positive_random.append(i)
    
    smallest_number=positive_random[0]
    while flag_change<1:
        for i in positive_random:
            if smallest_number==i:
                smallest_number=smallest_number+1
                print(i)
            else: flag_change=1
    return smallest_number

time_table=[]

for i in [10,100,1000,10000,100000,1000000,10000000]:
        print(i)
        start_time=datetime.now()
        a=smallest_positive(i)
        end_time=datetime.now()
        time_table.append((end_time-start_time).seconds)
        
