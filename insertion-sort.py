
# generate random integer values
from numpy.random import seed
from numpy.random import randint

# generate some integers

value=list(randint(0,10,10))
values = value.copy()

#starting from 2nd element
for i in range(1,len(values)):
    key=values[i]
    prior_list=values[:i] #get list of item until nearest element

    for j in range(len(prior_list)): #compare with whole prior list, find index of minimum item higher than i value and move it to that location
        if key<prior_list[j]:
            #values[i],values[j]=values[j],values[i]
            j=0 if j==0 else j
            values.insert(j,key)
            values.pop(i+1) 
            break
            print(values)
