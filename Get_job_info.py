#Get basic information (job title, location, company, salary) for desired amount of pages on Careerbuilder

import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import requests
import time


input_n=input("Enter number of pages you need:")
input_n=int(input_n)
start=time.time()
page_link=[]

for n in list(np.arange(start=1,stop=input_n+1,step=1)):
    page_link.append("https://careerbuilder.vn/viec-lam/tat-ca-viec-lam-trang-"+str(n)+"-vi.html")
class_list=["job","namecom","location","salary"]
job_data={}
job_listing=pd.DataFrame()

job_content=[]

for page in page_link:
    for class_item in class_list:
        
        page_response=requests.get(page,timeout=5)
        page_content=BeautifulSoup(page_response.content,"html.parser")
        job_content=page_content.find_all(class_=class_item)
        job_1=[]
        for content in job_content:
            job_1.append(content.get_text())
            job_data[class_item]=job_1
        job_list=pd.DataFrame(job_data)
    job_listing=job_listing.append(job_list)

job_listing=job_listing.reset_index()
del job_listing["index"]
end=time.time()
print(end-start)
