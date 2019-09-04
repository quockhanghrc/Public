import pandas as pd
import numpy as np
import datetime


imported_df=pd.read_csv("D:/User behavior sample 2.csv")
#net value is same as order value if it was delivered otherwise 0
imported_df["Net Value"]=np.where(imported_df["Status"]=="Cancel",0,imported_df["Order Value"])

weekday_list=[]
for item in imported_df["OrderDate"]:
    weekday_list.append(datetime.datetime.strptime(item,'%m/%d/%y').strftime('%A'))
weekday_list=pd.DataFrame(weekday_list,columns=["Weekday"])
imported_df=pd.concat([imported_df,weekday_list],axis=1)
weekday_sort=["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

#use percentile to clearly find the gap
#cancelled orders likely have longer distance (~>=4km)
table_1=pd.DataFrame()
for number in range(60,100,5):
    pivot_frame=imported_df.pivot_table(index="Status",values="Distance",aggfunc=lambda x: np.percentile(x,number))
    pivot_frame.columns=[number]
    table_1=pd.concat([table_1,pivot_frame],axis=1)

#successful orders via Now have more discounts and higher average discount value
table_2=imported_df[imported_df["Status"]=="Delivered"].pivot_table(columns="AppType",index="Category",values="TotalDiscount",aggfunc=["mean","count"])

#cancelled orders likely have high amount of items, low order value and high discount value
table_3=imported_df.pivot_table(columns=["Status"],index="Category",values=["TotalItem","TotalDiscount","Order Value"],aggfunc=["mean"])

#discounted orders have higher value than non-discount ones
imported_df["DiscountType"]=np.where(imported_df["TotalDiscount"]==0,"Non-Discount","Discounted")
table_4=imported_df[imported_df["Status"]=="Delivered"].pivot_table(columns=["Status","DiscountType"],index="Category",values=["Order Value"],aggfunc=["mean"])

#orders from Now have higher cancel rate than Foody
table_5=imported_df.pivot_table(columns=["AppType"],index="Status",values=["Order Value"],aggfunc=["count"])

#weekends has higher cancel rate than weekdays
table_6=imported_df.pivot_table(columns=["Status"],index="Weekday",values=["Order Value"],aggfunc=["count"]).apply(lambda r: r/r.sum(),axis=1)
table_6=table_6.reindex(weekday_sort)

#Users usually use Web to create big orders and iOS is the OS that have highest 
table_7=imported_df.pivot_table(index="DeviceOSType",values=["Net Value"],aggfunc=["count",np.sum,"mean"])

#Cash still have higher cancel rate than non-cash method (VNPay and PayNow excluded because they didn't have much orders)
table_8=pd.concat([imported_df.pivot_table(index="PaymentMethod",columns="Status",values=["Net Value"],aggfunc=["count"]),imported_df.pivot_table(index="PaymentMethod",columns="Status",values=["Net Value"],aggfunc=["count"]).apply(lambda r: r/r.sum(),axis=1)],axis=1)

table_9=imported_df.pivot_table(index="PaymentMethod",values=["Net Value"],aggfunc=["count",np.sum,"mean"])
print(table_9)

#get list of top users (20% highest in total successful order value)
grouped_user=imported_df.pivot_table(index="UserId",values="Net Value",aggfunc=np.sum)
top_user=list(grouped_user[grouped_user["Net Value"]>=np.percentile(grouped_user["Net Value"],80)].index)

imported_df["Top Users"]=np.where(imported_df["UserId"].isin(top_user),"Top Users","Normal Users")

top_user_df=imported_df[imported_df["Top Users"]=="Top Users"]
#cash is still the dominant payment method in both quantity and values
top_table_1=top_user_df[top_user_df["Status"]=="Delivered"].pivot_table(index="PaymentMethod",values="Net Value",aggfunc=["mean","count"])

#Total Item has the highest impact to Order Values 
#top_delivered=top_user_df[(top_user_df["Status"]=="Delivered")&(top_user_df["PaymentMethod"]!="VNPay")]
##print(top_delivered[["Net Value","TotalItem"]].corr())
#print(top_delivered[["Net Value","CompletionTime"]].corr())
#print(top_delivered[["Net Value","Distance"]].corr())
#print(top_delivered[["Net Value","TotalDiscount"]].corr())

#top users likely purchase more items in 1 order
top_table_2=imported_df.pivot_table(index="Top Users",values="TotalItem",aggfunc=[np.min,"mean",np.max])

#top users spend more on drinks and have big gap with other categories
top_table_3=top_user_df.pivot_table(index="Category",values="Order Value",aggfunc="mean")
#with discounted deals, streetfood becomes dominant choice of top users
top_table_4=top_user_df.pivot_table(index="Category",columns="DiscountType",values="Order Value",aggfunc="mean")

