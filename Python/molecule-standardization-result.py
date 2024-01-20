from fuzzywuzzy import fuzz
import pandas as pd
import time
import operator

source_df=pd.read_csv("D:/07122018-result.csv",low_memory=False)
del source_df["STT File"]
target_df=pd.read_csv("D:/Chuanhoa-07052019-1.csv",low_memory=False)

start=time.time()
target_visa=target_df["VisaNo"]
list_visa=list(target_visa)
target_stt=target_df["STT File"]
target_price=target_df["Price"]
target_molecule=target_df["Generic"]
target_frame=pd.concat([target_stt,target_visa,target_price,target_molecule],axis=1)

n=0
item_list=[]
dosage_list=[]
manufacture_list=[]
nation_list=[]
presentation_list=[]
indetail_list=[]
measure_list=[]
formula_list=[]
start_loop=0
for item in list_visa:
    if start_loop==0:
        start_loop=time.time()
    try:
        start_loop=time.time()
        search_product=source_df["Product Name"][source_df["VisaNo"]==item]
        search_price=(source_df["Price"][source_df["VisaNo"]==item]).astype(int)
        search_molecule=source_df["Generic"][source_df["VisaNo"]==item]
        search_dosage=source_df["Dosage"][source_df["VisaNo"]==item]
        search_manufacture=source_df["Manufacture"][source_df["VisaNo"]==item]
        search_nation=source_df["Manufacture's Nation"][source_df["VisaNo"]==item]
        search_presentation=source_df["Presentation"][source_df["VisaNo"]==item]
        search_indetail=source_df["Indetail"][source_df["VisaNo"]==item]
        search_measure=source_df["Unit of measure"][source_df["VisaNo"]==item]
        search_formula=source_df["Formula"][source_df["VisaNo"]==item]
        
        if len(search_product.unique())==1 and (min(search_price)==max(search_price)):
            item_list.append(str(search_product.unique()[0]))
            dosage_list.append(str(search_dosage.unique()[0]))
            manufacture_list.append(str(search_manufacture.unique()[0]))
            nation_list.append(str(search_nation.unique()[0]))
            presentation_list.append(str(search_presentation.unique()[0]))
            indetail_list.append(str(search_indetail.unique()[0]))
            measure_list.append(str(search_measure.unique()[0]))
            formula_list.append(str(search_formula.unique()[0]))
            
        else:
            if len(search_molecule)!=0:
                price_compare=abs(search_price-(int(target_frame.iloc[n,2])))
                molecule_compare=list(source_df["Generic"][source_df["VisaNo"]==item].unique())
                for each in molecule_compare:
                    checking_dic={}
                    for each_item in search_molecule:
                        checking_dic[str(each)]=fuzz.ratio(each,each_item)
                    result=max(checking_dic.items(),key=operator.itemgetter(1))[0]
    
                item_name=search_product[(price_compare==min(price_compare))].unique()
                dosage_name=search_dosage[(price_compare==min(price_compare))].unique()
                manufacturer_name=search_manufacture[(price_compare==min(price_compare))].unique()
                nation_name=search_nation[(price_compare==min(price_compare))].unique()
                presentation_name=search_presentation[(price_compare==min(price_compare))].unique()
                indetail_name=search_indetail[(price_compare==min(price_compare))].unique()
                measure_name=search_measure[(price_compare==min(price_compare))].unique()
                formula_name=search_formula[(price_compare==min(price_compare))].unique()
                
                if len(item_name)==1:
                    item_list.append(str(item_name[0]))
                    dosage_list.append(str(dosage_name))
                    manufacture_list.append(str(manufacturer_name))
                    nation_list.append(str(nation_name))
                    presentation_list.append(str(presentation_name))
                    indetail_list.append(str(indetail_name))
                    measure_list.append(str(measure_name))
                    formula_list.append(str(formula_name))
                else:    
                    item_len=[len(each_part) for each_part in item_name]
                    select_item=pd.DataFrame([item_name,item_len]).transpose()
                    select_item.sort_values(by=1,inplace=True,ascending=False)
                    item_name_choose=select_item.iloc[0,0]
                    item_list.append(str(item_name_choose))
                    dosage_name=dosage_name[0]
                    dosage_list.append(str(dosage_name))
                    manufacturer_name=manufacturer_name[0]
                    manufacture_list.append(str(manufacturer_name))
                    nation_name=nation_name[0]
                    nation_list.append(str(nation_name))
                    presentation_name=presentation_name[0]
                    presentation_list.append(str(presentation_name))
                    indetail_name=indetail_name[0]
                    indetail_list.append(str(indetail_name))
                    measure_name=measure_name[0]
                    measure_list.append(str(measure_name))
                    formula_name=formula_name[0]
                    formula_list.append(str(formula_name))                       
            else:
                item_list.append("")
                dosage_list.append("")
                manufacture_list.append("")
                nation_list.append("")
                presentation_list.append("")
                indetail_list.append("")
                measure_list.append("")
                formula_list.append("")
        
    except:
        item_list.append("")
        dosage_list.append("")
        manufacture_list.append("")
        nation_list.append("")
        presentation_list.append("")
        indetail_list.append("")
        measure_list.append("")
        formula_list.append("")
    blank_formula=0
    for formula in formula_list:
        if formula=="":
            blank_formula+=1
    n+=1
    if n%1000==0:
        print(n,time.time()-start_loop,(1-blank_formula/len(formula_list))*100)
        start_loop=0

        
end=time.time()
print(end-start)
list_stt=list(target_df["STT File"])
result_df=pd.DataFrame([list_stt,item_list,dosage_list,manufacture_list,nation_list,presentation_list,indetail_list,measure_list,formula_list]).transpose()
