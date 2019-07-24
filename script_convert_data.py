from jsonprocessor.ConvertData import  ConvertData



cat ='Fitness'
folderpath='../'+ cat  +'/Data/'
groupfile='Groups/9_None_Groups.csv'
memberfilesfolder='Members/'
eventfilesfolder='Events/'
cd=ConvertData(folderpath,groupfile,memberfilesfolder,eventfilesfolder,'',False,True)
cd.StartConversion()


#OLD
#groupfile='Groups/final_esem_swdevonly_all.csv'
#groupfile='Groups/final_esem_swdevonly_missing_groups_events.csv'
#groupfile='Groups/final_esem_swdevonly_missing_groups_members.csv'


#folderpath='../deep-learning/Data/'
#groupfile='Groups/34_deep-learning_Groups.csv'




#groupfile='Groups/Tech_Groups_Filtered_city.csv'
#domain=''
#groupfile='Groups/34_python_Groups.csv'
#groupfile='Groups/34_None_Groups.csv'








