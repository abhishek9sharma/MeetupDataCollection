from jsonprocessor.ConvertData import ConvertData
import os
import  pandas as pd

cat = 'Tech'
catcode = None
topictofind = None
groupfile = None

#Set Topic to Mine
# if cat is None  and topictofind is None:
#     raise ValueError(" Please set a catgeory or topic whose data has to be extracted")
# elif cat:
#     opfolder = cat
#     groupfile='Groups/'+ catcode +'_None_Groups.csv'
# else:
#     opfolder = topictofind
#     groupfile='Groups/None_'+ topictofind +'_Groups.csv'


opfolder = 'Tech'

groupfile = 'Groups/groups_converted_new.csv'
folderpath= '../' + opfolder  + '/Data/'


opfolder = 'sanskrit'
groupfile='Groups/0_None_Groups.csv'
folderpath = os.getcwd()+'/data/'+opfolder+'/Data/'
print(os.path.abspath(folderpath))

#groupfile='Groups/9_None_Groups.csv'
memberfilesfolder='Members/'
eventfilesfolder='Events/'

#processed_groups_for_members = os.path.join(folderpath,'Groups','processed_group1.csv')
processed_groups = []#pd.read_csv(processed_groups_for_members)['Groups'].tolist()

cd=ConvertData(folderpath,groupfile,memberfilesfolder,eventfilesfolder,'',False,False, processed_groups)
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








