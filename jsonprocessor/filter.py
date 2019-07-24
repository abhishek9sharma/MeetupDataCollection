
# coding: utf-8

# In[1]:


#IMPORT REQUISTITE LIBRARIES
from datadownloader.MeetupClients import MeetUpClients
import json
import pandas as pd
from  datadownloader.Utils.Logging import LoggingUtil
from datetime import datetime
import  multiprocessing as mp
from functools import  partial
import numpy as np
import sys
import timeit
import pickle 

cores=mp.cpu_count()-1

#opfolder='/media/oldmonk/SAMSUNG/DATA_ABHISHEK/MEETUP_PROJECT/techall/'
opfolder='deep-learning/'
print(str(datetime.now()))
logfile=LoggingUtil(opfolder+'Logs/','CHECK_FILTER'+str(datetime.now())+'.txt')


# In[2]:


#FILTE GROUPS ON SO TAGS
#group_pd=pd.read_csv('Data/Groups/Tech_Groups.csv')
sotags=pd.read_csv(opfolder+'Data/SOTags/ALL.csv')
filterlist=['software development']

cattofind="34"
#topic='None'
topic='deep-learning'
group_pd=pd.read_csv(opfolder+"Data/Groups/"+cattofind+'_'+topic+'_Groups.csv')
#filterlist=['open-source']
#filterlist=['software development']
filterlist=[str(tg).lower() for tg in sotags['TagName'].tolist()]

logstr="Loaded stackoveflow tags :::" + str(len(filterlist))
logfile.Log(logstr)
print(logstr)

topicsmeetup=[]
topicsmeetup_intersect=[]
'''
def CheckSOTags(topics):
    if(type(topics)==str):
        topics=eval(topics)
    if(len(topics)==0):
        return False
    else:

        try:
            for topic_str in topics:

                #SAVE ORG TOPIC NAME IN MEETUP
                topicsmeetup.append(topic_str['name'])

                #CHECK IF ORG TOPIC STRING IN SO TAGS WHEN CONVERTED TO LOWER CASE
                if(topic_str['name'].lower() in filterlist):
                    if(topic_str['name'].lower() not in topicsmeetup_intersect):
                        topicsmeetup_intersect.append(topic_str['name'].lower())
                    return True

                #Check if Original Topic when converrted to SOFormat in SO Tags
                tpcs=topic_str['name'].split()
                tpcs_so_format=("-".join(tpcs)).lower()
                if(tpcs_so_format in filterlist):
                    if(tpcs_so_format not in topicsmeetup_intersect):
                        topicsmeetup_intersect.append(tpcs_so_format)
                    return True


                ##heck if Original Topic when words in Topics Considered to SOFormat in SO Tags
                for t in tpcs:
                    if(t.lower() in filterlist):
                        if(t.lower() not in topicsmeetup_intersect):
                            topicsmeetup_intersect.append(t.lower())
                        return True
            return False

        except :
            print(" Exception Occured " + str(sys.exc_info()[0]))
            return False
'''
def CheckSOTags(topics):
    if(type(topics)==str):
        topics=eval(topics)
    if(len(topics)==0):
        return (False,"No topics attached to this group ",None)
    else:

        try:
            for topic_str in topics:

                #SAVE ORG TOPIC NAME IN MEETUP
                #topicsmeetup.append(topic_str['name'])

                #CHECK IF ORG TOPIC STRING IN SO TAGS WHEN CONVERTED TO LOWER CASE
                if(topic_str['name'].lower() in filterlist):
                    #if(topic_str['name'].lower() not in topicsmeetup_intersect):
                    #    topicsmeetup_intersect.append(topic_str['name'].lower())
                    return (True,topic_str['name'],topic_str['name'].lower())

                #Check if Original Topic when converrted to SOFormat in SO Tags
                tpcs=topic_str['name'].split()
                tpcs_so_format=("-".join(tpcs)).lower()
                if(tpcs_so_format in filterlist):
                    #if(tpcs_so_format not in topicsmeetup_intersect):
                    #    topicsmeetup_intersect.append(tpcs_so_format)
                    return (True,topic_str['name'],tpcs_so_format)


                ##heck if Original Topic when words in Topics Considered to SOFormat in SO Tags
                for t in tpcs:
                    if(t.lower() in filterlist):
                        #if(t.lower() not in topicsmeetup_intersect):
                        #    topicsmeetup_intersect.append(t.lower())
                        return (True,topic_str['name'],t.lower())

            return (False,topic_str['name'],None)

        except :
            print(" Exception Occured " + str(sys.exc_info()[0]))
            return (False,topic_str['name'],None)
        
def ProcessChunk(df):
    df_so=df['topics'].apply(CheckSOTags)
    return df_so
    

def SWTag(topics,filterlist):
    if(type(topics)==str):
        topics=eval(topics)
    if(len(topics)==0):
        return (False,"No topics attached to this group ",None)
    else:

        try:
            for topic_str in topics:
                if(topic_str['name'].lower() in filterlist):
                    return (True,topic_str['name'],topic_str['name'].lower())
            return (False,topic_str['name'],None)

        except :
            print(" Exception Occured " + str(sys.exc_info()[0]))
            return (False,topic_str['name'],None)
        
                

        
def ProcessChunkSW(df):
    df_so=df['topics'].apply(CheckSWTag,['Software Development'])
    return df_so
        
        
        
        
#group_pd_software=group_pd['topics'].apply(CheckSOTags)
#group_pd['topics']


# In[3]:


#Filter Grousp By Prescence of Software Development


cores=mp.cpu_count()-1

def ParallelApply(df,func):
    chunks=np.array_split(df,cores)
    parpool=mp.Pool(cores)
    changed_df=pd.concat(parpool.map(ProcessChunk,chunks))
    parpool.close()
    parpool.join()
    return changed_df


def ParallelApplySW(df,func):
    chunks=np.array_split(df,cores)
    parpool=mp.Pool(cores)
    changed_df=pd.concat(parpool.map(ProcessChunkSW,chunks))
    parpool.close()
    parpool.join()
    return changed_df

'''
logstr= " filtering groups which contain Software Development Tag "
logfile.Log(logstr)
print(logstr)
softwar_filter=ParallelApplySW(group_pd,ParallelApplySW)
group_pd_software=group_pd[pd.Series([i[0] for i in softwar_filter])]
group_pd_software.to_csv(opfolder+"Data/Groups/"+cattofind+'_Groups_FilteredSSW.csv',index=False)
logstr= str(group_pd_software.shape[0]) +" groups found which contain Software Development Tag"
logfile.Log(logstr)
print(logstr)


so_software_filter=ParallelApply(group_pd_software,ParallelApply)
topicsmeetup=set(i[1] for i in softwar_filter)
topicsmeetup_intersect=set([i[2] for i in list(softwar_filter) if i[2] is not None])

group_pd_so_software=group_pd_software[pd.Series([i[0] for i in so_software_filter])]
group_pd_so_software.to_csv(opfolder+"Data/Groups/"+cattofind+'_Groups_FilteredSO_SW.csv',index=False)
logstr= str(group_pd_software.shape[0]) +" groups found which contain Stack Overflow Tag and Softwar Development Tag"
logfile.Log(logstr)
print(logstr)
'''

logstr= " filtering groups which contain Stack OVerflow Tag "
logfile.Log(logstr)
print(logstr)
softwar_filter=ParallelApply(group_pd,ParallelApply)
topicsmeetup=set(i[1] for i in softwar_filter)
topicsmeetup_intersect=set([i[2] for i in list(softwar_filter) if i[2] is not None])
group_pd_software=group_pd[pd.Series([i[0] for i in softwar_filter])]
#group_pd_software.shape
group_pd_software.to_csv(opfolder+"Data/Groups/"+cattofind+'_Groups_FilteredSO.csv',index=False)
logstr= str(group_pd_software.shape[0]) +" groups found which contain Stack OVerflow Tag"
logfile.Log(logstr)
print(logstr)

pickle.dump(group_pd_software,open(opfolder+'Data/Pickle/group_pd_software.p','wb'))


# In[4]:


#FILTER GROUPS ON MEMBERS
#group_pd_software=pd.read_csv('Data/Groups/Tech_Groups_FilteredSO.csv')
#group_pd=pd.read_csv('Data/Groups/Tech_Groups.csv')
group_pd_grtr_10=group_pd_software[group_pd['members']>=10]
logstr="Groups which have atelast 10 members  :::" + str(group_pd_grtr_10.shape[0])
logfile.Log(logstr)
print(logstr)


# In[5]:


import pandas as pd
group_event_counts=pd.read_csv(opfolder+'Data/events.csv',names=['groupid','EventCount'])
event_count_Final=pd.DataFrame()
try:
    event_count_failed_rep=pd.read_csv(opfolder+'Data/events_count_failed.csv',names=['groupid','EventCount'])
    event_count_Final=group_event_counts[group_event_counts['EventCount']!=-1]['groupid'].tolist()+event_count_failed_rep['groupid'].tolist()

except:
    event_count_Final=group_event_counts[group_event_counts['EventCount']!=-1]
    
#event_count_Final_df=group_event_counts[group_event_counts['EventCount']!=-1]+event_count_failed_rep
groups_filtered_events_rej=pd.concat([group_event_counts[group_event_counts['EventCount']!=-1],event_count_failed_rep])
groups_filtered_events_rej.shape
#len(event_count_Final)
#len(event_count_Final)
logstr="Event Count Caluclated for " + str(groups_filtered_events_rej.shape[0]) + " events "
logfile.Log(logstr)
print(logstr)



logstr="##END COUNT CALCULATION FINAL \n"
logfile.Log(logstr)
print(logstr)


# In[6]:


N=10

#EVENTS COUNTS GROUPS
groups_filtered_events_final=groups_filtered_events_rej[groups_filtered_events_rej['EventCount']>=N]['groupid'].tolist()
logstr= " No of groups which have organized atleast " + str(N)+ " events :"+str(len(groups_filtered_events_final))
print(logstr)


# In[7]:


#PUBLIC GROUPS
group_pd_software_public=group_pd_software[group_pd_software['visibility']=='public']
logstr= " No of groups which are publicly visible " + str(group_pd_software_public.shape[0])
print(logstr)


# In[8]:


group_pd_software_ids=group_pd_software['id'].tolist()
groups_filtered_events_ids=groups_filtered_events_final
groups_filtered_mebers_ids=group_pd_grtr_10['id'].tolist()
group_pd_software_public_ids=group_pd_software_public['id'].tolist()


final_groups_to_consider=list(set(group_pd_software_public_ids)&set(group_pd_software_ids) &set(groups_filtered_events_ids) &set(groups_filtered_mebers_ids))
print(str(len(final_groups_to_consider)))
df_filtered = pd.DataFrame(final_groups_to_consider, columns=["final_filtered"])
df_filtered.to_csv(opfolder+'Data/final_filtered.csv', index=False)

            


# In[9]:


final_grps_df=group_pd_software[group_pd_software['id'].isin(final_groups_to_consider)]
final_grps_df
final_grps_df.to_csv(opfolder+'Data/Groups/'+'final_esem.csv',index=False)

