
import pandas as pd
import os
import dask.dataframe as dd
import matplotlib.pyplot as plt
import dask.multiprocessing
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path
import numpy as np
from datadownloader.QueryAPI.GetDataFromAPI import *
from datadownloader.APIClient.MeetupClients import MeetUpClients

domain='techall'
mainfolder = '/home/abhisheksh/PROJECTS/Meetup/techall/'

uniqevents_eng=pd.read_csv(mainfolder+'/Data/CSVFormat_SWDEV_060518/Events/Events_Groups_Combined/'+'uniqevents_eng_filtered_185758.csv',
                          index_col='created',parse_dates=True, nrows = 10)
uniqevents_eng=uniqevents_eng.drop_duplicates(['description'])
event_details = uniqevents_eng[['id','event_url']]







cat = 'techall'
topictofind = None
city_country_state = None #Location Requires City+Country in Case of Non US Locations

#Set Topic to Mine
if cat is None  and topictofind is None:
    raise ValueError(" Please set a catgeory or topic whose data has to be extracted")
elif cat:
    opfolder = cat
else:
    opfolder = topictofind

opfolder = '../'+opfolder+'/'
opfolder = '/home/abhisheksh/PROJECTS/Meetup/techall/'

configfolder = None
configfile = 'MeetupKeys3.json'

apide=APIDataExtractionFacade(configfolder,opfolder,configfile, cattofind=cat,topic=topictofind,locinfo=city_country_state,specificgroups=None)
apide.ExtractEventDetails(event_details)

