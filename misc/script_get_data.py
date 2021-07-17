import os

from datadownloader.queryAPI.GetDataFromAPI import *
from datadownloader.filter_groups import FilterGroups

# cat = 'Tech'
# topictofind = None

cat = None
topictofind = 'sanskrit'
city_country_state = None #Location Requires City+Country in Case of Non US Locations

#Set Topic to Mine
if cat is None and topictofind is None:
    raise ValueError(" Please set a catgeory or topic whose data has to be extracted")
elif cat:
    opfolder = cat
else:
    opfolder = topictofind

opfolder = os.getcwd()+'/data/'+opfolder+'/'

print(opfolder)
#Creat Data Directories
try:
    os.mkdir(opfolder)
    os.mkdir(opfolder + 'Logs')
    os.mkdir(opfolder + 'Data')
    os.mkdir(opfolder + 'Data/Members')
    os.mkdir(opfolder + 'Data/SOTags')
    os.mkdir(opfolder + 'Data/Events')
    os.mkdir(opfolder + 'Data/CSVFormat')
    os.mkdir(opfolder + 'Data/CSVFormat/Members')
    os.mkdir(opfolder + 'Data/CSVFormat/Members/Members_Groups_Individual')
    os.mkdir(opfolder + 'Data/CSVFormat/Members/Members_Groups_Combined')
    os.mkdir(opfolder + 'Data/CSVFormat/Events')
    os.mkdir(opfolder + 'Data/CSVFormat/Events/Events_Groups_Combined')
    os.mkdir(opfolder + 'Data/CSVFormat/Events/Events_Groups_Individual')
    os.mkdir(opfolder + 'Data/CSVFormat/Groups')
    os.mkdir(opfolder + 'Data/Pickle')
    os.mkdir(opfolder + 'Data/TopicCategories')
    os.mkdir(opfolder + 'Data/Groups')
except :
    print("Some Directory already exists")


#configfolder= os.path.dirname(os.path.realpath(__file__))
configfolder = os.path.join(os.getcwd(),'datadownloader','Config')
configfile = 'MeetupKeys3.json'
#opfolder ='data/'
#num_of_clients = 2

apide = APIDataExtractionFacade(configfolder,\
                              opfolder,\
                              configfile, \
                              cattofind=cat,\
                              topic=topictofind,\
                              locinfo=city_country_state,\
                              specificgroups=None)

apide.start_info_extraction(groups=False,\
                            events=True,\
                            members=False,
                            eventcountsextract=False)
#apide = APIDataExtractionFacade(configfolder, opfolder, configfile)


#cat = apide.cattofind_id
#topictofind = None
#load SO TAGS


# sotags=pd.read_csv('SOTags/ALL.csv')
# filterlist=[str(tg).lower() for tg in sotags['TagName'].tolist()]
# #filterlist = ['python']
# filter = FilterGroups(opfolder, cat, topictofind, filterlist)
# filter.filter()

