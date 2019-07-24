from datadownloader.QueryAPI.GetDataFromAPI import *







cat = 'Tech'
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
configfolder = None
#opfolder ='data/'

apide=APIDataExtractionFacade(configfolder,opfolder,cattofind=cat,topic=topictofind,locinfo=city_country_state,specificgroups=None)
apide.StartInfoExtraction()
