__author__ = 'abhisheksh'
import pandas as pd
import json
from datetime import  datetime

class CategoryInfoExtractor:
    def __init__(self,meetupclients,opfolder):
        #self.cattofind=cattofind
        self.opfolder=opfolder
        self.meetup_clients=meetupclients
        self.GetAllCategories()


    def GetAllCategories(self):
        mtupcl=self.meetup_clients[0][1]
        self.cat_all=self.GetData(self.opfolder+'Data/CategoriesAll',mtupcl.GetCategories())
        self.topic_Cats=self.GetData(self.opfolder+'Data/TopicCategories',mtupcl.GetTopicCategories())
        print(self.cat_all)
        print(self.topic_Cats)



    def GetData(self,fname,api_info):
        info_pd=pd.read_json(json.dumps(api_info.results))
        info_pd.to_csv(fname+'.csv',index=False)
        return info_pd


    def GetCategoryId(self,cattofind):
        cattofind_id=self.cat_all['id'][self.cat_all['name']==cattofind].iloc[0]
        return cattofind_id