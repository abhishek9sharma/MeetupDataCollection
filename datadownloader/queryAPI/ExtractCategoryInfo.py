__author__ = 'abhisheksh'
import pandas as pd
import json

class CategoryInfoExtractor:

    def __init__(self, meetupclients, opfolder, configfolder):

        self.opfolder = opfolder
        self.configfolder = configfolder
        self.meetup_clients = meetupclients
        self.cat_all = None
        self.topic_cats = None
        try:
            self.get_allcategories()
        except:
            self.get_allcategories_new()

    def get_allcategories(self):
        try:
            mtupcl = self.meetup_clients[0][1]
            self.cat_all = self.get_data(self.opfolder + 'Data/CategoriesAll', mtupcl.GetCategories())
            self.topic_cats = self.get_data(self.opfolder + 'Data/TopicCategories', mtupcl.GetTopicCategories())
            print(self.cat_all)
            print(self.topic_cats)
        except Exception as e:
           raise ValueError(e)

    def get_data(self, fname, api_info):
        info_pd=pd.read_json(json.dumps(api_info.results))
        info_pd.to_csv(fname+'.csv', index=False)
        return info_pd

    def get_allcategories_new(self):
        try:
            self.cat_all = self.get_data_new(self.configfolder + '/cats/CategoriesAll.csv')
            self.topic_cats = self.get_data_new(self.configfolder + '/cats/TopicCategories.csv')
            print(self.cat_all)
            print(self.topic_cats)
        except Exception as e:
            print(e)

    def get_data_new(self, fname):
        print(fname)
        info_pd=pd.read_csv(fname)
        #info_pd.to_csv(fname+'.csv', index=False)
        return info_pd

    def get_categoryid(self, cattofind):
        cattofind_id = self.cat_all['id'][self.cat_all['name'] == cattofind].iloc[0]
        return cattofind_id