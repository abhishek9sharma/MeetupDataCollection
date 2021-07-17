import sys
import argparse
import os

from datadownloader.queryAPI.GetDataFromAPI import APIDataExtractionFacade

def create_dirs(datafolder, topic, cat=None):
    print(datafolder)
    if cat is None and topic is None:
        raise ValueError(" Please set a catgeory or topic whose data has to be extracted")
    elif cat:
        opfolder = cat
    else:
        opfolder = topic

    opfolder = os.path.join(datafolder, opfolder)
    print(opfolder)

    try:
        os.mkdir(opfolder)
        os.mkdir(opfolder + '/Logs')
        os.mkdir(opfolder + '/Data')
        os.mkdir(opfolder + '/Data/Members')
        os.mkdir(opfolder + '/Data/SOTags')
        os.mkdir(opfolder + '/Data/Events')
        os.mkdir(opfolder + '/Data/CSVFormat')
        os.mkdir(opfolder + '/Data/CSVFormat/Members')
        os.mkdir(opfolder + '/Data/CSVFormat/Members/Members_Groups_Individual')
        os.mkdir(opfolder + '/Data/CSVFormat/Members/Members_Groups_Combined')
        os.mkdir(opfolder + '/Data/CSVFormat/Events')
        os.mkdir(opfolder + '/Data/CSVFormat/Events/Events_Groups_Combined')
        os.mkdir(opfolder + '/Data/CSVFormat/Events/Events_Groups_Individual')
        os.mkdir(opfolder + '/Data/CSVFormat/Groups')
        os.mkdir(opfolder + '/Data/Pickle')
        os.mkdir(opfolder + '/Data/TopicCategories')
        os.mkdir(opfolder + '/Data/Groups')
    except Exception as e:
        print(e)
    return opfolder +'/'


def download_data(topictofind, datatafolder='../data/', configfolder=None, configfilename=None,
                  category=None, downloadevents=False, downloadgroups=True,
                  downloadmembers=False, downloadeventcounts=False):

    data_folder_final = create_dirs(datatafolder, topic=topictofind, cat=category)

    if configfolder is None:
        configfolder = os.path.join(os.getcwd(), 'datadownloader', 'Config')
    if configfilename is None:
        configfilename = 'MeetupKeys3.json'

    city_country_state = None  # Location Requires City+Country in Case of Non US Locations
    apide = APIDataExtractionFacade(configfolder,
                                    data_folder_final,
                                    configfilename,
                                    cattofind=category,
                                    topic=topictofind,
                                    locinfo=city_country_state,
                                    specificgroups=None)

    apide.start_info_extraction(groups=bool(downloadgroups),
                                events=bool(downloadevents),
                                members=bool(downloadmembers),
                                eventcountsextract=bool(downloadeventcounts)
                                )



def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('-datafol', '--datafolder', required=True, default='../data/',
                        help='Folder in which data will be stored ')

    parser.add_argument('-top', '--topic', required=True,
                        help='Topic whose data has to be downloaded( https://www.meetup.com/topics/)')

    parser.add_argument('-cat', '--category', required=False,
                        help='Main category of a topic whose data has to be downloaed')

    parser.add_argument('-G', '--downloadgroups', required=True, default=True,
                        help='Flag specifiying if group data is to be downloaded')

    parser.add_argument('-E', '--downloadevents', required=False, default=False,
                        help='Flag specifiying if event data is to be downloaded')

    parser.add_argument('-EC', '--downloadeventcounts', required=False, default=False,
                        help='Flag specifiying if event coounts are to be downloaded')

    parser.add_argument('-M', '--downloadmembers', required=False, default=False,
                        help='Flag specifiying if member data is to be downloaded')

    parser.add_argument('-cfgfol', '--configfolder', required=False,
                        default=os.path.join(os.getcwd(), 'datadownloader', 'Config'),
                        help='Folder in which config file is present')

    parser.add_argument('-cfgfile', '--configfilename', required=False,
                        default='MeetupKeys.json',
                        help='Name of file in which auth keys are stored')


    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    download_data(topictofind=args.topic,
                  datatafolder=args.datafolder,
                  configfolder=args.configfolder,
                  configfilename=args.configfilename,
                  category=args.category,
                  downloadevents=args.downloadevents,
                  downloadgroups=args.downloadgroups,
                  downloadmembers=args.downloadmembers,
                  downloadeventcounts=args.downloadeventcounts)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(e)