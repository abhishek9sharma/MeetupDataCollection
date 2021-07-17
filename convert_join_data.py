from jsonprocessor.ConvertData import ConvertData
import os
import argparse

def convert_join_data(topic, groupfile, datafolder,cat):

	opfolder = cat if cat else topic
	groupfile = 'Groups/' +groupfile
	folderpath = datafolder + opfolder + '/Data/'
	print(os.path.abspath(folderpath))

	memberfilesfolder = 'Members/'
	eventfilesfolder = 'Events/'

	processed_groups = []

	cd = ConvertData(folderpath, groupfile, memberfilesfolder, eventfilesfolder, '', False, False, processed_groups)
	cd.StartConversion()

def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument('-datafol', '--datafolder', required=True, default='../data/',
                        help='Folder in which data will be stored ')

    parser.add_argument('-top', '--topic', required=True,
                        help='Topic whose data has to be downloaded( https://www.meetup.com/topics/)')

    parser.add_argument('-grpfilename', '--grpfilename', required=True,
                        help='Name of file in which extracted group info was stored')

    parser.add_argument('-cat', '--category', required=False, default=None,
                        help='Topic whose data has to be downloaded( https://www.meetup.com/topics/)')
    args = parser.parse_args()
    return args


def main():
    args = parse_args()
    print(args)
    convert_join_data(topic=args.topic,
                      groupfile=args.grpfilename,
                      datafolder=args.datafolder,
                      cat=args.category)



if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print(e)




