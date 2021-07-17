A set of scripts which can help to download public data from [Meetup](https://www.meetup.com/) for a specific topic
using there [RESTful API](https://www.meetup.com/meetup_api/). [https://www.meetup.com/api/guide/#graphQl-guide](https://www.meetup.com/api/general) is not supported 


Used for data collection for below paper

### Citation
    @article{sharma2019analyzing,
      title={Analyzing Offline Social Engagements: An Empirical Study of Meetup Events Related to Software Development},
      author={Sharma, Abhishek and Prana, Gede Artha Azriadi and Sawhney, Anamika and Nagappan, Nachiappan and Lo, David},
      journal={arXiv preprint arXiv:1912.07352},
      year={2019}
    }

### Set API Key
- Get authorization tokens' from  [Meetup RESTful API](https://www.meetup.com/meetup_api/)
- Store tokens' as json file in the format mentioned in [MeetupKeysOAUTH.json](blob/master/datadownloader/Config/MeetupKeysOAUTH.json)

### DOWNLOAD DATA

- install python3.6
- run source ./setupvenv.sh
- run below from command line with the flag values based on your machine 

  - Format
          
                python get_data_json.py \
                      -datafol <folderpath where data needs to be stored> \
                      -top <name of of the topic whose data is to be downloaded> \
                      -G <flag to indicate if group data needs to be downloaded> \
                      -E <flag to indicate if event data needs to be downloaded> \
                      -M <flag to indicate if member data needs to be downloaded> \
                      -cfgfol <folder where config file containing meetup api keys is present> \
                      -cfgfile <name of config file which contains meetup api keys> \
                      -cat <Category name of of the topic whose data is to be downloaded> (optional)
   
      - Example (cat mentioned)
             
             python get_data_as_json.py \
                    -datafol $PWD/data/ \
                    -cfgfile MeetupKeys3.json \
                    -top quantum-algorithms \
                    -G True -E True -M True\
                    -cat Tech

      - Example (cat not mentioned)

             python get_data_as_json.py \
                    -datafol $PWD/data/ \
                    -cfgfile MeetupKeys3.json \
                    -top quantum-algorithms \
                    -G True -E True -M True\

#### JOIN AND CONVERT DATA TO CSV (Can be donw only after downloading data)

 - copy file name in the format *id_None_Groups.csv* or *id1_id2_Groups.csv*  that shouldbe present at below path
in `datafol` location specified in earlier step
        -  `datafol/Data/Groups/`
  - run the below command with flags specified
    
    - Format
      
                python get_data_json.py \
                      -datafol <folderpath where data needs to be stored> \
                      -top <name of of the topic whose data is to be downloaded> \
                      -grpfilename <name of  file found in previous step
      
    - Example (cat mentioned)
   
                 python convert_join_data.py \
                    -datafol $PWD/data/ \
                    -grpfilename 0_34_Groups.csv \
                    -top quantum-algorithms\
                    -cat Tech\
   
    - Example (cat not mentioned)
               
                python convert_join_data.py \
                    -datafol $PWD/data/ \
                    -grpfilename 0_None_Groups.csv \
                    -top quantum-algorithms \
