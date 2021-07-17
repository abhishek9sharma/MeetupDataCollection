

### DOWNLOAD DATA

- install python3.6
- run ./setupvenv.sh
- run below from command line with the flag values based on your machine 
        
    - Format
      
          python get_data_json.py
                -datafol <folderpath where data needs to be stored>
                -top <name of of the topic whose data is to be downloaded> 
                -G <flag to indicate if group data needs to be downloaded>
                -E <flag to indicate if event data needs to be downloaded>
                -M <flag to indicate if member data needs to be downloaded>
                -cfgfol <folder where config file containing meetup api keys is present>
                -cfgfile < name of config file which contains meetup api keys>
   
    - Example
            
            python get_data_as_json.py -datafol $PWD/data/ -cfgfile MeetupKeys3.json -top sanskrit -G True

