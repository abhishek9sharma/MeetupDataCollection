rm -rf meetupvenv
python3.6 -m pip install --user virtualenv
python3.6 -m virtualenv meetupvenv
source meetupvenv/bin/activate
pip install git+https://github.com/pferate/meetup-api.git
pip install -r requirements.txt
cd meetupvenv/lib/python3.6/site-packages
git clone https://github.com/pferate/meetup-api.git
cp -r meetup-api/meetup/api_specification/    meetup/api_specification
cd ../../../../