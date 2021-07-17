#https://gist.githubusercontent.com/reustle/97aa4c24051472edab1f20e5d8494cdd/raw/a8eb4b1e7faecd046a8b800069af2dc3758c2a0f/meetup-oauth.py
from requests_oauthlib import OAuth2Session

client_id = input('Client ID: ')
client_secret = input('Client Secret: ')
#redirect_uri = r'https://abhishek9sharma.github.io/callback/' # Any URL, but with /callback
#redirect_uri=''
redirect_uri = input('Redirect URI: ') # Any URL, but with /callback

scopes = ['basic', 'ageless'] # it's essential for long refresh time

oauth = OAuth2Session(client_id, redirect_uri=redirect_uri, scope=scopes)
authorization_url, state = oauth.authorization_url('https://secure.meetup.com/oauth2/authorize')

print('Visit this URL: ' + str(authorization_url))


code =input('OAuth Code: ')  # Get this from the redirected url
token_url = r'https://secure.meetup.com/oauth2/access'

token = oauth.fetch_token(token_url, client_secret=client_secret, code=code, include_client_id=True)

print('Token: ' + str(token))
# {'access_token': '###', 'refresh_token': '###', 'token_type': 'bearer', 'expires_in': 72576000, 'expires_at': 123456}

