import sqlalchemy
import re
import json
from collections import  Counter
from sqlalchemy.orm import Session
from google.cloud import secretmanager
from w3lib.html import  replace_escape_chars
from supabase import create_client, Client

class CloudConnect:
    def __init__(self):
        self.version_id = '/versions/latest'
        self.project_id = 'projects/906425369069/secrets/'
        self.production = False


    def access_secret_version(self, secret_version_id):
        """Return the value of a secret credential"""

        # Create the Secret Manager client.
        client = secretmanager.SecretManagerServiceClient()

        # Access the secret version.
        response = client.access_secret_version(name=secret_version_id)

        # Return the decoded payload.

        return response.payload.data.decode('UTF-8')

    def connect_tcp_socket(self) -> sqlalchemy.engine.base.Engine:
        """ Initializes a TCP connection pool for a Cloud SQL instance of MySQL. """
        if  self.production is False:
            password = "Cwodp3eateL"
            host = "localhost"
            user = "root"
            port = "3306"
            db_name = "harvest_db" #"eco_db"
        else:

            password = self.access_secret_version(f"{self.project_id}db_password{self.version_id}")
            host = self.access_secret_version(f"{self.project_id}db_host{self.version_id}")
            user = self.access_secret_version(f"{self.project_id}db_user{self.version_id}")
            db_name = "harvest_db"
            port = self.access_secret_version(f"{self.project_id}db_port{self.version_id}")


        connect_args = {}
        # For deployments that connect directly to a Cloud SQL instance without
        # using the Cloud SQL Proxy, configuring SSL certificates will ensure the
        # connection is encrypted.


        # [START cloud_sql_mysql_sqlalchemy_connect_tcp]
        pool = sqlalchemy.create_engine(
            # Equivalent URL:
            # mysql+pymysql://<valentine>:<db_pass>@<localhost>:<3306>/<db_name>
            sqlalchemy.engine.url.URL.create(
                drivername="mysql+pymysql",
                username=user,
                password=password,
                host=host,
                port=port,
                database=db_name,
            ),

            # [START cloud_sql_mysql_sqlalchemy_limit]
            # Pool size is the maximum number of permanent connections to keep.
            pool_size=5,
            # Temporarily exceeds the set pool_size if no connections are available.
            max_overflow=2,
            # The total number of concurrent connections for your application will be
            # a total of pool_size and max_overflow.
            pool_timeout=30,  # 30 seconds

            # 'pool_recycle' is the maximum number of seconds a connection can persist.
            # Connections that live longer than the specified amount of time will be
            # re-established
            pool_recycle=1800,  # 30 minutes

        )
        return pool

    def get_slack_key(self,testing=False):
        if testing:
            return self.access_secret_version(f"{self.project_id}"
                                              f"slack_testspace{self.version_id}")
        return self.access_secret_version(f"{self.project_id}slack_workspace{self.version_id}")

    def get_scraping_api_key(self):
        return self.access_secret_version(f"{self.project_id}"
                                          f"scraping_api_key{self.version_id}")

    def get_eco_endpoint(self):
        '''value = self.access_secret_version(f"{self.project_id}"
                                          f"eco_endpoint{self.version_id}")
        print(value)'''
        return self.access_secret_version(f"{self.project_id}"
                                          f"eco_endpoint{self.version_id}")

    def get_gmaps_key(self):

        return self.access_secret_version(f"{self.project_id}"
                                          f"gmaps_key{self.version_id}")

    def get_eco_token(self):
        return self.access_secret_version(f"{self.project_id}"
                                          f"eco_auth{self.version_id}")

    def get_staging_url(self):
        return self.access_secret_version(f"{self.project_id}"
                                          f"SUPABASE_URL{self.version_id}")
    def get_staging_key(self):
        return self.access_secret_version(f"{self.project_id}"
  
  
                                          f"SUPABASE_KEY{self.version_id}")

    def get_raven_key(self):
        return self.access_secret_version(f"{self.project_id}"
                                          f"RAVEN_API_KEY{self.version_id}")
    def normal_query(self):
        return 'and live=1 and needs_processing=0'
    def processing_query(self):
        return 'and live=0 and needs_processing=1 and done=0'


class BaseQuery:
    """This class constructs the building blocks for db queries used throughout the code base,
     To allow for dynamic queries, the specific columns for each asset are memoized  available
     when called for
    """
    def __init__(self):
        self.cloud_conn = CloudConnect()
        self.curr = self.cloud_conn.connect_tcp_socket().connect()
        self.config = Session(self.curr)
        self.news_asset = 'news'
        self.event_asset = 'event'
        self.job_asset = 'job'
        self.asset_keys = Counter()
        self.asset_columns = Counter()

        self.url = self.cloud_conn.get_staging_url()  # os.environ.get("SUPABASE_URL")
        self.key = self.cloud_conn.get_staging_key()  # os.environ.get("SUPABASE_KEY")
        self.supabase: Client = create_client(self.url, self.key)

    """ 'Get_x_asset' used by various clients to access sensitive db table names"""

    def get_news_asset(self):
        return self.news_asset

    def get_event_asset(self):
        return self.event_asset

    def get_job_asset(self):
        return self.job_asset


    def column_format(self,value:list)->str:
        '''transform list of strings into format suitable for db query'''

        # removes outer brackets and string quotations, leaving just the column names and commas
        return str(value)[1:-1].replace("'",' ')

    def make_dataset(self,keys,values):
        '''used to make ad hoc data sets'''
        if len(keys) != len(values):
            raise  ValueError(f"Keys and Values must be "
                              f"commensurate\nkey len={len(keys)}, value len ={len(values)}")
        return dict(zip(keys, values))


    def get_data(self,table_name):
        table_keys = self.curr.execute(f""" describe {table_name}""")

        table_keys = [i[0] for i in table_keys.fetchall()]

        columns = self.column_format(table_keys)
        data = self.curr.execute(
            f"""select {columns}  from {table_name}
                                                                         """)
        return data.fetchall(), table_keys

class UrlWorks:
    def get_base_url(self,url):
        base = re.compile(r'^((?:https?:\/\/(?:www\.)?|www\.)[^\s\/]+\/?)').findall(url)
        if base:
            return base[0]
        return None

    def bad_prefix(self,url):
        return re.compile(r'^/|^//', flags=re.IGNORECASE).findall(url)

    def confirm_url(self,url: str, main_url=None):
        #Todo: Abstract out link analysis step

        """Confirm the links are valid"""
        if url is None or type(url)!= str:
            return None

        if 'base64' in url:
            return None
        if 'data:image' in url:
            return None

        if '<' in url or '>' in url:
            return None

        if len(url) >500:
            #print(f'LINK TOO BIG: {url}')
            return None

        no_spaces = url.strip().split(' ')

        if len(no_spaces) > 1:
            """There should be no spaces in the links, if there are, check to see if its a list of 
            valid urls and take the first one, if not see if its a domain and page link and join it"""
            get_url = [self.get_base_url(i) for i in no_spaces if self.get_base_url(i)]
            size =len(get_url)
            if size > 1:
                # indicates a 'list' of n urls
                url = no_spaces[0]

            elif size ==1:
                'indicates only the first url is valid'
                url = ''.join(no_spaces)

            else:
                return None
        """if link is absolute, confirm its domain"""
        absolute = self.get_base_url(url)

        if absolute:
            valid_url = url.replace(absolute, absolute.lower())
            if re.compile(r'^www').findall(valid_url):
                valid_url = 'https://'+valid_url
            return valid_url


        if '.com/' in url and url[:2]=='//':
            return 'https:' + url
        if main_url:
            """For relative paths, get the base url from the main url"""
            base_url = self.get_base_url(main_url)
            # make sure theres no slashes at the start of the path
            if self.bad_prefix(url):
                new_url = url.split('/')
                clean_url = [i for i in new_url if i]
                # if the base url has no slash add one to make a valid url
                if base_url[-1] != '/':
                    base_url = base_url + '/' + '/'.join(clean_url)
                else:
                    base_url = base_url + '/'.join(clean_url)

            else:
                base_url = base_url + url
            no_spaces = replace_escape_chars(base_url).strip().split(' ')

            if len(no_spaces) > 1:

                return None
            if re.compile(r'^www').findall(base_url):
                return  'https://'+base_url
            return base_url

        return None



