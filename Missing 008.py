import requests
import json
import configparser
from base64 import b64encode
#from sierra_ils_utils import SierraAPI
import psycopg2

def get_token():
    # config api    
    config = configparser.ConfigParser()
    config.read('api_info.ini')
    base_url = config['api']['base_url']
    client_key = config['api']['client_key']
    client_secret = config['api']['client_secret']
    auth_string = b64encode((client_key + ':' + client_secret).encode('ascii')).decode('utf-8')
    header = {}
    header["authorization"] = 'Basic ' + auth_string
    header["Content-Type"] = 'application/x-www-form-urlencoded'
    body = {"grant_type": "client_credentials"}
    url = base_url + '/token'
    response = requests.post(url, data=json.dumps(body), headers=header)
    json_response = json.loads(response.text)
    token = json_response["access_token"]
    return token

def mod_bib(bib_id,language,token,s):
    config = configparser.ConfigParser()
    config.read('api_info.ini')
    bibpatch = {"varFields": [{"fieldTag": "y","marcTag": "008","content": "||||||s                      000 | {} d".format(language)}]} 
    url = config['api']['base_url'] + "/bibs/" + bib_id
    header = {"Authorization": "Bearer " + token, "Content-Type": "application/json;charset=UTF-8"}

    request = s.put(url, data=json.dumps(bibpatch), headers = header)
    
def main():
	
    config = configparser.ConfigParser()
    config.read('api_info.ini')
    #open API session using sierra_ils_utils library
    
    #testing with query to pull up known test record in Sierra
    query = """\
    SELECT
        rm.record_num,
        b.language_code

    FROM sierra_view.bib_record b
    LEFT JOIN sierra_view.control_field f
        ON
        b.id = f.record_id AND f.control_num = 8
    JOIN sierra_view.record_metadata rm
        ON
        b.id = rm.id

    WHERE f.id IS NULL
    AND b.language_code !~ '(^eng|^zxx|^und)'
    AND b.language_code !=''
    AND b.is_suppressed = FALSE
    AND b.bcode3 NOT IN ('c', 'n', 'o', 'q', 'r', 'z')

    ORDER BY 1
    """
    
    #connect to Sierra database and run query
    conn = psycopg2.connect("dbname='iii' user='" + config['api']['sql_user'] + "' host='" + config['api']['sql_host'] + "' port='1032' password='" + config['api']['sql_pass'] + "' sslmode='require'")
    cursor = conn.cursor()
    cursor.execute(query)
    results = cursor.fetchall()
    conn.close()
    
    #start up a requests session
    token = get_token()
    s = requests.Session()
    #for each row in query results call mod_bib
    for rownum, row in enumerate(results):
        mod_bib(str(row[0]),row[1],token,s)

main()

