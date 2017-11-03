import requests
from bs4 import BeautifulSoup
import xlsxwriter
from datetime import datetime
from datetime import timedelta
import json
import pandas as pd

# First api call to get the api key

base_url = 'https://pi.pardot.com/api'
Login_url = base_url + '/login/version/3'
user_key_value = '6912e458afcefc57259dd2c1d63a298f'
emailid = 'ravisingh6@deloitte.com'
path_of_final_tagslist = 'C:\\Users\\ravisingh6\\Desktop\\hugo\\Post_Dev\\final_tag_list\\CopyofFinalTagsforFAST_10_29.csv'

def Login_function(emailid,user_key_value,Login_url):
    Login_request = requests.post(Login_url,
                                  data={'email': emailid, 'password': 'Deloitte!23', 'user_key': user_key_value},
                                  verify=False)
    print(Login_request.content)
    print(Login_request.status_code)
    soup = BeautifulSoup(Login_request.content, 'lxml')
    return soup.api_key.text

def Tag_api_call(user_key_value,api_key_value,base_url):
    print(user_key_value + ' ----->' + api_key_value)
    pardt_tags_url = base_url + '/tag/version/3/do/query'
    tag_count_id = 0
    lsttgs = []
    Mapidtagname = {}
    count = 200
    Numberofwhilerun = 0

    # loop to get all the tags and there ids
    while (count > 0):
        pardot_tags_req = requests.post(pardt_tags_url,
                                        data={'api_key': api_key_value, 'user_key': user_key_value, 'output': 'bulk',
                                              'limit': 200, 'id_greater_than': tag_count_id, 'sort_by': 'id'},
                                        verify=False)
        pardot_tags_resp_parsd = BeautifulSoup(pardot_tags_req.content, 'lxml')
        #print(pardot_tags_resp_parsd.prettify())
        lsttgs.extend(pardot_tags_resp_parsd.find_all('tag'))
        tagid = pardot_tags_resp_parsd.find_all('id')
        count = len(tagid)
        if count > 0 :
            tag_count_id = int(tagid[len(tagid) - 1].text)
        Numberofwhilerun = Numberofwhilerun + 1
    print('Number of times while loop run: ' + str(Numberofwhilerun))
    print('line number 50' + str(count))

    # Creating a map of id and tag
    for tag in lsttgs:
        Mapidtagname[int(tag.id.text)] = tag.find('name').text

    return Mapidtagname

def TagObjectfunction(user_key_value,api_key_value,base_url,tag_id_value):
    id_value = 0
    Tagobject_req_url = base_url + '/tagObject/version/3/do/query'
    TagObject_req = requests.post(Tagobject_req_url,
                                  data={'api_key': api_key_value, 'user_key': user_key_value,
                                        'type': 'Prospect',
                                        'sort_by': 'id', 'id_greater_than': id_value,
                                        'limit': 1, 'tag_id': tag_id_value, 'format': 'json'},
                                  verify=False)
    #print(TagObject_result)
    TagObject_result = TagObject_req.json()
    #print(TagObject_result)
    return TagObject_result


api_key_value = Login_function(emailid, user_key_value,Login_url)
Tag_stringMap = {}
if api_key_value == 'Login failed':
    print('user key incorrect')
else:
    Mapidtagname = Tag_api_call(user_key_value, api_key_value, base_url)
    print(Mapidtagname)
    req_count = 0
    #df2 = pd.DataFrame(list(Mapidtagname.items()), columns=['Tagid', 'Name'])
    #df2.to_csv('C:\\Users\\ravisingh6\\Desktop\\Tags\\TagnumapOldinstancemap_10_20_again.csv')
    for tag_id_value in Mapidtagname:
        print(req_count)
        TagObject_result = TagObjectfunction(user_key_value, api_key_value, base_url, tag_id_value)
        Tag_string = TagObject_result['result']['total_results']
        Tag_stringMap[tag_id_value] = TagObject_result['result']['total_results']
        req_count +=1
        #break
    #print(Tag_stringMap)
    df = pd.DataFrame(list(Tag_stringMap.items()), columns=['Tagid', 'Count'])
    df2 = pd.DataFrame(list(Mapidtagname.items()), columns=['Tagid', 'Name'])
    df3 = df.merge(df2, on='Tagid')
    df4 = pd.read_csv(path_of_final_tagslist)
    df4['Tags to Migrate'] = df4['Tags to Migrate'].str.lower()
    dd = df3.merge(df4, left_on='Name', right_on='Tags to Migrate', how='inner')
    dd.to_csv('C:\\Users\\ravisingh6\\Desktop\\Tags\\Tagcountmap10_30.csv')