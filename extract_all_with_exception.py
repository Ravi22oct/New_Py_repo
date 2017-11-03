import requests
from bs4 import BeautifulSoup
import xlsxwriter
from datetime import datetime
from datetime import timedelta
import json
import pandas as pd
import numpy as np

def Login_function(emailid,user_key_value,Login_url):
    Login_request = requests.post(Login_url,
                                  data={'email': emailid, 'password': 'Deloitte!23', 'user_key': user_key_value},
                                  verify=False)
    #print(Login_request.content)
    print(Login_request.status_code)
    soup = BeautifulSoup(Login_request.content, 'lxml')
    return soup.api_key.text

def TagObjectfunction(user_key_value,api_key_value,base_url,tag_id_value, noofloops, tag_name, MapPrstidtagname, Count_val):
    id_value = 0
    locallst = []
    err_code = 0
    i = 0
    for i in range(i,noofloops):
        Tagobject_req_url = base_url + '/tagObject/version/3/do/query'
        TagObject_req = requests.post(Tagobject_req_url,
                                      data={'api_key': api_key_value, 'user_key': user_key_value,
                                            'type': 'Prospect',
                                            'sort_by': 'id', 'id_greater_than': id_value,
                                            'limit': 200, 'tag_id': tag_id_value, 'format': 'json', 'output': 'bulk'},
                                      verify=False)

        TagObject_result = TagObject_req.json()
        print(TagObject_result)
        if TagObject_result['@attributes']['stat'] == 'ok':
            if TagObject_result['result'] is not None:
                listrespvalue = TagObject_result['result']['tagObject']
                print('Length of result: ' + str(len(listrespvalue)))
                print('Tagname: ' + tag_name)
                if Count_val == 1:
                    locallst.append(listrespvalue['object_id'])
                else:
                    try:
                        id_value = listrespvalue[len(listrespvalue) - 1]['id']
                        for y in listrespvalue:
                            locallst.append(y['object_id'])
                    except Exception as tp:
                        locallst.append(listrespvalue['object_id'])
                        print('Handled Error: ' + str(tp))
                        locallst.append(listrespvalue['object_id'])
                    #print('last_prospectid_value: ' + str(id_value))


        else:
            err_code = TagObject_result['@attributes']['err_code']
            break

    #print(locallst)
    #print(listrespvalue)
    if err_code == 0:
        if locallst:
            for prospectid in locallst:
                if prospectid in MapPrstidtagname:
                    MapPrstidtagname[prospectid] += (tag_name + ',')
                else:
                    MapPrstidtagname[prospectid] = (tag_name + ',')

    return (MapPrstidtagname,err_code)

# Get list of tag id values
def  listoftagid(tagid_file_path):
    df = pd.read_csv(tagid_file_path, usecols=['Tagid', 'Name', 'Count'])
    #print(type(df))
    df['Count_div'] = df['Count']/200
    df['noofloops'] = df['Count_div'].apply(np.ceil)
    #df.drop(['Tags to Migrate', 'Count_div'], axis=1, inplace=True)
    abc = df.loc[((df['Count'] > 0) & (df['noofloops'] < 1000)), ['Tagid', 'Name', 'Count', 'noofloops']]
    #print(abc.shape)
    return abc


def main():
    base_url = 'https://pi.pardot.com/api'
    Login_url = base_url + '/login/version/3'
    user_key_value = '6912e458afcefc57259dd2c1d63a298f'
    emailid = 'ravisingh6@deloitte.com'
    tagid_file_path = 'C:\\Users\\ravisingh6\\Desktop\\Tags\\Tagcountmap10_30.csv'
    Path_to_save = 'C:\\Users\\ravisingh6\\Desktop\\Tags\\Test_Final_10_31.csv'
    MapPrstidtagname = {}
    listoftagstorun = listoftagid(tagid_file_path)
    # print(listoftagstorun)
    api_key_value = Login_function(emailid, user_key_value, Login_url)
    tagids = ()
    tagids = tuple(listoftagstorun['Tagid'])
    listoftagstorun.set_index('Tagid', inplace=True)
    for tag_id_val in tagids:
        tag_id_value = int(tag_id_val)
        # print(tag_id_value)
        # print(type(tag_id_value))
        Count_val = int(listoftagstorun.loc[tag_id_val, 'Count'])
        noofloops = int(listoftagstorun.loc[tag_id_val, 'noofloops'])
        tag_name = listoftagstorun.loc[tag_id_val, 'Name']
        print('tag_name: ' + tag_name)
        print('noofloops :' + str(noofloops))
        # print('tagname & numberofloops' + str(noofloops) + tag_name)
        # print(MapPrstidtagname)
        (MapPrstidtagname, err_code) = TagObjectfunction(user_key_value, api_key_value, base_url, tag_id_value,
                                                         noofloops, tag_name,
                                                         MapPrstidtagname, Count_val)
        if err_code != 0:
            if err_code == 1:
                api_key_value = Login_function(emailid, user_key_value, Login_url)
                (MapPrstidtagname, err_code) = TagObjectfunction(user_key_value, api_key_value, base_url, tag_id_value,
                                                                 noofloops, tag_name,
                                                                 MapPrstidtagname, Count_val)


        # print(MapPrstidtagname)
    df = pd.DataFrame(list(MapPrstidtagname.items()), columns=['Prospectid', 'Tagname'])
    df.to_csv(Path_to_save)
    #print(df)

if __name__ == '__main__':
    main()

