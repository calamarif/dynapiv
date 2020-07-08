__author__ = 'Callum'
#dynapiv.py - Dynamically pivot a project by updating it's dataset
#Python 3.7.3
#Version: 0.2
#Date: Jul 7th 2020

import requests, json, copy
from requests.auth import HTTPBasicAuth

def get_project_script(auth_token,paxata_url,projectId):
    url_request = (paxata_url + "/scripts?projectId=" + projectId + "&version=" + "-1")
    myResponse = requests.get(url_request, auth=auth_token)
    if (myResponse.ok):
        # Loads (Load String) takes a Json file and converts into python data structure (dict or list, depending on JSON)
        json_of_project = json.loads(myResponse.content)
    else:
        json_of_project = 0
        myResponse.raise_for_status()
    return(json_of_project)

# Update an existing Project's script file
def update_project_with_new_script(auth_token,paxata_url,updated_json_script,projectId):
    url_request = (paxata_url + "/scripts?update=script&force=true&projectId=" + str(projectId))
    s = {'script': json.dumps(updated_json_script)}
    myResponse = requests.put(url_request, data=s, auth=auth_token)
    result = False
    if (myResponse.ok):
        # json_of_existing_project = json.loads(myResponse.content)
        result = True
    else:
        #if there is a problem in updating the project, it would indicate a problem with the script, so lets output it
        print(myResponse.content)
        result = False
    return result

def run_a_project(auth_token,paxata_url,projectId):
    post_request = (paxata_url + "/project/publish?projectId=" + projectId)
    postResponse = requests.post(post_request, auth=auth_token)
    if (postResponse.ok):
        print("Project Run - " + projectId)
    else:
        print("Something went wrong with POST call " + str(postResponse))
    # I need to investigate the below, sometimes postResponse.content is a dict, sometimes a list, hence the two below trys
    try:
        AnswersetId = json.loads(postResponse.content)[0].get('dataFileId')
    except(AttributeError):
        AnswersetId = json.loads(postResponse.content).get('dataFileId', 0)
    return AnswersetId

# (3) POST Library data from Paxata and load it into a JSON structure
def get_paxata_library_data(auth_token,paxata_url,library_dataset_id):
    post_request = (paxata_url + "/datasource/exports/local/" + library_dataset_id + "?format=json")
    post_response = requests.post(post_request,auth=auth_token)
    JsonData = ""
    if (post_response.ok):
        JsonData = json.loads(post_response.content)
    return JsonData

def update_project_with_new_dataset(auth_token,paxata_url,updated_json_script,projectId):
    url_request = (paxata_url + "/rest/scripts?update=datasets&force=true&projectId=" + str(projectId))
    s = {'script': json.dumps(updated_json_script)}
    myResponse = requests.put(url_request, data=s, auth=auth_token)
    result = False
    print(myResponse)
    if (myResponse.ok):
        # json_of_existing_project = json.loads(myResponse.content)
        result = True
    else:
        #if there is a problem in updating the project, it would indicate a problem with the script, so lets output it
        print(myResponse.content)
        result = False
    return result

def get_name_and_schema_of_datasource(auth_token,paxata_url,libraryId):
    url_request = (paxata_url + "/library/data/"+str(libraryId))
    my_response = requests.get(url_request, auth=auth_token, verify=True)
    library_name = ""
    library_schema_dict = ""
    if(my_response.ok):
        jdata_datasources = json.loads(my_response.content)
        library_name = jdata_datasources[0].get('name')
        library_version = jdata_datasources[0].get('version')
        library_schema_dict = jdata_datasources[0].get('schema')
    return library_name,library_version,library_schema_dict

# Not used in this script
def insert_initial_data_into_empty_project(auth_token,paxata_url,json_of_existing_project,libraryId):
    #update the script... take the existing script and manipulate it.
    updated_json_script = copy.deepcopy(json_of_existing_project)
    updated_json_script['steps'][0]['importStep']['libraryId'] = str(libraryId)
    updated_json_script['steps'][0]['importStep']['libraryVersion'] = 1
    updated_json_script['steps'][0]['importStep']['libraryIdWithVersion'] = str(libraryId) + "_" + str(1)
    #function to get the metadata
    library_name,library_version,library_schema_dict = get_name_and_schema_of_datasource(auth_token,paxata_url,libraryId)
    i=0
    for schema_item in json_of_datasource_schema:
        temp_name = schema_item.get('name')
        temp_type = schema_item.get('type')
        updated_json_script['steps'][0]['importStep']['columns'].insert(i,{'hidden': False})
        updated_json_script['steps'][0]['importStep']['columns'][i]['columnDisplayName'] = temp_name
        updated_json_script['steps'][0]['importStep']['columns'][i]['columnType'] = temp_type
        updated_json_script['steps'][0]['importStep']['columns'][i]['columnName'] = temp_name
        #go to the next element
        i+=1

    return(updated_json_script)

def update_project_script_with_new_libraryid(temp_json_of_project, libraryId, library_version):
    #update the datasource id and version values (assumes the schema is the same)
    for step in temp_json_of_project['steps']:
        if (step['type']) == "AnchorTable":
            step['importStep']['libraryId'] = libraryId
            step['importStep']['libraryIdWithVersion'] = str(libraryId) + "_" + str(library_version)
            step['importStep']['libraryVersion'] = library_version
    return(temp_json_of_project)


def update_project(event, context):
    # *****************THESE ARE YO VARIABLES - YOU NEED TO EDIT THESE *******
    paxata_url = event['paxata_url']
    paxata_rest_token = event['paxata_rest_token']
    projectId = event['projectId']
    libraryId = event['libraryId']

    # variables to set
    #paxata_rest_token = "47dfcdd37fa64428acffc0ed32f93e61"
    # ID of the project you want to dynamically update the script of
    #projectId = "37969e3b0b0b49e1bc1d8ace785343d4"
    # This is the libraryId of the dataset that will update the project
    #libraryId = "d285c04e4d63456d97e8b6daf8d682f4"
    #paxata_url = "https://dataprep.paxata.com/rest"
    # end of variables to set

    auth_token = HTTPBasicAuth("", paxata_rest_token)

    # Open config files
    json_deduplicate_config = '{"type": "Pivot","active": true,"publishPoints": null,"annotation": null,"anchors": ["FIELD 1 OF 2 GOES HERE"],"columnNames": [],"aggregateFunctions": [{"columnName": "FIELD 2 OF 2 GOES HERE","newColumnName": "","aggregateType": "Sum"}],"pivotValues": [],"unpivotColumnName": "Columns","unpivotMetricName": "Values","pivotType": "DeDuplicate","dedupMode": "Exact","facets": null,"muteFacets": null,"$validationErrors": {"Error": [],"Warning": []},"color": "#999"}'
    json_deduplicate_config = json.loads(json_deduplicate_config)

    #get the json of the project we want to update
    json_of_project = get_project_script(auth_token,paxata_url,projectId)

    #existing pivot values
    for step in json_of_project['steps']:
        if (step['type']) == "Pivot":
            previous_pivot_value = step['pivotValues']
            existing_pivot_values_anchors = step['anchors']
            existing_pivot_column_name = step['columnNames']
            existing_pivot_values_aggregateFunctions_columnName = step['aggregateFunctions'][0]['columnName']

    #Update the dedup config.json to the column that is used in the pivot (in the project)
    json_deduplicate_config['aggregateFunctions'][0]['columnName'] = existing_pivot_values_aggregateFunctions_columnName
    json_deduplicate_config['anchors'] = existing_pivot_column_name

    #take a deep copy of the json dictionary so we're not updating the same thing
    temp_json_of_project = copy.deepcopy(json_of_project)
    #remove all but the first step from the project
    del temp_json_of_project['steps'][1:]
    #then add a dedupicate step to get the new values
    temp_json_of_project['steps'].append(json_deduplicate_config)

    # Get the library version
    library_name, library_version, library_schema = get_name_and_schema_of_datasource(auth_token,paxata_url,libraryId)
    
    temp_json_of_project = update_project_script_with_new_libraryid(temp_json_of_project, libraryId, library_version)

    # This might be used if the schema changes:
    #updated_json_script = insert_initial_data_into_empty_project(auth_token,paxata_url,json_of_existing_project,libraryId)

    # Update 1/2 to get the new column names
    result = update_project_with_new_script(auth_token,paxata_url,temp_json_of_project,projectId)

    answerset_id = run_a_project(auth_token,paxata_url,projectId)
    pivot_columns_json = get_paxata_library_data(auth_token,paxata_url,answerset_id)

    # nested for loop to extract the new column names (in a list)
    pivot_columns_names = [v for lst in pivot_columns_json['data'] for k, v in lst.items()]
    temp_list = []
    #create the structure we need (a list of lists)
    for item in pivot_columns_names:
        temp_list.append([item]) 

    # Update the main project script with the new columns
    for step in json_of_project['steps']:
        if (step['type']) == "Pivot":   
            step['pivotValues']= temp_list

    json_of_project = update_project_script_with_new_libraryid(json_of_project, libraryId, library_version)
    # Update 2/2 to get the new column names
    result = update_project_with_new_script(auth_token,paxata_url,json_of_project,projectId)
    if result:
        print ("Successfully updated project: " + projectId + ", with Library: " + libraryId)
    else:
        print ("Something went wrong")

if __name__ == "__main__":
    update_project()
