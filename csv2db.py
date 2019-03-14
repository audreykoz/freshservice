import pandas as pd 
import numpy
import re
import json 
import requests 
import freshlogin as fresh 


def get_calls():
    """Returns number of API calls made in Python session
    """
    get_calls.counter +=1 


get_calls.counter = 0     


def get_tickets():
    """Returns all tickets from freshservice 
        
    """
    headers={'Content-Type': 'application/json'}
    tickets = requests.get(f"https://{fresh.domain}.freshservice.com/helpdesk/tickets.json", headers=headers,auth=(fresh.user,fresh.password))
    get_calls()
    return json.loads(tickets.content)


def get_changes(): 
    """Returns all changes from freshservice 
        
    """
    headers={'Content-Type': 'application/json'}
    changes = requests.get(f"https://{fresh.domain}.freshservice.com/itil/changes.json", headers=headers,auth=(fresh.user,fresh.password))
    get_calls()
    return json.loads(changes.content)


def get_releases(): 
    """Returns all releases from freshservice 
        
    """
    headers={'Content-Type': 'application/json'}
    releases = requests.get(f"https://{fresh.domain}.freshservice.com/itil/releases.json", headers=headers,auth=(fresh.user,fresh.password))
    get_calls()
    return json.loads(releases.content)


def get_problems():
    """Returns all problems from freshservice 
   
    """
    headers={'Content-Type': 'application/json'}
    problems = requests.get(f"https://{fresh.domain}.freshservice.com/itil/problems.json", headers=headers,auth=(fresh.user,fresh.password))
    get_calls()
    return json.loads(problems.content)


def get_tasks():
    """Returns all agent tasks from freshservice 
    
    """
    headers={'Content-Type': 'application/json'}
    tasks = requests.get(f"https://{fresh.domain}.freshservice.com/itil/it_tasks.json", headers=headers,auth=(fresh.user,fresh.password))
    get_calls()
    return json.loads(tasks.content)


def get_users():
    """Returns all users from freshservice 
    
    """
    headers = {'Content-Type': 'application/json'}
    users = requests.get(f"https://{fresh.domain}.freshservice.com/itil/requesters.json", headers=headers,auth=(fresh.user,fresh.password))
    get_calls()
    return json.loads(users.content)


def get_agents():
    """Returns all agents from freshservice 
        
    """
    headers = {'Content-Type': 'application/json'}
    agents = requests.get(f"https://{fresh.domain}.freshservice.com/agents.json", headers=headers,auth=(fresh.user,fresh.password))
    get_calls()
    return json.loads(agents.content)


def get_rela_types(): 
    """Returns relationship types from freshservice 
    
    """
    name = requests.get(f"https://{fresh.domain}.freshservice.com/cmdb/relationship_types/list.json", auth=(fresh.user,fresh.password))
    get_calls()
    return json.loads(name.content)


def get_asset_types():
    """Returns asset types from freshservice 
        
    """
    name = requests.get(f"https://{fresh.domain}.freshservice.com/cmdb/ci_types.json", auth=(fresh.user,fresh.password))
    get_calls()
    return json.loads(name.content)


def get_assets(rela=False, dwnl_csv=False):
    """Returns all assets/CIs from freshservice 
    
    Parameters
    ----------
    rela: bool, optional 
        If True, asset relationships will be displayed. Default is False
        
    """
    asset_table = pd.DataFrame() #Create empty dataframe
    page_num = 1 #start pagination at 1
    pages_left = True #start with more than 1 page of data as default
    while pages_left: #while we have pages left
        name = requests.get(f"https://{fresh.domain}.freshservice.com/cmdb/items.json?page="+str(page_num), auth = (fresh.user, fresh.password)) #get assets
        get_calls() #add 1 to call counter
        content = json.loads(name.content) #load asset content as json data
        page_num += 1 
        asset_data = pd.DataFrame(content)
        asset_table = asset_table.append(asset_data,ignore_index=True) #append asset content onto created pandas dataframe
        if len(content) == 0: #if there's no content in this page, stop loop
            pages_left = False
    if rela: #if asking for relationship data as well
        asset_table["relationship_data"] = numpy.nan #add extra row onto pandas dataframe
        for index,row in asset_table.iterrows(): #for each thing in asset table, check if there's
                                                 # relationship data connected to it
            r = requests.get(f"https://{fresh.domain}.freshservice.com/cmdb/items/"+str(row["display_id"])+"/relationships.json", auth = (fresh.user, fresh.password))
            get_calls()
            relationship_data = json.loads(r.content)
            asset_table.loc[index,"relationship_data"] = relationship_data #append relationship onto our created row
                                                                           # at the index of the item it belongs to
        if dwnl_csv: #if user wants csv of data, download it and return table
            asset_table.to_csv("freshservice_export.csv")
            return asset_table
        else:    
            return asset_table    
    else: #if you don't want relationship data, just download csv and display data
        if dwnl_csv: 
            asset_table.to_csv("freshservice_export.csv")
            return asset_table
        else:    
            return asset_table    
    

def add_assets(csv="data.csv"):
    """Add assets/CIs to freshservice CMDB
    
    Parameters
    ----------
    csv: str, optional
        Specifies the exported Archi file that contains assets/CIs that should be uploaded. 
        Default value is "data.csv"

    """
    csv_data = pd.read_csv(csv)
    upload_data = pd.DataFrame(columns=["Name","Type","GUID","Documentation"])
    data_dict = {"ApplicationComponent":10000207420,
                 "ApplicationInterface":10000282585,
                 "Requirement":10000282494}
    names = [item for item in csv_data.Name]
    types = [data_dict[item] for item in csv_data.Type]
    guids = [item for item in csv_data.ID]
    documentation = [item for item in csv_data.Documentation]
    upload_data.Name = names
    upload_data['Name'].str.replace(r"\(.*\)","")
    upload_data.Type = types
    upload_data.GUID = guids
    upload_data.Documentation= documentation
    for index, row in upload_data.iterrows():
        new_label = 'guid_'+str(row.Type)
        d = {'cmdb_config_item':{'name': re.sub(r'\([^)]*\)', '', str(row.Name))
,'ci_type_id': str(row.Type),'description':str(row.Documentation), "level_field_attributes":{new_label:row.GUID}}}
        data = json.dumps(d)
        headers = {'Content-Type': 'application/json'}
        response = requests.post(f"https://{fresh.domain}.freshservice.com/cmdb/items.json", headers=headers, data=data, auth=(fresh.user, fresh.password))
        get_calls()
        print(response.content)


def add_rela(rela_data="relationships.csv",asset_data="data.csv"):
    """Add relationships to assets in freshservice CMDB. Assets must exist in the CMDB
    for a relationship to be created. 
    
    Parameters
    ----------
    rela_data: str, optional 
        Specifies the exported Archi file that contains the relationships to be created.
        Default value is "relationships.csv"
    item_data: str, optional
        Specifies the exported Archi file that contains assets/CIs that were uploaded to freshservice 
        Default value is "data.csv"
   """     
    rela_data = pd.read_csv(rela_data)
    asset_data = pd.read_csv(asset_data)
    rela_dict = {"CompositionRelationship": "10000085526",
                 "RealizationRelationship": "10000126146",
                 "AccessRelationship": "10000126147",
                 "AssignmentRelationship": "10000138549",
                 'FlowRelationship': "10000138550",
                 'TriggeringRelationship': "10000126148",
                 'AssociationRelationship': "10000126149",
                 'ServingRelationship': "10000126173"}
    asset_dict = {"ApplicationComponent": 10000207420,
                  "ApplicationInterface": 10000282585,
                  "Requirement": 10000282494}
    
    new_data = pd.DataFrame(columns=["Name","Type","GUID"])
    new_data.Name = [re.sub(r'\([^)]*\)', '', str(item)) for item in asset_data.Name]
    new_data.Type = [asset_dict[item] for item in asset_data.Type]
    guids = [item for item in asset_data.ID]
    new_data.GUID = guids
    
    babysourcedata = rela_data.loc[rela_data["Source"].isin(guids) & rela_data["Target"].isin(guids)]
    
    final_source_guid = [row.Source for index, row in babysourcedata.iterrows()]
    final_target_guid = [row.Target for index, row in babysourcedata.iterrows()]
    final_source_name = [re.sub(r'\([^)]*\)', '', str(asset_data.loc[asset_data['ID'] == item, 'Name'].item())) for item in final_source_guid]
    final_target_name = [re.sub(r'\([^)]*\)', '', str(asset_data.loc[asset_data['ID'] == item, 'Name'].item())) for item in final_target_guid]
    final_rela_type_id = [rela_dict[row.Type] for index, row in babysourcedata.iterrows()]

    asset_table = get_assets()
               
    asset_table_names = {row["name"]:row["display_id"] for index, row in asset_table.iterrows()}
  
    for item,second_item, third in zip(final_source_name, final_target_name,final_rela_type_id):
        headers = {'Content-Type': 'application/json'}
        second_item = str(second_item).strip()
        item = str(item).strip()
        dictionary = {"type":"config_items", "type_id":[asset_table_names[second_item]], "relationship_type_id": third, "relationship_type":"forward_relationship"}
        data2 = json.dumps(dictionary)
        try:
            response = requests.post(f"https://{fresh.domain}.freshservice.com/cmdb/items/"+str(asset_table_names[item])+'/associate.json', headers=headers, data=data2, auth=(fresh.api_key, fresh.password))
            get_calls()
            print(response.content)
        except: 
            print(f"There was an error uploading the relationship with a source of {item} and a target of {second_item}.")
            pass


def delete_asset(display_id):
    """Delete a specified Asset/CI from the freshservice CMDB 
    
    Parameters
    ----------
    display_id : str
        Specify the display_id of the asset/CI you would like to delete from 
        the CMDB. 
    """
    headers = {'Content-Type': 'application/json'}
    testing = requests.delete(f"https://{fresh.domain}.freshservice.com/cmdb/items/"+str(display_id)+".json", headers=headers, auth=(fresh.api_key, fresh.password))
    get_calls()
    return json.loads(testing.content)


def restore_asset(display_id):
    """Restore a deleted Asset/CI from the freshservice CMDB
    
    Parameters
    ----------
    display_id : str
        Specify the display_id of the asset/CI you would like to restore in 
        the CMDB. 
    """
    headers = {'Content-Type': 'application/json'}
    restore = requests.put(f"https://{fresh.domain}.freshservice.com/cmdb/items/"+str(display_id)+"/restore.json", headers=headers, auth=(fresh.api_key, fresh.password))
    get_calls() 
    return json.loads(restore.content)


def search_assets(field_param,query_param):
    """Search items in the freshservice CMDB
    
    Parameters
    ----------
    field_param: str
        Specifies which field should be used to search for an asset. Allowed parameters 
        are 'name', 'asset_tag', or 'serial_number.
    query_param: str 
        What you would like to search for based off of the field_param. For example, 
        field_param="name", query_param="andrea". 
    """
    headers={'Content-Type': 'application/json'}
    search = requests.get(f"https://{fresh.domain}.freshservice.com/cmdb/items/list.json?field="+field_param+"&q="+query_param, headers=headers,auth=(fresh.user,fresh.password))
    get_calls()
    return json.loads(search.content)
    
    
    
   
    
    
    
    