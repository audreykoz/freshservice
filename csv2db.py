import pandas as pd 
import numpy 
from numpy import random as random
import re as re 
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

def get_my_tasks():
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

def get_item_types(): 
    """Returns asset types from freshservice 
        
    """
    name = requests.get(f"https://{fresh.domain}.freshservice.com/cmdb/ci_types.json", auth=(fresh.user,fresh.password))
    get_calls()
    return json.loads(name.content)

def get_items(rela=False):
    """Returns all assets/CIs from freshservice 
    
    Parameters
    ----------
    rela: bool, optional 
        If True, asset relationships will be displayed. Default is False
        
    """
    asset_table = pd.DataFrame()
    page_num = 1
    freshtest1 = True 
    while freshtest1 == True: 
        name = requests.get(f"https://{fresh.domain}.freshservice.com/cmdb/items.json?page="+str(page_num), auth = (fresh.user, fresh.password)) 
        get_calls()
        content = json.loads(name.content)
        page_num += 1 
        asset_test = pd.DataFrame(content)
        asset_table = asset_table.append(asset_test,ignore_index=True)
        if len(content) == 0: 
            freshtest1 = False 
    if rela: 
        index = [item for item in asset_table.display_id]
        for item in index:
            r = requests.get(f"https://{fresh.domain}.freshservice.com/cmdb/items/"+str(item)+"/relationships.json", auth = (fresh.user, fresh.password))
            get_calls()
            relationship_data = json.loads(r.content)
            name = requests.get(f"https://{fresh.domain}.freshservice.com/cmdb/items/"+str(item)+".json", auth = (fresh.user, fresh.password))
            get_calls()
            namedata = json.loads(name.content)
            print("Asset:\n",namedata)
            print("Associated Relationships:\n",relationship_data)
            print("")
    else:         
        return asset_table

def add_items(item_data="data.csv"): 
    """Add assets/CIs to freshservice CMDB
    
    Parameters
    ----------
    item_data: str, optional
        Specifies the exported Archi file that contains assets/CIs that should be uploaded. 
        Default value is "data.csv"
    """
    item_data = pd.read_csv(item_data)
    new_data = pd.DataFrame(columns=["Name","Type","GUID","Documentation"])
    data_dict = {"ApplicationComponent":10000207420,
                 "ApplicationInterface":10000282585,
                 "Requirement":10000282494}
    names = [item for item in item_data.Name]
    types = [data_dict[item] for item in item_data.Type]
    guids = [item for item in item_data.ID]
    documentation = [item for item in item_data.Documentation]
    new_data.Name = names 
    new_data['Name'].str.replace(r"\(.*\)","")
    new_data.Type = types
    new_data.GUID = guids
    new_data.Documentation= documentation
    for index, row in new_data.iterrows():
        new_label = 'guid_'+str(row.Type)
        d = {'cmdb_config_item':{'name': re.sub(r'\([^)]*\)', '', str(row.Name))
,'ci_type_id': str(row.Type),'description':str(row.Documentation), "level_field_attributes":{new_label:row.GUID}}}
        data = json.dumps(d)
        headers = {'Content-Type': 'application/json'}
        print(data)
        response = requests.post(f"https://{fresh.domain}.freshservice.com/cmdb/items.json", headers=headers, data=data, auth=(fresh.user, fresh.password))
        get_calls()
        print(response.content)
        
def add_rela(rela_data = "relationships.csv",item_data="data.csv"):
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
    item_data = pd.read_csv(item_data)
    rela_dict = {"CompositionRelationship":"10000085526",
                     "RealizationRelationship":"10000126146",
                     "AccessRelationship":"10000126147",
                     "AssignmentRelationship": "10000138549", 
                     'FlowRelationship':"10000138550",
                     'TriggeringRelationship':"10000126148",
                     'AssociationRelationship':"10000126149",
                     'ServingRelationship':"10000126173"}
    data_dict = {"ApplicationComponent":10000207420,
                 "ApplicationInterface":10000282585,
                 "Requirement":10000282494}
    
    new_data = pd.DataFrame(columns=["Name","Type","GUID"])
    names = [re.sub(r'\([^)]*\)', '', str(item)) for item in item_data.Name]
    types = [data_dict[item] for item in item_data.Type]
    guids = [item for item in item_data.ID]
    
    new_data.Name = names
    new_data.Type = types
    new_data.GUID = guids
  
    new_rela_data = pd.DataFrame(columns=["Source_Name","Source_GUID","Target_Name","Target_GUID","Rela_Name","Rela_Type_ID","Rela_GUID"])
    
    babysourcedata = rela_data.loc[rela_data["Source"].isin(guids) & rela_data["Target"].isin(guids)]
    
    final_source_guid = [row.Source for index, row in babysourcedata.iterrows()]
    final_target_guid = [row.Target for index, row in babysourcedata.iterrows()]
    final_source_name = [re.sub(r'\([^)]*\)', '', str(item_data.loc[item_data['ID'] == item, 'Name'].item())) for item in final_source_guid]
    final_target_name = [re.sub(r'\([^)]*\)', '', str(item_data.loc[item_data['ID'] == item, 'Name'].item())) for item in final_target_guid]
    final_relationship_type = ["forward_relationship" for index, row in babysourcedata.iterrows()]
    final_rela_type_id = [rela_dict[row.Type] for index, row in babysourcedata.iterrows()]
    final_rela_guid = [row.ID for index, row in babysourcedata.iterrows()]
   
    page_num = 1
    asset_table = pd.DataFrame()
    freshtest1 = True 
    while freshtest1 == True: 
        name = requests.get(f"https://{fresh.domain}.freshservice.com/cmdb/items.json?page="+str(page_num), auth = (fresh.user, fresh.password)) 
        get_calls()
        content = json.loads(name.content)
        page_num += 1
        asset_test = pd.DataFrame(content)
        asset_table = asset_table.append(asset_test,ignore_index=True)
        if len(content) == 0: 
            freshtest1 = False 
               
    asset_table_names = {row["name"]:row["display_id"] for index, row in asset_table.iterrows()}
  
    for item,second_item, third in zip(final_source_name, final_target_name,final_rela_type_id):
        headers = {'Content-Type': 'application/json'}
        second_item = str(second_item).strip()
        item = str(item).strip()
        dictionary = {"type":"config_items", "type_id":[asset_table_names[second_item]], "relationship_type_id": third, "relationship_type":"forward_relationship"}
        data2 = json.dumps(dictionary)
        try:
            response = requests.post(f"https://{fresh.domain}.freshservice.com/cmdb/items/'+str(asset_table_names[item])+'/associate.json', headers=headers, data=data2, auth=(fresh.api_key, fresh.password))
            get_calls()
            print(response.content)
        except: 
            print("error")
            pass

def delete_item(display_id): 
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
    
def restore_item(display_id): 
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
    
def search_items(field_param,query_param):
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
    
    
    
   
    
    
    
    