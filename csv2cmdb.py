"""
These functions read/write data generated from Archi into Freshservice's CMDB via API.
"""

import re
import json
import difflib
import datetime as dt
import requests
import numpy
import pandas as pd
import freshlogin as fresh


ASSET_DICT = {"ApplicationComponent": "10001075119",
              "ApplicationInterface": "10001075120",
              "ApplicationService": "10001075121",
              "ApplicationProcess": "10001075122",
              "Artifact": "10001075124",
              "Node": "10001075125",
              "TechnologyInterface": "10001075126",
              "TechnologyProcess": "10001075127",
              "TechnologyService": "10001075128",
              "DataObject": "10001075342",
              "Grouping": "10001075579",
              "Contract": "10001075891",
              "BusinessService": "10000946884",
              "BusinessProcess": "10001075893",
              "Representation": "10001075894",
              "BusinessEvent": "10001075895"
             }

RELA_DICT = {'CompositionRelationship': '10000527161',
             'RealizationRelationship': '10000527162',
             'AccessRelationship': '10000527160',
             'AssignmentRelationship': '10000527163',
             'FlowRelationship': '10000527164',
             'TriggeringRelationship': '10000527166',
             'AssociationRelationship': '10000527165',
             'ServingRelationship': '10000527167'}


def get_rela_ids():
    """Return a dictionary consisting of Archi relationship types paired with their
    Freshservice relationship IDs.
    """
    match_dict = ['CompositionRelationship',
                  'RealizationRelationship',
                  'AccessRelationship',
                  'AssignmentRelationship',
                  'FlowRelationship',
                  'TriggeringRelationship',
                  'AssociationRelationship',
                  'ServingRelationship']
    rela_dict = {}
    for item in get_rela_types():
        early_creation = dt.datetime(2018, 8, 2, 0, 0, 0)
        if dt.datetime.strptime(item["created_at"], "%Y-%m-%dT%H:%M:%S-04:00") > early_creation:
            rela = difflib.get_close_matches(item["forward_relationship"].replace(" ", "")
                                             .replace("to", "")
                                             .replace("with", "")+"Relationship", match_dict)[0]
            rela_dict[rela] = str(item["id"])
        else:
            pass
    return rela_dict


def get_calls():
    """Returns number of API calls made in Python session
    """
    get_calls.counter += 1

get_calls.counter = 0


def get_tickets():
    """Returns all tickets from freshservice
    """
    headers = {'Content-Type': 'application/json'}
    tickets = requests.get(f"https://{fresh.domain}/helpdesk/tickets.json",
                           headers=headers, auth=(fresh.user, fresh.password)
                           )
    get_calls()
    return json.loads(tickets.content)


def get_object(item='changes'):
    """Returns specified item from Freshservice.
    Parameters:
    -----------
    item: str
        Specifies what's returned from Freshservice. Options are "changes",
        "releases", "it_tasks","problems",or "requestors".
    """
    if item not in ["changes", "releases", "problems", "it_tasks", "requesters"]:
        raise ValueError(' Requested item must be equal to "changes", "releases","problems",'
                         '"it_tasks", or "requesters" ')
    else:
        headers = {'Content-Type': 'application/json'}
        itil_object = requests.get(f"https://{fresh.domain}/itil/{item}.json",
                                   headers=headers, auth=(fresh.user, fresh.password)
                                   )
        get_calls()
        return json.loads(itil_object.content)


def get_assoc(display_id, assoc):
    """Returns requests or contracts related to an asset.
    Parameters:
    ----------
    display_id: str
        Specify which asset you want to display associated items for.
    assoc: str
        Specify if you want to view contracts or requests associated with an asset.
        Options are "contracts" or "requests"
    """
    headers = {'Content-Type': 'application/json'}
    data = requests.get(f"https://{fresh.domain}/api/v2/assets/{display_id}/{assoc}",
                        headers=headers, auth=(fresh.user, fresh.password)
                        )
    get_calls()
    return json.loads(data.content)


def get_agents():
    """Returns all agents from freshservice
    """
    headers = {'Content-Type': 'application/json'}
    agents = requests.get(f"https://{fresh.domain}/agents.json", headers=headers,
                          auth=(fresh.user, fresh.password)
                          )
    get_calls()
    return json.loads(agents.content)


def get_rela_types():
    """Returns relationship types from freshservice
    """
    name = requests.get(f"https://{fresh.domain}/cmdb/relationship_types/list.json",
                        auth=(fresh.user, fresh.password)
                        )
    get_calls()
    return json.loads(name.content)


def get_asset_types():
    """Returns asset types from freshservice
    """
    name = requests.get(f"https://{fresh.domain}/cmdb/ci_types.json",
                        auth=(fresh.user, fresh.password))
    get_calls()
    return json.loads(name.content)


def get_assets(rela=False, dwnl_csv=False):
    """Returns all assets/CIs from freshservice
    Parameters
    ----------
    rela: bool, optional
        If True, asset relationships will be displayed. Default is False.
    dwnl_csv: bool, optional
        If True, CSV of assets is downloaded to current directory. Created file name is
        "freshservice_export.csv".
    """
    asset_table = pd.DataFrame()
    page_num = 1
    pages_left = True
    while pages_left:
        name = requests.get(f"https://{fresh.domain}/cmdb/items.json?page={str(page_num)}",
                            auth=(fresh.user, fresh.password)
                            )
        data_content = json.loads(name.content)
        page_num += 1
        asset_data = pd.DataFrame(data_content)
        asset_table = asset_table.append(asset_data, ignore_index=True)
        if len(data_content) == 0:
            pages_left = False
    if rela:
        asset_table["relationship_data"] = numpy.nan
        for index, row in asset_table.iterrows():
            request = requests.get(f"https://{fresh.domain}/cmdb/items/{str(row['display_id'])}/relationships.json",
                                       auth=(fresh.user, fresh.password)
                                  )
            get_calls()
            relationship_data = json.loads(request.content)
            asset_table.loc[index, "relationship_data"] = relationship_data
        if dwnl_csv:
            asset_table.to_csv("freshservice_export.csv", index=False)
            return asset_table
    else:
        if dwnl_csv:
            asset_table.to_csv("freshservice_export.csv", index=False)
            return asset_table
    return asset_table      


def add_update_assets(file="", filetype = ""):
    """Adds or updates assets/CIs in freshservice CMDB. Assets not present in the upload
    file, but present in the CMDB will be deleted from the CMDB.
    Parameters
    ----------
    csv: str, optional
        Specifies the exported Archi file that contains assets/CIs that should be uploaded.
        Default value is "elements.csv".
    """
    if filetype == "csv": 
        csv_data = pd.read_csv(file)
    if filetype == "xlsx":
        csv_data = pd.read_excel(file, usecols = "B:E")
    upload_data = pd.DataFrame(columns=["Name", "Type", "GUID", "Documentation"])
    # cnx = sqlite3.connect('/Users/audreykoziol/Desktop/freshservice/Untitled.sqlite')
    # csv_data = pd.read_sql_query("SELECT * FROM elements", cnx)
    # print(csv_data['class'])
    names = [item for item in csv_data.Name]
    types = [ASSET_DICT[item] for item in csv_data.Type]
    guids = [item for item in csv_data.ID]
    documentation = [item for item in csv_data.Documentation]
    upload_data.Name = names
    upload_data['Name'].str.replace(r"\(.*\)", "")
    upload_data.Type = types
    upload_data.GUID = guids
    upload_data.Documentation = documentation
    current_assets = get_assets()
    print(current_assets.empty)
    if current_assets.empty == False:
        #for index, row in current_assets.iterrows():
            #if row["asset_tag"] in list(upload_data['GUID']):
                #pass
            #else:
                #delete_asset(row["display_id"])
        for index, row in upload_data.iterrows():
            dump = {'cmdb_config_item':
                    {'name': re.sub(r'\([^)]*\)', '', str(row.Name)),
                     'ci_type_id': str(row.Type),
                     'description':str(row.Documentation),
                     'asset_tag':str(row.GUID),
                     'level_field_attributes':
                      {f'file_imported_from_{row.Type}': file
                      }
                    }
                   }
            data = json.dumps(dump)
            headers = {'Content-Type': 'application/json'}
            if current_assets['asset_tag'].str.contains(row["GUID"]).any():
                if str(row["Documentation"]).strip() == current_assets[current_assets['asset_tag']
                        .str.match(row["GUID"])]['description'].values[0] \
                        and row["Type"] == current_assets[current_assets['asset_tag'].str
                        .match(row["GUID"])]['ci_type_id'].values[0] \
                        and re.sub(r'\([^)]*\)', '', str(row.Name)).strip() == \
                        current_assets[current_assets['asset_tag'].str.match(row["GUID"])]['name'].values[0]:
                    pass
                else:
                    item_id = current_assets[current_assets['asset_tag'].str.match(row["GUID"])]['display_id'].values[0]
                    response = requests.put(f"https://{fresh.domain}/cmdb/items/{item_id}.json",
                                            headers=headers, data=data, auth=(fresh.user, fresh.password)
                                            )
                    print(response.content)
            else:
                response = requests.post(f"https://{fresh.domain}/cmdb/items.json",
                                         headers=headers, data=data, auth=(fresh.user, fresh.password)
                                         )
                print(response.content)
    else:
        for index, row in upload_data.iterrows():
            dump = {'cmdb_config_item':
                    {'name': re.sub(r'\([^)]*\)', '', str(row.Name)),
                     'ci_type_id': str(row.Type),
                     'description':str(row.Documentation),
                     'asset_tag':str(row.GUID),
                     'level_field_attributes':
                      {f'file_imported_from_{row.Type}': file
                      }
                    }
                   }
            data = json.dumps(dump)
            headers = {'Content-Type': 'application/json'}
            response = requests.post(f"https://{fresh.domain}/cmdb/items.json",
                                     headers=headers, data=data, auth=(fresh.user, fresh.password)
                                     )
            print(response.content)

def add_rela(rela_data="relations.csv", asset_data="elements.csv", filetype=""):
    """Add relationships to assets in freshservice CMDB. Assets must exist in the CMDB
    for a relationship to be created.
    Parameters
    ----------
    rela_data: str, optional 
        Specifies the exported Archi file that contains the relationships to be created.
        Default value is "relations.csv"
    asset_data: str, optional
        Specifies the exported Archi file that contains assets/CIs that were uploaded to 
        Freshservice. Default value is "elements.csv".
   """
    if filetype == "csv":
        rela_data = pd.read_csv(rela_data)
        asset_data = pd.read_csv(asset_data)
    if filetype == "xlsx":
        rela_data = pd.read_excel(rela_data, usecols = "B:G")
        asset_data = pd.read_excel(asset_data, usecols = "B:E")
    new_data = pd.DataFrame(columns=["Name", "Type", "GUID"])
    new_data.Name = [re.sub(r'\([^)]*\)', '', str(item)) for item in asset_data.Name]
    new_data.Type = [ASSET_DICT[item] for item in asset_data.Type]
    guids = [item for item in asset_data.ID]
    new_data.GUID = guids
    babysourcedata = rela_data.loc[rela_data["Source"].isin(guids)
                                   & rela_data["Target"].isin(guids)]
    final_source_guid = [row.Source for index, row in babysourcedata.iterrows()]
    final_target_guid = [row.Target for index, row in babysourcedata.iterrows()]
    final_source_name = [re.sub(r'\([^)]*\)', '', str(asset_data.loc[asset_data['ID'] == item, 'Name'].item()))
                         for item in final_source_guid]
    final_target_name = [re.sub(r'\([^)]*\)', '', str(asset_data.loc[asset_data['ID'] == item, 'Name'].item()))
                         for item in final_target_guid]
    final_rela_type_id = [RELA_DICT[row.Type] for index, row in babysourcedata.iterrows()]
    asset_table = get_assets()
    asset_table_names = {row["name"]:row["display_id"] for index, row in asset_table.iterrows()}
    for item, second_item, third in zip(final_source_name, final_target_name, final_rela_type_id):
        headers = {'Content-Type': 'application/json'}
        second_item = str(second_item).strip()
        item = str(item).strip()
        dictionary = {"type": "config_items",
                      "type_id": [asset_table_names[second_item]],
                      "relationship_type_id": third,
                      "relationship_type": "forward_relationship"}
        data2 = json.dumps(dictionary)
        try:
            response = requests.post(f"https://{fresh.domain}/cmdb/items/{str(asset_table_names[item])}/associate.json",
                                     headers=headers, data=data2, auth=(fresh.api_key, fresh.password)
                                     )
            get_calls()
            print(response.content)
        except:
            print(f"There was an error uploading a relationship with a source of {item} and a target of {second_item}.")


def delete_asset(file = '', filetype = '', permanant=False, asset_type = "asset"):
    """Delete a specified Asset/CI from the freshservice CMDB 
    Parameters
    ----------
    display_id : str
        Specify the display_id of the asset/CI you would like to delete from
        the CMDB.
    """
#     if filetype == "csv": 
#         csv_data = pd.read_csv(file)
#     if filetype == "xlsx":
#         csv_data = pd.read_excel(file, usecols = "B:E")
        
#     upload_data = pd.DataFrame(columns=["Name", "Type", "GUID", "Documentation"])
#     # cnx = sqlite3.connect('/Users/audreykoziol/Desktop/freshservice/Untitled.sqlite')
#     # csv_data = pd.read_sql_query("SELECT * FROM elements", cnx)
#     # print(csv_data['class'])
#     names = [item for item in csv_data.Name]
#     types = [ASSET_DICT[item] for item in csv_data.Type]
#     guids = [item for item in csv_data.ID]    
        
    headers = {'Content-Type': 'application/json'}
    if asset_type == "asset": 
        ending = ".json"
    if asset_type == "relationship": 
        ending = "/detach_relationship.json"  
    if not permanant:
        testing = requests.delete(f"https://{fresh.domain}/cmdb/items/{str(display_id)}{ending}",
                                  headers=headers, auth=(fresh.api_key, fresh.password)
                                  )
        get_calls()
        return json.loads(testing.content)
    else:
        requests.put(f"https://{fresh.domain}/api/v2/assets/{str(display_id)}/delete_forever",
                               headers=headers, auth=(fresh.api_key, fresh.password)
                               )
        get_calls()
        return f"Asset #{display_id} deleted permanently"


def mass_delete(file="", filetype = ""):
    """Make delete calls for every asset in the specified file

    :param file: ingest list to parse
    :param filetype: format of the file
    :return: None
    """
    if filetype == "csv":
        csv_data = pd.read_csv(file)
    if filetype == "xlsx":
        csv_data = pd.read_excel(file, usecols = "B:E")
    guids = [item for item in csv_data.ID]
    asset_table = get_assets()
    for guid in guids:
        try:
            record_frame = asset_table.loc[asset_table['asset_tag'] == guid]
            display_id = record_frame['display_id'].values[0]
            print('Deleting asset ' + str(display_id) + '//' + guid)
            delete_asset(display_id)
        except IndexError:
            print('Could not find asset ' + guid)


def restore_asset(display_id):
    """Restore a deleted Asset/CI from the freshservice CMDB
    Parameters
    ----------
    display_id : str
        Specify the display_id of the asset/CI you would like to restore in
        the CMDB.
    """
    headers = {'Content-Type': 'application/json'}
    restore = requests.put(f"https://{fresh.domain}/cmdb/items/{str(display_id)}/restore.json",
                           headers=headers, auth=(fresh.api_key, fresh.password)
                           )
    get_calls()
    return json.loads(restore.content)


def filter_assets(query=""):
    """Similar to the function of search_assets() but provides more search fields.
    Parameters
    ---------
    query: str
        query must be composed of a query field (options are asset_type_id, department_id, location_id, asset_state,
        user_id, agent_id, name, asset_tag, created_at, or updated_at) and a query. Queries must be surrounded by
        quotes ("%27 can be used"). Dates are in UTD formatting. An example is "created_at:%272019-05-30%27",
        which returns all assets created on May 30th, 2019.
    """
    asset_table = pd.DataFrame()
    page_num = 1
    pages_left = True
    while pages_left:
        name = requests.get(f'https://{fresh.domain}/api/v2/assets?include=type_fields&query="{query}"&page={page_num}',
                            auth=(fresh.user, fresh.password)
                            )
        content = json.loads(name.content)
        page_num += 1
        asset_data = pd.DataFrame(content["assets"])
        asset_table = asset_table.append(asset_data, ignore_index=True)
        if not content["assets"]:
            pages_left = False
    return asset_table


def search_assets(field_param, query_param):
    """Search items in the freshservice CMDB
    Parameters
    ----------
    field_param: str
        Specifies which field should be used to search for an asset. Allowed parameters
        are 'name', 'asset_tag', or 'serial_number'.
    query_param: str
        What you would like to search for based off of the field_param. For example,
        field_param="name", query_param="andrea".
    """
    headers = {'Content-Type': 'application/json'}
    search = requests.get(f"https://{fresh.domain}/cmdb/items/list.json?field={field_param}&q={query_param}",
                          headers=headers,auth=(fresh.api_key, "1234")
                          )
    get_calls()
    return json.loads(search.content)


def add_dns(dns_csv="Archi_Exports/dns_csv.csv"):
    """Add information to the "dns_names" field of an asset
    
    Parameters
    ----------
    dns_csv: str
        Specifies which file contains your nodes and dns names. Format inside CSV must be
        "node_name", "dns1, dns2, dns3, etc."
    
    """
    imported = pd.read_csv(dns_csv, names=("Name", "DNS"), quotechar='"', skipinitialspace=True)
    headers = {'Content-Type': 'application/json'}
    for index, row in imported.iterrows():
        node_name = re.sub(r'\([^)]*\)', '', row["Name"]).strip()
        name_match = search_assets("name", node_name)
        dump = {'cmdb_config_item':
                 {'name': name_match["config_items"][0]["name"],
                  'ci_type_id': name_match["config_items"][0]['ci_type_id'],
                  'description': name_match["config_items"][0]['description'],
                  'asset_tag': name_match["config_items"][0]['asset_tag'],
                  'level_field_attributes':
                      {'connected_to_provisioning_10001075125': 'Yes',
                       "dns_names_10001075125": str(row["DNS"])
                      }
                 }
               }
        data = json.dumps(dump)
        response = requests.put(f"https://{fresh.domain}/cmdb/items/{name_match['config_items'][0]['display_id']}.json",
                                headers=headers, data=data, auth=(fresh.user, fresh.password)
                                )
        print(response.content)


def clone_artifacts(csv="Archi_Exports/VladArtifacts.csv", era="Archi_Exports/LSST_eras.csv"):
    """Adds or updates assets/CIs in Freshservice CMDB. Assets not present in the upload
    file, but present in the CMDB will be deleted from the CMDB.
    Parameters
    ----------
    csv: str, optional
        Specifies the exported Archi file that contains assets/CIs that should be uploaded.
        Default value is "elements.csv"
    era: str, optional
        Specifies which era parameters file to pull from. Default value is "Archi_Exports/LSST_eras.csv"
    """
    csv_data = pd.read_csv(csv)
    upload_data = pd.DataFrame(columns=["Name", "Type", "GUID", "Documentation"])
    era_data = pd.read_csv(era)
    # cnx = sqlite3.connect('/Users/audreykoziol/Desktop/freshservice/Untitled.sqlite')
    # csv_data = pd.read_sql_query("SELECT * FROM elements", cnx)
    # print(csv_data['class'])
    names = ["*FUTURE* "+item for item in csv_data.Name]
    types = [ASSET_DICT[item] for item in csv_data.Type]
    guids = ["f_"+item for item in csv_data.ID]
    documentation = [item for item in csv_data.Documentation]
    upload_data.Name = names
    upload_data['Name'].str.replace(r"\(.*\)", "")
    upload_data.Type = types
    upload_data.GUID = guids
    upload_data.Documentation = documentation
    current_assets = get_assets()
    if current_assets.empty == False:
        # for index, row in current_assets.iterrows():
            # if row["asset_tag"] in list(upload_data['GUID']):
                # pass
            # else:
                # delete_asset(row["display_id"])
        for index, row in upload_data.iterrows():
            dump = {'cmdb_config_item':
                     {'name': re.sub(r'\([^)]*\)', '', str(row.Name)),
                      'ci_type_id': str(row.Type),
                      'description': str(row.Documentation),
                      'asset_tag': str(row.GUID),
                      'level_field_attribute':
                          {f'bytes_{row.Type}': str(era_data.loc[0]["Fires"]),
                           f'era_2_storage_{row.Type}': str(era_data.loc[1]["Fires"])
                          }
                     }
                   }
            data = json.dumps(dump)
            print(data)
            headers = {'Content-Type': 'application/json'}
            if current_assets['asset_tag'].str.contains(row["GUID"]).any():
                if str(row["Documentation"]).strip() == current_assets[current_assets['asset_tag'].str.match(row["GUID"])]['description'].values[0] and row["Type"] == current_assets[current_assets['asset_tag'].str.match(row["GUID"])]['ci_type_id'].values[0] and re.sub(r'\([^)]*\)', '', str(row.Name)).strip() == current_assets[current_assets['asset_tag'].str.match(row["GUID"])]['name'].values[0]:
                    pass
                else:
                    item_id = current_assets[current_assets['asset_tag'].str.match(row["GUID"])]['display_id'].values[0]
                    response = requests.put(f"https://{fresh.domain}/cmdb/items/{item_id}.json",
                                            headers=headers, data=data, auth=(fresh.user, fresh.password)
                                            )
                    print(response.content)
            else:
                response = requests.post(f"https://{fresh.domain}/cmdb/items.json",
                                         headers=headers, data=data, auth=(fresh.user, fresh.password)
                                         )
                print(response.content)
    else:
        for index, row in upload_data.iterrows():
            dump = {'cmdb_config_item':
                     {'name': re.sub(r'\([^)]*\)', '', str(row.Name)),
                      'ci_type_id': str(row.Type),
                      'description':str(row.Documentation),
                      'asset_tag':str(row.GUID),
                      'level_field_attributes':
                          {f'bytes_{row.Type}': str(era_data.loc[0]["Fires"]),
                           f'era_2_storage_{row.Type}': str(era_data.loc[1]["Fires"])
                          }
                     }
                   }
            data = json.dumps(dump)
            headers = {'Content-Type': 'application/json'}
            response = requests.post(f"https://{fresh.domain}/cmdb/items.json", headers=headers,
                                     data=data, auth=(fresh.user, fresh.password)
                                     )
            print(response.content)
