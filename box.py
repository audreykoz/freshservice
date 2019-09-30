from boxsdk import JWTAuth, Client # need pyjwt and cryptography
import boxsdk as b


def box_upload_elements(file):
    # uploads the file to the elements archive
    box_uploader(88582643157, file)


def box_upload_relations(file):
    # uploads the file to the relationships archive
    box_uploader(88582357248, file)


def box_uploader(folder_id, file):
    # this is the function that does the actual uploading
    auth = JWTAuth.from_settings_file('key.json')
    cli = Client(auth)
    f = cli.folder(folder_id=folder_id)
    try:
        uploaded_file = f.upload(file)
    except b.exception.BoxAPIException as ex:
        # update the file instead
        conflict = ex.context_info['conflicts']['id']
        uploaded_file = cli.file(conflict).update_contents(file, etag=None, preflight_check=False)
        print('File already exists.')
    print(file + ' can be acessed with this link: ' + uploaded_file.get_shared_link(access='company'))


def share_folders(client):
    # run this to retrieve URLs for the archive folders
    url = client.folder(folder_id='88582643157').get_shared_link(access='company')
    print('The elements folder can be acessed at {0}'.format(url))
    url = client.folder(folder_id='88582357248').get_shared_link(access='company')
    print('The relationships folder can be acessed at {0}'.format(url))


if __name__ == '__main__':
    sdk = JWTAuth.from_settings_file('key.json')
    client = Client(sdk)
    share_folders(client)