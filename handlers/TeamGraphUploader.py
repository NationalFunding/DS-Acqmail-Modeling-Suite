import requests
import os
import re



class TeamsGraphUploader:
    def __init__(self, client_id, client_secret, tenant_id, team_id, channel_id, logger, session):
        self.client_id = client_id
        self.client_secret = client_secret
        self.tenant_id = tenant_id
        self.team_id = team_id
        self.channel_id = channel_id
        self.log = logger
        self.token = self._authenticate()
        self.session = session

    def _authenticate(self):
        url = f'https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token'
        data = {
            'client_id': self.client_id,
            'scope': 'https://graph.microsoft.com/.default',
            'client_secret': self.client_secret,
            'grant_type': 'client_credentials'
        }
        response = requests.post(url, data=data)
        response.raise_for_status()
        token = response.json()['access_token']
        return token
    
    @staticmethod
    def sanitize_filename(name):
        return re.sub(r'[^A-Za-z0-9._-]', '_', name)


    def upload_file(self, file_path, upload_name=None):
        #upload_name = upload_name or os.path.basename(file_path)
        upload_name = self.sanitize_filename(upload_name)
        headers = {'Authorization': f'Bearer {self.token}'}


        # Get the SharePoint folder ID for the Teams channel
        folder_resp = requests.get(
            f"https://graph.microsoft.com/v1.0/teams/{self.team_id}/channels/{self.channel_id}/filesFolder",
            headers=headers
        )
        
        
        folder_resp.raise_for_status()
        folder_info = folder_resp.json()
        drive_id = folder_info['parentReference']['driveId']
        folder_id = folder_info['id']

        # Upload file
        upload_url = f"https://graph.microsoft.com/v1.0/drives/{drive_id}/items/{folder_id}:/{upload_name}:/content"
        with open(file_path, 'rb') as f:
            self.log.info(f"Uploading file '{upload_name}' to Teams channel...")
            upload_resp = requests.put(upload_url, headers=headers, data=f)
            self.log.info(f"Upload response: {upload_resp.status_code} - {upload_resp.text}")
            upload_resp.raise_for_status()

        self.log.info(f"File '{upload_name}' uploaded successfully.")
        upload_result = upload_resp.json()
        file_url = upload_result.get("webUrl")
        if file_url:
            self.session.teams.send_debug_message(
                 f"New file uploaded: {upload_name}\n{file_url}")
        else:
            self.log.warning(f"No file URL returned for '{upload_name}', skipping debug message.")
        return upload_result

    
