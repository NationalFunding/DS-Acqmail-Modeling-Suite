from handlers.TeamsWebhookMessenger import TeamsWebhookMessenger
from handlers.TeamGraphUploader import TeamsGraphUploader


class TeamsHandler:
    def __init__(self, session):
        self.session = session
        self.log = session.log

        creds = session._credentials
        self.messenger = TeamsWebhookMessenger(
            webhook_url=creds.TEAMS_WEBHOOK_URL,
            logger=self.log
        )

        self.uploader = TeamsGraphUploader(
            client_id=creds.TEAMS_CLIENT_ID,
            client_secret=creds.TEAMS_CLIENT_SECRET,
            tenant_id=creds.TEAMS_TENANT_ID,
            team_id=creds.TEAMS_TEAM_ID,
            channel_id=creds.TEAMS_CHANNEL_ID,
            logger=self.log,
            session=self.session
        )

    def send_debug_message(self, text):
        self.messenger.send_message(text)

    def upload_file_to_debug(self, file_path, title=None):
        self.uploader.upload_file(file_path, upload_name=title)

    def upload_log_files(self, file_paths, title="Log File"):
        for file_path in file_paths:
            self.upload_file_to_debug(file_path, title)
