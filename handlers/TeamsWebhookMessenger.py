import requests
import os
class TeamsWebhookMessenger:
    def __init__(self, webhook_url, logger):
        self.webhook_url = webhook_url
        self.log = logger

    def send_message(self, text):
        payload = {"text": text}
        try:
            self.log.info(f"Sending message to Teams: {text}")
            resp = requests.post(self.webhook_url, json=payload)
            resp.raise_for_status()
        except requests.exceptions.RequestException as e:
            self.log.error(f"Failed to send message via Teams webhook: {e}")
