import requests

class TeleInformer:

    def __init__(self, chat_id, token):
        self.baseurl = "https://api.telegram.org/bot"
        self.chat_id = chat_id
        self.token = token

    def send_mess(self, text):
        params = {'chat_id': self.chat_id, 'text': text}
        response = requests.post(self.baseurl + self.token + "/" + 'sendMessage', data=params)
        return response


