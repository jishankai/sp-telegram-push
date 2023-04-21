import hmac
import time
import base64
import requests

# Request Host
host = 'https://api.prod.paradigm.trade'

class Paradigm:
    def __init__(self, access_key, secret_key):
        self.access_key = access_key
        self.secret_key = secret_key

    def sign_request(self, method, path, body):
        signing_key = base64.b64decode(self.secret_key)
        timestamp = str(int(time.time() * 1000)).encode('utf-8')
        message = b'\n'.join([timestamp, method.upper(), path, body])
        digest = hmac.digest(signing_key, message, 'sha256')
        signature = base64.b64encode(digest)
        return timestamp, signature

    def get_trade_tape(self, path, method, payload):
        timestamp, signature = self.sign_request(
            method=method.encode('utf-8'),
            path=path.encode('utf-8'),
            body=payload.encode('utf-8'),
        )
        headers = {
            'Paradigm-API-Timestamp': timestamp,
            'Paradigm-API-Signature': signature,
            'Authorization': f'Bearer {self.access_key}'
        }
        # Send request
        response = requests.get(
            host+path,
            headers=headers
        )
        return response.json()
