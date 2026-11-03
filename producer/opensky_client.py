# Gets aircraft data from the OpenSky API
import logging
import time

import requests

from config import BBOX, STATES_URL, TOKEN_URL

log = logging.getLogger(__name__)


class OpenSkyClient:
    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token = None
        self.token_expiry = 0.0
        self.session = requests.Session()

    def _get_token(self):
        if not (self.client_id and self.client_secret):
            return None
        if self.token and time.time() < self.token_expiry - 60:
            return self.token
        r = self.session.post(TOKEN_URL, data={
            "grant_type": "client_credentials",
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }, timeout=20)
        r.raise_for_status()
        body = r.json()
        self.token = body["access_token"]
        self.token_expiry = time.time() + body.get("expires_in", 1800)
        log.info("Got new OpenSky token (valid %ss)", body.get("expires_in"))
        return self.token

    def fetch_states(self, bbox=BBOX):
        headers = {}
        token = self._get_token()
        if token:
            headers["Authorization"] = f"Bearer {token}"
        r = self.session.get(STATES_URL, params=bbox, headers=headers, timeout=30)
        if r.status_code == 429:
            wait = int(r.headers.get("X-Rate-Limit-Retry-After-Seconds", "600"))
            log.warning("Rate limited (429). Waiting %s s", wait)
            return None, wait
        if r.status_code == 401:
            self.token = None
            log.warning("401 Unauthorized, token will be refreshed")
            return None, 5
        r.raise_for_status()
        remaining = r.headers.get("X-Rate-Limit-Remaining")
        if remaining is not None:
            log.info("API credits remaining: %s", remaining)
        return r.json(), 0
