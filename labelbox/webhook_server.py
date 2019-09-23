from enum import Enum, auto
import logging
from uuid import uuid4

from flask import Flask
import requests

from labelbox import Webhook
from labelbox.exceptions import LabelboxError


logger = logging.getLogger(__name__)


def register(project, topic, url, secret=None):
    if secret is None:
        # TODO generate secrete
        pass


class WebhookServer:

    class CallbackWrapper:

        def __init__(self, callback, secret):
            self.callback = callback
            self.secret = secret

        def __call__(self, *args, **kwargs):
            # TODO check secret
            print("Webhook callback", args, kwargs)
            try:
                self.callback(*args, **kwargs)
            except Exception as e:
                logger.exception("Failed to invoke callback %r with args %r and "
                                 "kwargs %r, exception occurred: %r",
                                 self.callback, args, kwargs, e)

            return "Cool"

    def __init__(self, client, public_address=None):
        self.app = Flask(__name__)
        self.port = None
        self.client = client

        self.public_address = public_address

    def run(self, host="0.0.0.0", port=5000):
        self.port = port

        if self.public_address == None:
            ip = requests.get('https://api.ipify.org').text
            self.public_address = "%s:%d" % (ip, port)

        logger.debug("Webhook server starting with public addres: %s",
                     self.public_address)
        self.app.run(host, port)

    def create_and_register(self, topics, callback, secret=None, project=None):
        if self.public_address is None:
            raise LabelboxError("Can't create a Webhook before the WebhookServer"
                                "is started, or a public address supplied.")
        if secret is None:
            secret = str(uuid4())

        # register the callback
        endpoint = str(uuid4())
        self.app.add_url_rule("/%s" % endpoint, endpoint,
                              WebhookServer.CallbackWrapper(callback, secret),
                              methods=["GET", "POST"])

        # create a webhook
        url = "http://" + self.public_address + "/" + endpoint
        return Webhook.create(self.client, topics, url, secret, project)
