import logging
from threading import Thread
import time

from labelbox import Webhook, WebhookServer


logger = logging.getLogger(__name__)


def test_webhook_create_update(project, rand_gen):
    client = project.client
    url = "https:/" + rand_gen(str)
    secret = rand_gen(str)
    topics = [Webhook.LABEL_CREATED, Webhook.LABEL_DELETED]
    webhook = Webhook.create(client, topics, url, secret, project)

    assert webhook.project() == project
    assert webhook.organization() == client.get_organization()
    assert webhook.url == url
    assert webhook.topics == topics
    assert webhook.status == Webhook.ACTIVE
    assert list(project.webhooks()) == [webhook]
    assert webhook in set(client.get_organization().webhooks())

    webhook.update(status=Webhook.REVOKED, topics=[Webhook.LABEL_UPDATED])
    assert webhook.topics == [Webhook.LABEL_UPDATED]
    assert webhook.status == Webhook.REVOKED

    project.delete()


def test_webhook_server(label_pack):
    project, _, data_row, label = label_pack

    server = WebhookServer(project.client)
    Thread(target=server.run, daemon=True).start()
    time.sleep(3)

    calls = 0

    def callback(*args, **kwargs):
        nonlocal calls
        calls += 1
        print("Callback", args, kwargs)

    webhook_project = server.create_and_register(
        [Webhook.LABEL_CREATED, Webhook.LABEL_UPDATED],
        callback, None, project)
    webhook_org = server.create_and_register(
        [Webhook.LABEL_CREATED, Webhook.LABEL_UPDATED],
        callback, None, None)
    logger.debug("Project webhook: %s", webhook_project)
    logger.debug("Organization webhook: %s", webhook_org)

    label_2 = project.create_label(data_row=data_row, label="test",
                                 seconds_to_label=0.0)

    time.sleep(2)
    notifications_org = list(webhook_org.notifications())
    assert len(notifications_org) == 1
    assert calls == 1

    label_2.update(label="test2")
    time.sleep(3)
    assert calls == 2
