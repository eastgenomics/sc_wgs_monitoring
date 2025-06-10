import logging

from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

logger = logging.getLogger("basic")


def slack_notify(message, channel, slack_token) -> None:
    """Notify the channel with the given message

    Args:
        message (str): Message to send
        channel (str): Channel to send the message to
    """

    logger.info(f"Sending message to {channel}")

    http = Session()
    retries = Retry(total=5, backoff_factor=10, allowed_methods=["POST"])
    http.mount("https://", HTTPAdapter(max_retries=retries))

    try:
        response = http.post(
            "https://slack.com/api/chat.postMessage",
            {"token": slack_token, "channel": f"#{channel}", "text": message},
        ).json()

        if not response["ok"]:
            # error in sending slack notification
            logger.error(
                f"Error in sending slack notification: {response.get('error')}"
            )

    except Exception as err:
        logger.error(
            f"Error in sending post request for slack notification: {err}"
        )
