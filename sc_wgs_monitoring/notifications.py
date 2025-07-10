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


def build_report(jobs_dict: dict, date: str) -> str:
    """Build the report message for the daily check

    Parameters
    ----------
    jobs_dict : dict
        Dict containing the job status and the data for the captured jobs
    date : str
        Date for the check

    Returns
    -------
    str
        String representing the report message to send to Slack
    """

    status_dict = {
        "done": ":white_check_mark:",
        "failed": ":warning:",
        "terminated": ":skull:",
    }

    report = f":excel: Solid Cancer WGS workbooks | Jobs for {date}:\n"

    if jobs_dict:
        for job_status, job_data in jobs_dict.items():
            report += f"- {status_dict[job_status]} {job_status}:\n"

            for job in job_data:
                report += f"    - {job['referral_id']} | `{job['job_id']}`\n"
    else:
        report += "\nNo jobs detected."

    return report
