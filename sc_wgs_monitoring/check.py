import re


def check_dnanexus_id(file):
    return (
        True if re.search(r"file|project|job-[0-9a-zA-Z]{24}", file) else False
    )
