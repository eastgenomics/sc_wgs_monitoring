import json
from typing import Dict, List
import re

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.sql.schema import MetaData


def load_config() -> Dict:
    """Load the configuration file

    Returns
    -------
    Dict
        Dict containing the keys for the various parameters used
    """

    with open("config.json") as f:
        config_data = json.loads(f.read())

    return config_data


def connect_to_db(
    endpoint: str, port: str, user: str, pwd: str
) -> List[Session, MetaData]:
    """Connect to a postgres db using the given endpoint and credentials

    Parameters
    ----------
    endpoint : str
        Endpoint for the database to connect to
    port : str
        Port to use
    user : str
        Username to connect with
    pwd : str
        Password for the username

    Returns
    -------
    List[Session, MetaData]
        Session and metadata objects
    """

    # Create SQLAlchemy engine to connect to AWS database
    url = "postgresql+psycopg2://" f"{user}:{pwd}@{endpoint}:{port}/ngtd"

    engine = create_engine(url)

    meta = MetaData(schema="testdirectory")
    meta.reflect(bind=engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    return session, meta


def get_sample_id_from_files(files: list) -> Dict:
    """Get the sample id from the new files detected and link sample ids to
    their files

    Parameters
    ----------
    files : list
        List of DNAnexus file objects

    Returns
    -------
    Dict
        Dict containing the sample id and their files
    """

    patterns = [
        r"[-_]reported_structural_variants\..*\.csv",
        r"[-_]reported_variants\..*\.csv",
        r"\..*\.supplementary\.html",
    ]

    detected_sample_ids = set()

    for file in files:
        file_name = file.name
        # we have some .html files that we don't want to match
        matched_pattern = None

        for pattern in patterns:
            match = re.search(pattern, file_name)

            if match:
                matched_pattern = match

        if matched_pattern:
            detected_sample_ids.add(file_name[: matched_pattern.start()])

    file_dict = {}

    for sample_id in detected_sample_ids:
        file_dict.setdefault(sample_id, [])

        for file in files:
            if sample_id in file.name:
                file_dict[sample_id].append(file)

    assert all(file_dict)

    return file_dict


def write_confluence_csv(date: str, data: Dict) -> str:
    """Write conflence csv

    Parameters
    ----------
    date : str
        Date string
    data : Dict
        Dict containing the data that needs to be imported in the Confluence db

    Returns
    -------
    str
        File name of the CSV
    """

    file_name = f"{date}.csv"
    data = pd.DataFrame(data)
    data.to_csv(file_name, index=False)
    return file_name
