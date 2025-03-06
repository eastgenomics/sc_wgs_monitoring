from typing import Tuple

from sqlalchemy import create_engine, select, insert
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.sql.schema import MetaData


def connect_to_db(
    endpoint: str, port: str, user: str, pwd: str
) -> Tuple[Session, MetaData]:
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


def look_for_processed_samples(session, table, sample_id):
    res = session.execute(select(table).filter_by(referral_id=sample_id))

    if len(res.all()) >= 2:
        raise Exception(
            f"2 or more rows have the same referral id: {sample_id}"
        )
    else:
        return res.all()


def insert_in_db(session, table, data):
    """Insert the data in the database

    Parameters
    ----------
    session : SQLAlchemy session object
        Session object for the connected database
    table : SQLAlchemy Table object
        Table object in which the data will be imported to
    data : list
        List of dict that need to be imported in the database
    """

    insert_obj = insert(table).values(data)
    session.execute(insert_obj)
    session.commit()
