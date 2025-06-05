from typing import Tuple, List

from sqlalchemy import create_engine, select, insert
from sqlalchemy.schema import Table
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.sql.schema import MetaData


def connect_to_db(
    user: str, pwd: str, db_name: str
) -> Tuple[Session, MetaData]:
    """Connect to a postgres db using the given endpoint and credentials

    Parameters
    ----------
    user : str
        Username to connect with
    pwd : str
        Password for the username
    db_name : str
        Endpoint for the database to connect to

    Returns
    -------
    List[Session, MetaData]
        Session and metadata objects
    """

    # Create SQLAlchemy engine to connect to AWS database
    url = f"postgresql+psycopg://{user}:{pwd}@db/{db_name}"

    engine = create_engine(url)

    meta = MetaData()
    meta.reflect(bind=engine)
    Session = sessionmaker(bind=engine)
    session = Session()

    return session, meta


def look_for_processed_samples(
    session: Session, table: Table, sample_id: str
) -> str:
    """Query the database for a given sample id

    Parameters
    ----------
    session : Session
        Session SQLAlchemy object
    table : Table
        Table SQLAlchemy object
    sample_id : str
        String for the sample id to query with

    Returns
    -------
    str
        String with the result of the query
    """

    res = session.execute(
        select(table.c.referral_id).filter_by(referral_id=sample_id)
    )

    if res.one_or_none():
        return sample_id
    else:
        return None


def insert_in_db(session: Session, table: Table, data: List):
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
