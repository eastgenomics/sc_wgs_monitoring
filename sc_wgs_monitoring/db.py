import datetime
from typing import Tuple, List

from sqlalchemy import create_engine, select, insert, update
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


def update_in_db(
    session: Session, table: Table, referral_id: str, update_data: dict
):
    """Update the database where the referral id is equal to the one given
    with the given update data

    Parameters
    ----------
    session : Session
        Session SQLAlchemy object
    table : Table
        Table SQLAlchemy object
    referral_id : str
        Referral id to get the appropriate row
    update_data : dict
        Dict containing the data used for updating the row
    """

    update_obj = (
        update(table)
        .where(table.c.referral_id == referral_id)
        .values(**update_data)
    )
    session.execute(update_obj)
    session.commit()


def get_samples_for_the_day(
    session: Session, table: Table, datetime_object: datetime.datetime
) -> dict:
    """Get the samples that ran for the day

    Parameters
    ----------
    session : SQLAlchemy session object
        Session object for the connected database
    table : SQLAlchemy Table object
        Table object which contains the data of interest
    datetime_object : datetime.datetime
        Datetime object which will be used to compare to the dates stored in
        the database

    Returns
    -------
    dict
        Dict containing the result of the query based on the job status of each
        row of result
    """

    res = session.execute(
        select(table).filter(table.c.date >= datetime_object)
    )

    data = {}

    if res.rowcount != 0:
        for row in res.mappings().all():
            data.setdefault(row["job_status"], []).append(row)

    return data


def remove_processed_samples(
    session: Session, table: Table, files: dict
) -> dict:
    """Remove processed samples from detected sample dict

    Parameters
    ----------
    session : Session
        Session SQLAlchemy object
    table : Table
        Table SQLAlchemy object for the table to check for sample presence
    files : dict
        Dict containing the files and the sample id

    Returns
    -------
    dict
        Dict containing the same information but without samples that were
        removed
    """

    files_without_processed_samples = {}

    # query the database to find samples that have already been
    # processed
    for sample_id in files:
        processed_sample = look_for_processed_samples(
            session, table, sample_id
        )

        if processed_sample is None:
            files_without_processed_samples[sample_id] = files[sample_id]
        else:
            print(f"Removed {sample_id} as it has already been processed")

    # all samples were removed
    if not files_without_processed_samples:
        print("All files detected have already been processed. Exiting...")
        exit()
    else:
        return files_without_processed_samples


def prepare_data_for_import(table, **kwargs):
    """Prepare data for import by generating empty inputs for fields not used
    and adding given key value pairs for the rest

    Parameters
    ----------
    table : Table
        Table in which the data will be imported in

    Returns
    -------
    dict
        Dict containing given data and empty values for non used fields
    """

    sample_data = {
        column.name: "" for column in table.columns if column.name != "id"
    }

    for key, value in kwargs.items():
        sample_data[key] = value

    return sample_data
