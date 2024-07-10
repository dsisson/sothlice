import os
import argparse
import logging
from atlassian import Jira
import psycopg2
from datetime import datetime
from zoneinfo import ZoneInfo

from utils import plog


def setup_logging(name, log_file, level=logging.DEBUG):
    # create logger
    logger = logging.getLogger(name)
    logger.setLevel(level)

    # create file handler
    fh = logging.FileHandler(log_file)
    fh.setLevel(level)

    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)

    # add file handler to logger
    logger.addHandler(fh)

    return logger


def get_token():
    """
        Get the Jira API token from the local ENV. This means that
        the token should be listed in the .bash_profile file with
        the key `JIRA_API_TOKEN`.

       :return key: str, Jira API token
    """
    key = os.getenv('JIRA_API_TOKEN')
    return key


def get_db_mode():
    """
        Get the database mode from arguments passed through the CLI.

        Options for `--mode` are:
            insert -- take any tickets pulled from JIra that are not in the db,
                      and add them to the db
            update -- take any tickets pulled from Jira that are either not in
                      the db or that have been updated in Jira, and update the db
        :return: str, database mode
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', type=str, default='insert')
    args = parser.parse_args()
    print(f"args: {args}")
    return args.mode


def get_tickets_from_jira(jira, logger):
    """
        Get all the non-completed tickets from Jira using the Jira API.

       :param jira: Jira, the Jira API object
       :param logger: logging.Logger, the logger
       :return issues: dict, the Jira tickets
    """
    jql_query = 'project = "Sample Scrum Project" ' \
                  'AND status NOT IN (Closed, Resolved) ' \
                  'ORDER BY issuekey'

    # get the tickets
    issues = jira.jql(jql_query)
    # logger.info(f"issues: \n{plog(issues)}")
    return issues


def process_tickets(issues, logger):
    """
        Process the Jira tickets to extract the desired fields
        and format or enrich the field values.

        Note: if a field is added to `ticket` or renamed, the db schema
        has to be rebuilt AND the insert query has to be updated.

       :param issues: dict, the Jira tickets
       :param logger: logging.Logger, the logger
       :return ticket_data: list, the extracted ticket information
    """
    # create data container for the extracted ticket information
    ticket_data = []
    # create container for just the ticket IDs
    ticket_ids = []

    # extract the desired ticket information
    for issue in issues['issues']:
        id = issue['key']
        ticket_ids.append(id)

        # build the data (with special handling as needed)
        ticket = {}

        ticket['key'] = id
        ticket['type'] = issue['fields']['issuetype']['name']
        ticket['status'] = issue['fields']['status']['name']
        ticket['summary'] = issue['fields']['summary']
        ticket['description'] = issue['fields']['description']

        # these are string values; convert to datetime objects that are
        # timezone aware (using "America/New_York" because that is how the
        # Jira demo account is configured)
        tz = ZoneInfo('America/New_York')
        created = datetime.fromisoformat(issue['fields']['created'])
        ticket['created'] = created.replace(tzinfo=tz)

        updated = datetime.fromisoformat(issue['fields']['updated'])
        ticket['updated'] = updated.replace(tzinfo=tz)

        # assignee can be null
        assignee = issue['fields'].get('assignee')
        ticket['assignee'] = issue['fields']['assignee']['displayName'] \
            if assignee else assignee

        # get and set a timestamp for records that are changed by this code
        processed = datetime.now(tz)
        ticket['processed'] = processed.replace(tzinfo=tz)
        logger.info(f"\nprocessed: {ticket['processed']}")

        # add this reduced ticket to the data container
        ticket_data.append(ticket)

    logger.info(f"ticket_data: \n{plog(ticket_data)}")
    return ticket_data, ticket_ids


def get_ids_and_updateds_from_db(connection,logger):
    """
        Get the ticket IDs and updated timestamps from the database

        :param connection: psycopg2.connection, the database connection
        :param logger: logging.Logger, the logger
        :return ticket_ids: list, the ticket IDs
    """
    cursor = connection.cursor()
    query = "SELECT jkey, updated FROM jira;"
    cursor.execute(query)
    raw_data = cursor.fetchall()
    cursor.close()

    # compare the ticket IDs from the DB with the Jira ticket IDs
    ids_and_updateds = {item[0]:item[1] for item in raw_data}
    logger.info(f"\nids_and_updateds: {ids_and_updateds}")

    return ids_and_updateds


def create_insert_query(ticket, logger):
    """
        Create the insert query for the database.

       :param ticket: dict, the ticket data
       :return query: str, the insert query
    """
    logger.info(f"\nthis ticket: \n{ticket}")

    # simplify the insert
    jkey = ticket['key']
    jtype = ticket['type']
    status = ticket['status']
    summary = ticket['summary']
    description = ticket['description']
    created = ticket['created']
    updated = ticket['updated']
    assignee = ticket['assignee']
    processed = ticket['processed']

    values = (jkey, jtype, status, summary,
              description, created, updated, assignee, processed)
    sql = "INSERT INTO jira (jkey, jtype, status, summary, description, created, updated, assignee, processed) " \
          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s) RETURNING jkey;"
    return sql, values

def create_update_query(ticket, logger):
    """
        Create the update query for the database.

       :param ticket: dict, the ticket data
       :return query: str, the insert query
    """
    logger.info(f"\nthis ticket: \n{ticket}")

    # simplify the update; only fields we are likely to update
    jkey = ticket['key']
    jtype = ticket['type']
    status = ticket['status']
    summary = ticket['summary']
    description = ticket['description']
    #created = ticket['created']
    updated = ticket['updated']
    assignee = ticket['assignee']
    processed = ticket['processed']

    values = (jtype, status, summary,
              description, updated, assignee, processed, jkey)
    sql = "Update jira SET jtype = %s, status = %s, summary = %s, description = %s, " \
          "updated = %s, assignee = %s, processed = %s " \
          "WHERE jkey = %s RETURNING jkey;"
    return sql, values


def main():
    # start logging
    logger = setup_logging('get_data', 'get_data.log')
    logger.info('Starting ')

    # ----------------- JIRA -----------------
    # Step 1: connect to Jira
    jira = Jira(
        url='https://philosophe.atlassian.net',
        username='philosophe@gmail.com',
        password=get_token(),
        cloud=True)

    logger.info(f"dir.api_version: {jira.api_version}")
    # logger.info(f"jira.__dict__: \n{plog(jira.__dict__)}")

    # Step 2: get the tickets from Jira
    raw_issues = get_tickets_from_jira(jira, logger)

    # ----------------- massage the tickets data -----------------
    # Step 3: extract and enrich the tickets data
    processed_jira_tickets, jira_ids = process_tickets(raw_issues, logger)

    # ----------------- db -----------------
    # Step 4: identify the database mode (because we may do different things)
    mode = get_db_mode()

    # Step 5: connect to the postgres database
    connection = psycopg2.connect(
        host='localhost',
        database='qdata',
        user='derek')

    # ##################################################
    # # take specific db actions based on the CLI args #
    # ##################################################
    if mode == 'insert':
        # ##################################################
        # # insert records that do not exist in DB        #
        # ##################################################

        # get the ids that are in the Jira data AND the db
        db_tickets = get_ids_and_updateds_from_db(connection, logger)
        # db_ids = [t[0] for t in db_tickets]
        db_ids = list(db_tickets.keys())
        # get the ids to delete from the Jira data
        delete_ids = [id for id in jira_ids if id in db_ids]

        # update our Jira tickets data model to be only NEW tickets (wrt the db)
        new_jira_tickets = [t for t in processed_jira_tickets
                            if t['key'] not in delete_ids]

        logger.info(f"\n---> will insert {len(new_jira_tickets)} tickets")
        # insert the data
        for ticket in new_jira_tickets[:6]:

            # assemble the query and the values to insert
            sql, values = create_insert_query(ticket, logger)
            try:
                cursor = connection.cursor()
                # for each record to be inserted, insert it
                cursor.execute(sql, values)
                connection.commit()

                logger.info(f"\ncursor.statusmessage: {cursor.statusmessage}")
                logger.info(f"\ncursor.rowcount: {cursor.rowcount}")
                res = cursor.fetchone()
                logger.info(f"\nthis res: {res}")
                cursor.close()
            except psycopg2.errors.UniqueViolation as e:
                logger.error(f"\nerror: {e}")

    elif mode == 'update':
        # ##################################################
        # # insert new records AND updated records         #
        # ##################################################

        # get the ids and Jira `updated` timestamps from the db
        db_tickets = get_ids_and_updateds_from_db(connection, logger)

        for ticket in processed_jira_tickets:
            logger.info(f"\nchecking ticket: {ticket['key']}"
                        f"\n--> `updated` from Jira: {ticket['updated']}"
                        f"\n--> `updated` from db:   {db_tickets.get(ticket['key'])}")

            if ticket['key'] not in db_tickets:
                # if the Jira ticket is NOT in the db, then we want to INSERT it
                # assemble the query and the values to insert
                logger.info(f"\n~~~> inserting ticket: {ticket['key']}")
                sql, values = create_insert_query(ticket, logger)
                try:
                    cursor = connection.cursor()
                    # for each record to be inserted, insert it
                    cursor.execute(sql, values)
                    connection.commit()

                    # logger.info(f"\ncursor.statusmessage: {cursor.statusmessage}")
                    # logger.info(f"\ncursor.rowcount: {cursor.rowcount}")
                    res = cursor.fetchone()
                    logger.info(f"\nthis res: {res}")
                    cursor.close()
                except psycopg2.errors.UniqueViolation as e:
                    logger.error(f"\nerror: {e}")

            elif ticket['key'] in db_tickets:
                # if the ticket is in the db, check whether the `updated` from Jira
                # is greater than the `updated` column in the db
                logger.info(f"\njira updated: {ticket['updated']}\n  db updated: {db_tickets[ticket['key']]}")
                if ticket['updated'] > db_tickets[ticket['key']]:
                    # if the ticket's data from Jira is newer, then we want to UPDATE that row                    row_to_add = ticket
                    logger.info(f"\n~~~> updating ticket: {ticket['key']}")
                    # assemble the query and the values to insert
                    sql, values = create_update_query(ticket, logger)
                    try:
                        cursor = connection.cursor()
                        # for each record to be inserted, insert it
                        # logger.info(f"\nsql: \n{sql}")
                        # bdata = cursor.mogrify(sql, values)
                        # logger.info(bdata.decode('utf-8'))
                        cursor.execute(sql, values)
                        connection.commit()

                        res = cursor.fetchone()
                        logger.info(f"\nthis res: {res}")
                        cursor.close()
                    except psycopg2.errors.UniqueViolation as e:
                        logger.error(f"\nerror: {e}")

            else:
                logger.warning(f"\nnot updating ticket: {ticket['key']}")

    # get the updated data from the db as a check
    cursor = connection.cursor()
    cursor.execute("SELECT * from jira;")
    result = cursor.fetchall()
    for i, row in enumerate(result):
        logger.info(f"row {i}: {row}")

    # clean up
    cursor.close()
    connection.close()


if __name__ == '__main__':
    main()
