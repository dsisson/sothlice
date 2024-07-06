import os
import argparse
import logging
from atlassian import Jira
import psycopg2

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
        Get the database mode from the local ENV. This means that
        the mode should be listed in the .bash_profile file with
        the key `DB_MODE`.

       :return mode: str, database mode
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
        Process the Jira tickets to extract the desired information.

       :param issues: dict, the Jira tickets
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

        # build the data
        ticket = {
            'key': id,
            'type': issue['fields']['issuetype']['name'],
            'status': issue['fields']['status']['name'],
            'summary': issue['fields']['summary'],
            'description': issue['fields']['description'],
            'created': issue['fields']['created'],
            'updated': issue['fields']['updated'],
        }
        # assignee can be null
        assignee = issue['fields'].get('assignee')
        ticket['assignee'] = issue['fields']['assignee']['displayName'] \
            if assignee else assignee

        # add this reduced ticket to the data container
        ticket_data.append(ticket)

    logger.info(f"ticket_data: \n{plog(ticket_data)}")
    return ticket_data, ticket_ids


def get_ticket_ids_from_db(connection, jira_ids, logger):
    """
        Get the ticket IDs from the database, compare with the ids for
        our Jira tickets (because we only want to handle new ones), and
        return the list of IDs we want to keep.

        NOTE that we are only ADDING rows, and NOT updating rows with changes.
        Also note that we need to convert the db fdata into a form we can use.

        :param connection: psycopg2.connection, the database connection
        :param jira_ids: list, the Jira ticket IDs
        :param logger: logging.Logger, the logger
        :return ticket_ids: list, the ticket IDs
    """
    cursor = connection.cursor()
    query = "SELECT jkey FROM jira;"
    cursor.execute(query)
    raw_existing_ids = cursor.fetchall()
    cursor.close()

    # compare the ticket IDs from the DB with the Jira ticket IDs
    existing_ids = [id[0] for id in raw_existing_ids]
    logger.info(f"\nexisting_ids: {existing_ids}")

    # get the ids to delete from the Jira data
    delete_ids = [id for id in jira_ids if id in existing_ids]

    return delete_ids


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

    values = (jkey, jtype, status, summary,
              description, created, updated, assignee)
    sql = "INSERT INTO jira (jkey, jtype, status, summary, description, created, updated, assignee) " \
          "VALUES (%s, %s, %s, %s, %s, %s, %s, %s) RETURNING jkey;"
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
    updated_tickets, jira_ids = process_tickets(raw_issues, logger)

    # ----------------- db -----------------
    # Step 4: identify the database mode (because we may do different things)
    mode = get_db_mode()

    # Step 5: connect to the postgres database
    connection = psycopg2.connect(
        host='localhost',
        database='qdata',
        user='derek')

    # Step 6: get the ids that are in the Jira data AND the db
    delete_ids = get_ticket_ids_from_db(connection, jira_ids, logger)

    # Step 7: update our Jira tickets data model to be only NEW tickets (wrt the db)
    new_jira_tickets = [t for t in updated_tickets if t['key'] not in delete_ids]

    # ##################################################
    # # take specific db actions based on the CLI args #
    # ##################################################
    if mode == 'insert':
        # ##################################################
        # # insert records that do not exists in DB        #
        # ##################################################

        logger.info(f"\n---> will insert {len(new_jira_tickets)} tickets")
        # insert the data
        for ticket in new_jira_tickets:

            # Step 8a: assemble the query and the values to insert
            sql, values = create_insert_query(ticket, logger)
            try:
                cursor = connection.cursor()
                # Step 8b: for each record to be inserted, insert it
                cursor.execute(sql, values)
                connection.commit()

                logger.info(f"\ncursor.statusmessage: {cursor.statusmessage}")
                logger.info(f"\ncursor.rowcount: {cursor.rowcount}")
                res = cursor.fetchone()
                logger.info(f"\nthis res: {res}")
                cursor.close()
            except psycopg2.errors.UniqueViolation as e:
                logger.error(f"\nerror: {e}")

    # Step 9: get the updated data from the db as a check
    cursor = connection.cursor()
    cursor.execute("SELECT * from jira;")
    result = cursor.fetchall()
    for i, row in enumerate(result):
        logger.info(f"row {i}: {row}")

    # Step 10: clean up
    cursor.close()
    connection.close()


if __name__ == '__main__':
    main()
