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
    parser.add_argument('--mode', type=str, default='update')
    args = parser.parse_args()
    print(f"args: {args}")
    return args.mode


def main():
    # start logging
    logger = setup_logging('sothlice', 'sothlice.log')
    logger.info('Starting ')

    # ----------------- JIRA -----------------
    # connect to Jira
    jira = Jira(
        url='https://philosophe.atlassian.net',
        username='philosophe@gmail.com',
        password=get_token(),
        cloud=True)

    logger.info(f"Jira response: {jira}")
    logger.info(f"dir.api_version: {jira.api_version}")
    #logger.info(f"dir(jira): \n{plog(dir(jira))}")
    logger.info(f"jira.__dict__: \n{plog(jira.__dict__)}")

    # get the tickets
    jql_query = 'project = "Sample Scrum Project" ' \
                  'AND status NOT IN (Closed, Resolved) ' \
                  'ORDER BY issuekey'
    issues = jira.jql(jql_query)
    # logger.info(f"issues: \n{plog(issues)}")

    # ----------------- massage data -----------------
    # create the data container for the extracted ticket information
    ticket_data = []

    # extract the desired ticket information
    for issue in issues['issues']:
        ticket = {
            'key': issue['key'],
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

    # ----------------- db -----------------
    # identify the database mode
    mode = get_db_mode()

    # connect to the database
    connection = psycopg2.connect(
        host='localhost',
        database='qdata',
        user='derek')

    # Open a cursor to perform database operations
    cursor = connection.cursor()
    logger.info(f"\ncursor: {cursor}")

    # get existing IDs because are updating NEW tickets
    query = "SELECT jkey FROM jira;"
    cursor.execute(query)
    connection.commit()
    raw_existing_ids = cursor.fetchall()

    # compare the ticket IDs from the DB with the Jira ticket IDs
    existing_ids = [id[0] for id in raw_existing_ids]
    logger.info(f"\nexisting_ids: {existing_ids}")
    jira_ids = [t['key'] for t in ticket_data]

    # get the ids to delete from the Jira data
    delete_ids = [id for id in jira_ids if id in existing_ids]

    # update our Jira tickets data model to be only NEW tickets (wrt the db)
    new_jira_tickets = [t for t in ticket_data if t['key'] not in delete_ids]

    # ##################################################
    # # take specific db actions based on the CLI args #
    # ##################################################
    if mode == 'update':

        cursor.execute("SELECT * from jira;")
        connection.commit()
        logger.info(f"\ncursor.query: {cursor.query}")
        logger.info(f"\ncursor.fetchall(): {cursor.fetchall()}")

        logger.info(f"\n---> will insert {len(new_jira_tickets)} tickets")
        # insert the data
        for ticket in new_jira_tickets:
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
            try:
                cursor.execute(sql, values)
                connection.commit()

                logger.info(f"\ncursor.query: {cursor.query}")
                logger.info(f"\ncursor.description: {cursor.description}")
                logger.info(f"\ncursor.statusmessage: {cursor.statusmessage}")
                logger.info(f"\ncursor.rowcount: {cursor.rowcount}")
                res = cursor.fetchone()
                logger.info(f"\nthis res: {res}")
            except psycopg2.errors.UniqueViolation as e:
                logger.error(f"\nerror: {e}")


        cursor.execute("SELECT * from jira;")
        result = cursor.fetchall()
        for i, row in enumerate(result):
            logger.info(f"row {i}: {row}")


    # clean up
    cursor.close()
    connection.close()



if __name__ == '__main__':
    main()
