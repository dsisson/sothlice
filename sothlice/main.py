import os
import sys
import logging
from atlassian import Jira

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

       :return key: str, JIra API token
    """
    key = os.getenv('JIRA_API_TOKEN')
    return key


def main():
    # start logging
    logger = setup_logging('sothlice', 'sothlice.log')
    logger.info('Starting ')

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

    # projects = jira.projects(included_archived=None, expand=None)
    # logger.info(f"projects: \n{plog(projects)}")

    # get the tickets
    jql_request = 'project = "Sample Scrum Project" ' \
                  'AND status NOT IN (Closed, Resolved) ' \
                  'ORDER BY issuekey'
    issues = jira.jql(jql_request)
    logger.info(f"issues: \n{plog(issues)}")


if __name__ == '__main__':
    main()
