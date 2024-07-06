import logging
import psycopg2

from utils import plog, write_to_csv_file


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


def main():
    # start logging
    logger = setup_logging('generate_reports', 'generate_reports.log')
    logger.info('Starting ')

    # Step 1: connect to the postgres database
    connection = psycopg2.connect(
        host='localhost',
        database='qdata',
        user='derek')

    # Step 2: get all the rows from the jira table
    cursor = connection.cursor()
    cursor.execute("SELECT * from jira;")
    result = cursor.fetchall()

    # Step 3: get the column names
    desc = cursor.description
    # logger.info(f"\n--> {desc}")
    headers = [c.name for c in desc]
    logger.info(f"\nheaders: {headers}")

    # Step 4: insert the headings into the result
    result.insert(0, headers)
    # logger.info(f"\nresult: {result}")

    for i, row in enumerate(result):
        logger.info(f"row {i}: {row}")

    # Step 5: write the result to a csv file
    write_to_csv_file(result, 'db_all.csv')


if __name__ == '__main__':
    main()
