import argparse
import os
import logging
import logging.config

logging.config.fileConfig('logging.ini')

import sys

file_dir = os.path.dirname(__file__)
sys.path.append(file_dir)

from API import initApi

from dotenv import load_dotenv
from os.path import join, dirname

dotenv_path = join(dirname(__file__), '.env')
load_dotenv(dotenv_path=dotenv_path)

from lab.DocLoader import FileLoader

logger = logging.getLogger("PDWHLoader")
logger.info('Starting...')


list_of_choices = [
    'pdwh',
    'fileloader'
]

parser = argparse.ArgumentParser(description='PraxisDataWareHouse [PDWH] Service')

parser.add_argument(
    '-r',
    '--routines',
    required=True,
    nargs='+',
    choices=list_of_choices,
    metavar='R',
    help='List of routines to run: {}'.format(', '.join(list_of_choices))
)


parser.add_argument("-d", "--directories", nargs='+',
                    help="directories to be user for loader, works with --routines=fileloader", metavar="STRINGS")

def main(args=sys.argv[1:]):
    args = parser.parse_args(args)

    if 'pdwh' in args.routines:

        logger.info("WEB service started")
        app = initApi()
        app.run(debug=True,host=os.environ.get("API_HOST_IP"), port=os.environ.get("API_HOST_PORT"))


    elif 'fileloader' in args.routines:
        logger.info("FILELOADER service started")

        directories = args.directories

        if len(directories) > 0:

            for d in directories:
                logger.info("The following directory should be used {}".format(d))

                labFileLoader = FileLoader(dir=d)
                labFileLoader.post_files()

        else:
            logger.error("Please provide at least one directory to be monitored (-d/--directories)")

    else:
        logger.info("Please select valid routine. Type -r for help.")

main(sys.argv[1:])
