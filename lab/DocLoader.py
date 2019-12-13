import os
import requests
import datetime
import logging

from pathlib import Path

class FileLoader:

    def __init__(self, dir, **kwargs):

        self.logger = logging.getLogger(__name__)
        self.logger.info('PDWH Fileloader initialized')


        self.dir = dir

        self.filenames = []
        self.files = []

        self.load_files()

    def load_files(self):
        for filename in Path(self.dir).rglob('*.ldt'):
            self.logger.info("File found: %s" %filename)
            self.filenames.append(filename)
            self.files.append(open(filename, 'rb'))

    def post_files(self):

        multipart_form_data = []

        for f in self.files:
             multipart_form_data.append(('file', f))

        url = 'http://{apiBase}:{apiPort}{apiEndPoint}'.format(
            apiBase=os.environ.get("API_HOST_IP_TARGET"),
            apiPort=os.environ.get("API_HOST_PORT"),
            apiEndPoint=os.environ.get("API_ENDPOINT_LABFILES")
        )

        self.logger.info("Posting %s " % url)

        response = requests.post(url,files=multipart_form_data)

        if response.status_code >= 200 and response.status_code <=210:
            self.logger.info("OK | %s files have been processed and added to the PDWH" %len(self.files))
        else:
            self.logger.info("Result for ends with %s" % (response.status_code))
            self.logger.info("Result: %s" % str(response.text))

