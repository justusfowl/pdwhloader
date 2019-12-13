import json
import os
from flask import Flask, jsonify, flash, request, redirect, abort, Response, make_response
from werkzeug.utils import secure_filename
import pandas as pd
import pymysql
import sqlalchemy
import logging
from lab.File import LabFile
from Loader import Loader, LabLoader

def initApi():

    logger = logging.getLogger(__name__)
    logger.info('PDWH API initialized')

    app = Flask(__name__)

    app.config['SESSION_TYPE'] = 'filesystem'
    app.config['UPLOAD_FOLDER'] = os.environ.get("LDT_UPLOAD_FOLDER")

    ALLOWED_EXTENSIONS = {'ldt'}



    def allowed_file(filename):
        return '.' in filename and \
               filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


    def get_file_ext(filename):
        return filename.rsplit('.', 1)[1].lower()

    @app.route("/")
    def hb():
        msg = {
            "status" : 200,
            "msg" : "healthy"
        }
        return jsonify(msg)

    @app.route("/load", methods=['POST'])
    def handle_load():

        try:

            if not request.json:
                abort(400)

            cfg = json.loads(request.data)

            if not "src_table_list" in cfg:
                return Response("{'msg':'Please Provide Valid Config File'}", status=500, mimetype='application/json')

            print(cfg["src_table_list"])

            src_table_list = cfg["src_table_list"]

            if "post_processes" in cfg:
                post_processes = cfg["post_processes"]
            else:
                '''
                post_processes = [{
                    "database": "target",
                    "execute": ["drop table if exists o_dm_abweichungsanalyse; ",
                                "create table o_dm_abweichungsanalyse as select * from dm_AbweichungsAnalyse; ",
                                "drop table if exists dm_leistungenZeitverlauf; ",
                                "create table dm_leistungenZeitverlauf as select * from dm_leistungenZeitverlauf;"]
                }]
                '''

                post_processes = []


            my_loader = Loader(
                src_schema="WINACS",
                src_table_list=src_table_list,
                post_processes=post_processes,
                target_schema="pdwh")

            my_loader.process_tables()

            my_loader.post_process()

            return Response("{'msg':'Tables have been loaded'}", status=200, mimetype='application/json')

        except Exception as e:
            return make_response(str(e), 500)


    @app.route("/lab/ldt", methods=['POST'])
    def handle_load_lab_ldt():

        data = {"msg": ""}

        try:

            # check if the post request has the file part
            if 'file' not in request.files:
                flash('No file part')
                return redirect(request.url)

            files = request.files.getlist('file')

            # if user does not select file, browser also
            # submit an empty part without filename
            list_lab_requests = []
            list_lab_services = []

            list_file_names = []

            for file in files:

                list_file_names.append(file.filename)

                if file.filename == '':
                    flash('No selected file')
                    return redirect(request.url)

                if file and allowed_file(file.filename):
                    filename = secure_filename(file.filename)

                    file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)

                    file.save(file_path)

                    lab_file = LabFile(file_path=file_path)

                    lab_request = lab_file.get_lab_request()

                    list_lab_requests.append(lab_request.to_dict())
                    list_lab_services.extend(lab_request.get_services_as_list())


            df_lab_req = pd.DataFrame(list_lab_requests)
            df_lab_services = pd.DataFrame(list_lab_services)

            my_LabLoader = LabLoader(target_schema="pdwh")
            my_LabLoader.store_lab_requests(df_lab_req, df_lab_services)

            data["msg"] = "SUCCESS"
            data["files"] = list_file_names
            data["code"] = 201

            return make_response(jsonify(data), data["code"])

        except (pymysql.err.IntegrityError, sqlalchemy.exc.IntegrityError) as error:
            data["msg"] = str(error.orig)
            data["failed_files"] = list_file_names
            data["code"] = 409
            return make_response(jsonify(data), data["code"])

        except Exception as e:
            data["msg"] = str(e)
            data["failed_files"] = list_file_names
            data["code"] = 500
            return make_response(jsonify(data), data["code"])

    return app
