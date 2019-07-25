import json
from flask import Flask, jsonify, request, abort, Response
from Loader import Loader

app = Flask(__name__)

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
                            "drop table if exists o_dm_abweichungsanalyse; ",
                            "create table o_dm_abweichungsanalyse as select * from dm_leistungenZeitverlauf;"]
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
        return Response("{'msg':'Unknown error'}", status=500, mimetype='application/json')

if __name__ == "__main__":
    app.run(debug=True, host='0.0.0.0')