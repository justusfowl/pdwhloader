import json
from flask import Flask, jsonify, request, abort
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

    if not request.json:
        abort(400)

    src_table_list = json.loads(request.data)
    print(src_table_list)

    post_processes = [{
        "database": "target",
        "execute": ["drop table if exists o_dm_abweichungsanalyse; ",
                    "create table o_dm_abweichungsanalyse as select * from dm_AbweichungsAnalyse; ",
                    "drop table if exists o_dm_abweichungsanalyse; ",
                    "create table o_dm_abweichungsanalyse as select * from dm_leistungenZeitverlauf;"]
    }]

    my_loader = Loader(
        src_schema="WINACS",
        src_table_list=src_table_list,
        post_processes=post_processes,
        target_schema="pdwh")

    my_loader.process_tables()

    my_loader.post_process()


    return "Ok"



if __name__ == "__main__":
    app.run()