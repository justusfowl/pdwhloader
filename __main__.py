
'''

src_table_list = [
    {
        "table" : "Schein",
        "create" : True,
        "drop_if_exists" : True,
        "time_ref_col" : "DatumAnlage",
        "time_ref_from" : "DATEADD(qq, DATEDIFF(qq, 0, GETDATE()), 0)"
    },
    {
        "table": "ScheinLZ",
        "create": True,
        "drop_if_exists": True,
        "time_ref_col": "DatumÄnderung",
        "time_ref_from": "DATEADD(qq, DATEDIFF(qq, 0, GETDATE()), 0)"
    },
    {
        "table": "Patient",
        "create": True
    },
    {
        "table": "Schein",
        "create": True,
        "drop_if_exists": True,
        # "order_by": " DatumAnlage DESC",
        "where_clause" : " Nummer <> 63153" # diese Nummer enthält ungültiges Datum (>2500a.D.), 
        "post_process" : "UPDATE Schein SET rep_date = CF_getrepdatefromdate(DatumAnlage); UPDATE Schein SET compare_rep_date = cf_getcomparequarterfromrepdate(DatumAnlage);",
        "tructate_first": True
    }
    ]   
    
   
     {
        "table": "Schein",
        "where_clause": " Nummer <> 63153",
        "post_process": "UPDATE Schein SET rep_date = CF_getrepdatefromdate(DatumAnlage); UPDATE Schein SET compare_rep_date = cf_getcomparequarterfromrepdate(DatumAnlage);",
        "tructate_first": True
    },
    {
        "table": "ScheinLZ",
        "post_process": "UPDATE ScheinLZ SET rep_date = CF_getrepdatefromdate(Datum); ",
        "tructate_first": True
    },
    {
        "table": "ScheinDiagnosen",
        "post_process": "UPDATE ScheinDiagnosen SET rep_date = CF_getrepdatefromdate(Datum); ",
        "tructate_first": True
    }

    

src_table_list = [
    {
        "table": "Schein",
        "where_clause": " Nummer <> 63153",
        "post_process": "UPDATE Schein SET rep_date = CF_getrepdatefromdate(DatumAnlage); UPDATE Schein SET compare_rep_date = cf_getcomparequarterfromrepdate(DatumAnlage);",
        "tructate_first": True
    },
    {
        "table": "ScheinLZ",
        "post_process": "UPDATE ScheinLZ SET rep_date = CF_getrepdatefromdate(Datum); ",
        "tructate_first": True
    },
    {
        "table": "ScheinDiagnosen",
        "post_process": "UPDATE ScheinDiagnosen SET rep_date = CF_getrepdatefromdate(Datum); ",
        "tructate_first": True
    }
    ]

post_processes = [{
        "database" : "target",
        "execute" :  [ "drop table if exists o_dm_abweichungsanalyse; ",
                       "create table o_dm_abweichungsanalyse as select * from dm_AbweichungsAnalyse; ",
                       "drop table if exists o_dm_abweichungsanalyse; ",
                       "create table o_dm_abweichungsanalyse as select * from dm_leistungenZeitverlauf;"]
    }]

'''

if __name__ == "__main__":
    print("OK")