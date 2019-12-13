import io
import pandas as pd
import copy


class Token:
    def __init__(self, start_val, in_type, attr=None, line_nbr=-1):

        self.start_val = start_val
        self.type = in_type
        self.attr = attr
        self.line_nbr = line_nbr

        try:
            sep = self.attr.index(":")
            self.sub = {}
            self.sub[self.attr[:sep]] = self.attr[sep + 1:]
        except:
            sep = 0

    def __repr__(self):

        if hasattr(self, "sub"):
            return "{start: %s | type: %s // attr: %s + sub: %s}" % (self.start_val, self.type, self.attr, self.sub)
        else:
            return "{start: %s | type: %s // attr: %s}" % (self.start_val, self.type, self.attr)

    def to_dict(self):
        d = {
            "startVal": self.start_val,
            "type": self.type
        }

        if hasattr(self, "sub"):
            for key in self.sub.keys():
                d["sub_key"] = key
                d["sub_val"] = self.sub[key]
        else:
            d["attr"] = self.attr
        return d

    def match(self, type, attr=None):
        return self.type == type and (attr is None or self.attr == attr)

class LabRequest:

    def __init__(self, in_df, **kwargs):

        try:
            self.process_time = in_df[in_df["sub_key"] == 'processtime']["sub_val"].values[0]
            self.request_id = in_df[in_df["sub_key"] == 'Auftragsnummer']["sub_val"].values[0]
            self.form_type = in_df[in_df["sub_key"] == 'formulartyp']["sub_val"].values[0]
            self.form_name = in_df[in_df["sub_key"] == 'formular']["sub_val"].values[0]

            self.lanr = in_df[(in_df["type"] == '0212')]["attr"].values[0]
            self.bstnr = in_df[(in_df["type"] == '0201')]["attr"].values[0]

            self.patient_surname = in_df[(in_df["type"] == '3101')]["attr"].values[0]
            self.patient_name = in_df[(in_df["type"] == '3101')]["attr"].values[0]
            self.street = in_df[(in_df["type"] == '3107')]["attr"].values[0]

            self.services = []
            self._read_services(in_df)

        except Exception as e:
            raise e

    def __repr__(self):
        return "<LabRequest as of: date: %s | id: %s | #%s services>" % \
        (self.process_time, self.request_id, len(self.services))

    def _read_services(self, in_df):

        self.services = []

        service_obj = None

        for idx, row in in_df.iterrows():

            if row["type"] == '8410':
                if service_obj is not None:
                    self.services.append(copy.deepcopy(service_obj))
                service_obj = ServiceItem(request_id=self.request_id, test_ident=row["attr"])

            if row["type"] == '8411':
                service_obj.set_analyt_desc(analyt_description=row["attr"])

            if row["type"] == '8430':
                service_obj.set_analyt_probe_type(probe_type=row["attr"])

    def to_dict(self):

        d = {
            "process_time": self.process_time,
            "request_id": self.request_id,
            "form_type": self.form_type,
            "form_name": self.form_name,
            "lanr": self.lanr,
            "bstnr": self.bstnr,
            "patient_surname": self.patient_surname,
            "patient_name": self.patient_name,
            "street": self.street
        }

        return d

    def get_services_as_list(self):
        return [(s.to_dict()) for s in self.services]

class ServiceItem:

    def __init__(self, request_id, test_ident, **kwargs):

        if not test_ident or not request_id:
            raise Exception('Both test_ident or request_id are mandatory')

        self.analyt_test_ident = test_ident
        self.request_id = request_id

    def set_analyt_desc(self, analyt_description):
        self.analyt_desc = analyt_description

    def set_analyt_probe_type(self, probe_type):
        self.analyt_probe_type = probe_type

    def __repr__(self):
        return "<ServiceItem Analyt-Ident: %s>" % (self.analyt_test_ident)

    def to_dict(self):
        d = {
            "analyt_test_ident": self.analyt_test_ident,
            "request_id": self.request_id,
            "analyt_desc": self.analyt_desc,
            "analyt_probe_type": self.analyt_probe_type,
        }

        return d
class LabFile:

    def __init__(self, file_path, **kwargs):

        self.file_path = file_path

        self.contents = self.read_file()

        self.labrequest = self.process_contents()

    def read_file(self):
        with io.open(self.file_path, 'r', encoding="cp1252") as f:
            contents = f.read().split("\n")
        f.close()
        return contents

    def process_contents(self):

        sets = []
        for line_nbr, line in enumerate(self.contents):
            t = Token(line[0:2], line[3:7], line[7:], line_nbr + 1)

            if t.attr:
                sets.append(t)

        df_doc = pd.DataFrame([(s.to_dict()) for s in sets])

        return LabRequest(df_doc)

    def get_lab_request(self):
        return self.labrequest