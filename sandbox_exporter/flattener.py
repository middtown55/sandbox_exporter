import copy
import dateutil.parser
import json
import random
import re

import sandbox_exporter

def load_flattener(key):
    '''
    Load appropriate data flattener based on pilot site and message type
    '''
    pilot, message_type = key.split('/')[:2]
    try:
        pilot_flattener = getattr(sandbox_exporter, 'flattener_{}'.format(pilot))
        flattener = getattr(pilot_flattener, '{}{}Flattener'.format(pilot.title(), message_type))
    except:
        print('flattener_{}.{}{}Flattener not found. Load generic CVP flattener.'.format(pilot, pilot.title(), message_type))
        pilot_flattener = getattr(sandbox_exporter, 'flattener')
        flattener = getattr(pilot_flattener, 'CvDataFlattener')
    return flattener


class DataFlattener(object):
    def __init__(self):
        self.rename_prefix_fields = []
        self.rename_fields = []
        self.int_fields = []
        self.json_string_fields = []

    def flatten_dict(self, d, json_string_fields=[]):
        def expand(key, value):
            if isinstance(value, dict):
                # get key as value
                if len(value.items()) == 1 and list(value.values())[0] == None:
                    return [ (key, list(value.keys())[0])]
                # dump as json string instead of expanding further
                elif key in json_string_fields:
                    return [(key, json.dumps(value))]
                # expand dict
                else:
                    return [ (key + '_' + k, v) for k, v in self.flatten_dict(value, json_string_fields).items() ]
            else:
                return [ (key, value) ]

        items = [ item for k, v in d.items() for item in expand(k, v) ]
        return dict(items)

    def transform(self, raw_rec, rename_prefix_fields=[], rename_fields=[],
                int_fields=[], json_string_fields=[]):
        # order of operation: rename prefix, rename fields, stringify json fields
        out = self.flatten_dict(raw_rec, json_string_fields)

        for old_prefix, new_prefix in rename_prefix_fields:
            out = {k.replace(old_prefix, new_prefix) if old_prefix in k else k: v
                   for k,v in out.items()}

        for old_f, new_f in rename_fields:
            if old_f in out:
                out[new_f] = copy.deepcopy(out[old_f])
                del out[old_f]

        out = {k: int(v) if k in int_fields else v for k,v in out.items()}
        return out

    def add_enhancements(self, rec):
        return rec

    def process(self, raw_rec, **kwargs):
        rec = self.transform(raw_rec,
                             self.rename_prefix_fields,
                             self.rename_fields,
                             self.int_fields,
                             self.json_string_fields)
        rec = self.add_enhancements(rec)
        return rec

    def process_and_split(self, raw_rec, **kwargs):
        return [self.process(raw_rec, **kwargs)]


def parse_date(date_str):
    clean_date_str = lambda x: re.sub(r'\[[a-zA-Z]*\]', '', x)
    return dateutil.parser.parse(clean_date_str(date_str))

class CvDataFlattener(DataFlattener):
    def __init__(self, *args, **kwargs):
        super(CvDataFlattener, self).__init__(*args, **kwargs)
        self.rename_prefix_fields = [
            ('payload_data_coreData_', 'coreData_'),
            ('coreData_accelSet_', 'coreData_accelset_')
        ]
        self.rename_fields = [
            ('metadata_dataType', 'dataType'),
            ('metadata_recordGeneratedAt', 'metadata_generated_at'),
            ('metadata_recordGeneratedBy', 'metadata_generatedBy')
        ]
        self.int_fields = [
            'metadata_psid',
            'metadata_schemaVersion'
        ]
        self.json_string_fields = ['size']

    def add_enhancements(self, rec):
        metadata_generated_at = parse_date(rec['metadata_generated_at'])
        rec['metadata_generated_at'] = metadata_generated_at.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
        rec['randomNum'] = random.random()
        rec['mmetadata_generated_at_timeOfDay'] = metadata_generated_at.hour + metadata_generated_at.minute/60 + metadata_generated_at.second/3600
        return rec
