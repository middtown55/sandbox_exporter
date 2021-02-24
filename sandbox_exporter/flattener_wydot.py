import copy
import dateutil.parser
import json
import random

from sandbox_exporter.flattener import CvDataFlattener


class WydotBSMFlattener(CvDataFlattener):
    '''
    Reads each raw BSM data record from WYDOT CV Pilot and performs data transformation,
    including:
    1) Flatten the data structure
    2) Rename certain fields to achieve consistency across data sets
    3) Add additional fields to enhance usage of the data set in Socrata
    (e.g. randomNum, coreData_position)

    '''
    def __init__(self, **kwargs):
        super(WydotBSMFlattener, self).__init__(**kwargs)
        self.rename_prefix_fields += [
            ('metadata_receivedMessageDetails_locationData', 'metadata_rmd'),
            ('metadata_receivedMessageDetails', 'metadata_rmd'),
            ('payload_data_coreData', 'coreData'),
        ]
        self.rename_fields += [
            ('metadata_odeReceivedAt', 'metadata_received_at'),
            ('payload_dataType', 'dataType'),
            ('coreData_position_longitude', 'coreData_position_long'),
            ('coreData_position_latitude', 'coreData_position_lat'),
            ('coreData_position_elevation', 'coreData_elevation')
        ]
        self.json_string_fields += ['coreData_size', 'payload_data_coreData_size']
        self.part2_rename_prefix_fields = [
            ('pathHistory', 'part2_vse_ph'),
            ('pathPrediction', 'part2_vse_pp'),
            ('classDetails', 'part2_suve_cd'),
            ('vehicleAlerts', 'part2_spve_vehalert'),
            ('description', 'part2_spve_event'),
            ('trailers', 'part2_spve_tr'),
            ('events', 'part2_vse_events')
        ]
        self.part2_rename_fields = [
            ('part2_vse_ph_crumbData', 'part2_vse_ph_crumbdata'),
            ('part2_vse_pp_radiusOfCurve', 'part2_vse_pp_radiusofcurve'),
            ('lights', 'part2_vse_lights'),
            ('part2_suve_cd_height', 'part2_suve_vd_height'),
            ('part2_suve_cd_mass', 'part2_suve_vd_mass'),
            ('part2_suve_cd_trailerWeight', 'part2_suve_vd_trailerweight'),
            ('part2_spve_vehalert_event_sspRights', 'part2_spve_vehalert_events_sspRights'),
            ('part2_spve_vehalert_event_events', 'part2_spve_vehalert_events_events'),
            ('part2_spve_event_description', 'part2_spve_event_desc'),
            ('part2_spve_tr_sspRights', 'part2_spve_tr_ssprights'),
            ('part2_spve_tr_connection', 'part2_spve_tr_conn')
        ]
        self.part2_json_string_fields = ['events']

    def process(self, raw_rec):
        '''
        	Parameters:
        		raw_rec: dictionary object of a single BSM record

        	Returns:
        		transformed dictionary object of the BSM record
        '''
        out = super(WydotBSMFlattener, self).process(raw_rec)

        for part2_val in out.get('payload_data_partII', []):
            part2_val_out = self.transform(part2_val['value'],
                rename_prefix_fields=self.part2_rename_prefix_fields,
                rename_fields=self.part2_rename_fields,
                json_string_fields=self.part2_json_string_fields)
            out.update(part2_val_out)
        if 'payload_data_partII' in out:
            del out['payload_data_partII']

        if 'coreData_position_long' in out:
            out['coreData_position'] = "POINT ({} {})".format(out['coreData_position_long'], out['coreData_position_lat'])

        metadata_received_at = dateutil.parser.parse(out['metadata_received_at'][:23])
        out['metadata_received_at'] = metadata_received_at.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]

        return out


class WydotTIMFlattener(CvDataFlattener):
    '''
    Reads each raw TIM data record from WYDOT CV Pilot and performs data transformation,
    including:
    1) Flatten the data structure
    2) Rename certain fields to achieve consistency across data sets
    3) Add additional fields to enhance usage of the data set in Socrata
    (e.g. randomNum, coreData_position)

    '''
    def __init__(self, **kwargs):
        super(WydotTIMFlattener, self).__init__(**kwargs)

        self.rename_prefix_fields += [
            ('metadata_receivedMessageDetails_locationData', 'metadata_rmd'),
            ('metadata_receivedMessageDetails', 'metadata_rmd'),
            ('payload_data_MessageFrame_value_traveler_information_dataFrames_TravelerDataFrame', 'travelerdataframe'),
            ('payload_data_MessageFrame_value_traveler_information', 'travelerinformation'),
            ('_SEQUENCE', '_sequence'),
            ('travelerdataframe_msgId_roadSignID_position', 'travelerdataframe_msgId'),
            ('travelerdataframe_msgId_roadSignID', 'travelerdataframe_msgId'),
            ('travelerdataframe_regions_GeographicalPath_anchor', 'travelerdataframe_anchor'),
            ('travelerdataframe_regions_GeographicalPath_description_path', 'travelerdataframe_desc'),
            ('travelerdataframe_regions_GeographicalPath', 'travelerdataframe')

        ]

        self.rename_fields += [
            ('metadata_odeReceivedAt', 'metadata_received_at'),
            ('payload_dataType', 'dataType'),
            ('payload_data_MessageFrame_messageId', 'messageId'),
            ('travelerdataframe_desc_offset_xy_nodes_NodeXY', 'travelerdataframe_desc_nodes')
        ]
        self.json_string_fields += [
        ]

    def process(self, raw_rec):
        '''
        	Parameters:
        		raw_rec: dictionary object of a single BSM record

        	Returns:
        		transformed dictionary object of the BSM record
        '''
        out = super(WydotTIMFlattener, self).process(raw_rec)

        if 'travelerdataframe_msgId_lat' in out:
            travelerdataframe_msgid_lat = float(out['travelerdataframe_msgid_lat'])/10e6
            travelerdataframe_msgid_long = float(out['travelerdataframe_msgid_long'])/10e6
            out['travelerdataframe_msgId_position'] = "POINT ({} {})".format(travelerdataframe_msgid_long, travelerdataframe_msgid_lat)

        return out

    def process_and_split(self, raw_rec):
        '''
        Turn various Traveler Information DataFrame schemas to one where the Traverler DataFrame is stored at:
        rec['payload']['data']['MessageFrame']['value']['traveler_information']['dataFrames']['TravelerDataFrame']

        '''
        out_recs = []
        traveler_information = copy.deepcopy(raw_rec.get('payload', {}).get('data', {}).get('MessageFrame', {}).get('value', {}).get('traveler_information'))

        if not traveler_information:
            return [self.process(raw_rec)]

        if raw_rec['metadata']['schemaVersion'] == 5:
            out_recs.append(raw_rec)
        else:
            # elif raw_rec['metadata']['schemaVersion'] == 6:
            traveler_data_frames = traveler_information.get('dataFrames')
            if type(traveler_data_frames) == list:
                tdfs = [i.get('TravelerDataFrame') for i in traveler_data_frames if i.get('TravelerDataFrame')]
                if len(tdfs) != len(traveler_data_frames):
                    print('travelerDataFrames discrepancy: {} -> {}'.format(len(traveler_data_frames), len(tdfs)))
            elif type(traveler_data_frames) == dict:
                traveler_data_frames_opt1 = traveler_data_frames.get('TravelerDataFrame')
                traveler_data_frames_opt2 = traveler_data_frames.get('dataFrames', {}).get('TravelerDataFrame')
                tdfs = traveler_data_frames_opt1 or traveler_data_frames_opt2
                if type(tdfs) != list:
                    tdfs = [tdfs]
            else:
                print('No Traveler DataFrame found in this: {}'.format(traveler_data_frames))
                return [self.process(raw_rec)]

            for tdf in tdfs:
                geographical_path = copy.deepcopy(tdf.get('regions', {}).get('geographical_path'))
                if type(geographical_path) == list:
                    for path in geographical_path:
                        tdf['regions']['geographical_path'] = path

                        temp_rec = copy.deepcopy(raw_rec)
                        temp_rec['payload']['data']['MessageFrame']['value']['traveler_information']['dataFrames'] = {}
                        temp_rec['payload']['data']['MessageFrame']['value']['traveler_information']['dataFrames']['TravelerDataFrame'] = tdf
                        out_recs.append(temp_rec)
                else:
                    temp_rec = copy.deepcopy(raw_rec)
                    temp_rec['payload']['data']['MessageFrame']['value']['traveler_information']['dataFrames'] = {}
                    temp_rec['payload']['data']['MessageFrame']['value']['traveler_information']['dataFrames']['TravelerDataFrame'] = tdf
                    out_recs.append(temp_rec)

        return [self.process(out_rec) for out_rec in out_recs if out_rec]
