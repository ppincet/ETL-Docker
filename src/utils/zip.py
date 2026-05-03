import zipfile
import itertools
import time
from datetime import datetime
from utils import force, constants, common
from collections import defaultdict
import os
from config import settings
import csv
import shutil

def upload_file(sf, zip_filename, wm):
    # try:
    manifest_entries = []
    file_groups = defaultdict(list)
    header_size = 0
    total_size = 0
    # print('before ----')
    # print(f'scaffs from zip:{force.get_results(sf)}')
    for (filename, entity_name), list_of_gens in force.get_results(sf).items():
        for gen in list_of_gens:
            file_groups[filename].append((entity_name, gen))
   
    with zipfile.ZipFile(zip_filename, "w", zipfile.ZIP_DEFLATED) as zf:
        for filename, entity_list in file_groups.items():
            
            csv_entry = None
            file_header_written = False

            try:
                for entity_name, generator in entity_list:
                    try:
                        header_row = next(generator)
                    except StopIteration:
                        continue
                    if csv_entry is None:
                        csv_entry = zf.open(filename + '.csv', "w")
                        manifest_entries.append(filename)
                    if not file_header_written:
                        if isinstance(header_row, tuple): 
                            header_row = header_row[0]
                            header_size = len(header_row.encode('utf-8'))
                            total_size += header_size
                        csv_entry.write(header_row.encode('utf-8'))
                        file_header_written = True
                    else:
                        pass
                    for line_tuple in generator:
                        if isinstance(line_tuple, tuple):
                            csv_line, record_date = line_tuple
                        else:
                            csv_line, record_date = line_tuple, None
                        csv_entry.write(csv_line.encode('utf-8'))
                        total_size += len(csv_line.encode('utf-8'))
                        if record_date:
                            curr = wm.get(entity_name)
                            if curr is None or record_date > curr:
                                wm[entity_name] = record_date
            finally:
                if csv_entry:
                    csv_entry.close()
        #manifest region
        if total_size > header_size:
            manifest_header = 'propertyName,value\n'
            manifest_entry = zf.open('manifest.csv', "w")
            manifest_entry.write(manifest_header.encode('utf-8'))
            manifest_contents = force.get_manifest(sf)
            system_results = manifest_contents['systemResultsInternal']
            for m in manifest_contents['results']:
                manifest_prefix = m.get('prefix')
                manifest_property = m.get('property')
                manifest_value = m.get('value')
                if manifest_prefix == system_results.get('file') and manifest_property not in manifest_entries:
                    continue
                if manifest_property in manifest_entries:
                    manifest_value = system_results.get('delta')
                current_line = f"{manifest_prefix}.{manifest_property},{manifest_value}\n"
                manifest_entry.write(current_line.encode('utf-8'))
            manifest_entry.close()
    return constants.ETL_SUCCESS if total_size > header_size else constants.ETL_EMPTY

def get_latest_workload(sftp_conn, n=25):
    all_files = sftp_conn.listdir_attr(settings.SSH_REMOTE_UFOLDER)
    groups = defaultdict(list)
    for f in all_files:
        if f.filename.endswith('.zip'):
            prefix = f.filename.split('_')[0]
            groups[prefix].append(f)
    final_list = []
    for prefix in ['unit', 'bulk']:
        if prefix in groups:
            groups[prefix].sort(key=lambda x: x.filename, reverse=True)
            names = [f.filename for f in groups[prefix][:n]]
            final_list.extend(names)
    # print(f'from latest workload:final_list')
    return final_list
'''
    Args:
        app_settings (dict): Configuration dictionary for the operation.
         - 
'''
def perform_upsert(sf_client, data, app_settings):
    # zip_map = app_settings.get('zip_map')
    # print(f'zip_storage: {zip_map}')
    formatted_data = []
    for item in data:
        raw_name = item.get('Name')
        if raw_name and str(raw_name).strip():
            formatted_name = raw_name
        else:
            formatted_name = f'Learning - {datetime.now().strftime("%Y%m%d-%H%M%S")}'
        # print(f'item: {item.get('external_source_id__c')}')    
        record = {
            'Name': formatted_name,
            'CIPCODE': item.get('external_source_id__c'),
            'Composite_Source__c': f"{item.get('external_source_id__c')}",
            'isActive': item.get('Active_Custom__c'),
            'Type': 'LearningCourse'
        } 
        formatted_data.append(record)
        # print(f'from 1st upsert: {record}')
    
    try:
        external_id = 'Composite_Source__c' 
        sf_bulk_resource = getattr(sf_client.bulk, 'Learning')
        results = sf_bulk_resource.upsert(formatted_data, external_id)
        '''
            faults - numbers
            fault_lines - logs
        '''
        faults = set()
        fault_lines = []
        
        # print('step two (after learnings upsert)')
        for i, item in enumerate(results):
            if item.get('success'): 
                data[i]['LearningId'] = item.get('id')
                data[i]['CourseNumber'] = data[i].get('external_source_id__c') 
                # print('success from 1st upsert')
            else:
                faults.add(i)
                print('before first error')
                print(item)
                first_err = item.get('errors', [{}])[0]
                print(f'first error: {first_err}')
                fault_entry = {
                    'User__c' : settings.INTEGRATION_USER_ID,
                    'Process_name__c': 'back integration',
                    'Step__c' : 'Creating new learning',
                    'Message__c' : first_err.get('statusCode', 'UNKNOWN_ERROR'),
                    'Details__c' : first_err.get('message', 'No error message provided'),
                    'Context__c' : f"Source: {data[i].get('zipName')}"
                }
                fault_lines.append(fault_entry)
                data[i]['LearningId'] = None
        sf_bulk_resource = getattr(sf_client.bulk, app_settings['entityApiName'])
        upsert_payload = [
            {k: v for k, v in row.items() if k != 'zipName'} 
            for row in data if row.get('LearningId') is not None
        ]
        # print(f'upsert payload: {upsert_payload}')
        try:
            for i, item in enumerate(sf_bulk_resource.upsert(upsert_payload, app_settings['extIdName'])):
                # print(f'item from upsert({i}):{item}')
                if not item.get('success'):
                    errors = item.get('errors', [{}])
                    first_error = errors[0] if errors else {}
                    origin_zip = data[i].get('zipName', 'Unknown_Source')
                    fault_entry = {
                            'User__c' : settings.INTEGRATION_USER_ID,
                            'Process_name__c': 'back integration',
                            'Step__c' : 'Creating new learning course',
                            'Message__c': first_error.get('statusCode', 'UNKNOWN_ERROR'),
                            'Details__c': first_error.get('message', 'No error message provided'),
                            'Context__c' : f'Source: {origin_zip}'
                        }  
                    faults.add(i)
                    fault_lines.append(fault_entry)
                    # print(f'from final: {fault_entry}')
        except Exception as e:
            print(f'exception inside last upsert: {e}')
            raise
        if fault_lines:
            sf_bulk_resource = getattr(sf_client.bulk, 'User_Provisioning_Log__c')
            # 20260408 note
            results = sf_bulk_resource.insert(fault_lines)
    except Exception as e:
        print(f"critical upsert Error: {e}")
        raise   
    
    return faults


# def process_sftp_to_sf(sftp_client, sf_client, mappings):
def process_sftp_to_sf(conns, mappings):
    sftp_client = conns['sftp_client']
    sf_client = conns['sf_client']
    local_tmp = './tmp'
    remote_path = settings.SSH_REMOTE_UFOLDER
    os.makedirs(local_tmp, exist_ok=True)
    zip_files = get_latest_workload(sftp_client, settings.SSH_WINDOW)
    data_buffers = defaultdict(list)
    zip_storage = defaultdict(list)
    for zip_name in zip_files:
        local_zip_path = os.path.join(local_tmp, zip_name)
        sftp_client.get(os.path.join(remote_path, zip_name), local_zip_path)
        with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
            csv_files = [f for f in zip_ref.namelist() 
             if f.endswith('.csv') 
             and os.path.basename(f) not in constants.ETL_EXCLUDED_FILES]
            for csv_name in csv_files:
                csv_target = os.path.basename(csv_name)
                target_path = os.path.join(local_tmp, csv_target)
                stx = len(data_buffers.get(csv_target, []))
                with zip_ref.open(csv_name) as source:
                    with open(target_path, "wb") as target:
                        shutil.copyfileobj(source, target)
                csv_map = mappings.get(csv_target, {})
                external_settings = {
                    'extIdName': csv_map.get('header', {}).get('external_id_name'),
                    'entityApiName': csv_map.get('header', {}).get('object_name'),
                }
    
                details = csv_map.get('details', [])
                ext_id_field = external_settings['extIdName']
                with open(target_path, mode='r', encoding='utf-8') as f:
                    header_line = f.readline()
                    if not header_line: continue
                    headers = [h.strip() for h in header_line.split(',')]
                    unique_keys = set()
                    for line in f:
                        if not line.strip(): continue
                        row = dict(zip(headers, line.strip().split(',')))
                        entity = str(map_data(details, row).get(ext_id_field) or '').strip()
                        # print(f'mapped one:{entity}')
                        if entity: unique_keys.add(entity)
                    results = force.get_existing_entries(sf_client, external_settings, unique_keys)
                    # print(f'results from uni: {results}')
                    # id_lookup = {str(rec.get(ext_id_field.lower())): rec.get('id') for rec in results}
                    try:
                        id_lookup = {str(rec.get(ext_id_field.lower())): rec.get('id') for rec in results}
                        f.seek(0)
                        f.readline()
                        for line in f:
                            if not line.strip(): continue 
                            parts = next(csv.reader([line]))
                            row = dict(zip(headers, parts))
                            mapped_row = map_data(details, row)
                            mapped_row['zipName'] = zip_name
                            lookup_key = str(mapped_row.get(external_settings['extIdName']))
                            lookup_value = id_lookup.get(lookup_key)
                            if lookup_value :
                                mapped_row['id'] = lookup_value
                            if csv_target != constants.ETL_UNE_RESULTS or lookup_value:
                                if csv_target not in data_buffers:
                                    data_buffers[csv_target] = []
                                data_buffers[csv_target].append(mapped_row)
                        
                            
                            # if len(data_buffers[csv_name]) >= settings.BUFFER_SIZE:
                            #     print('temp flush')
                            #     match csv_name:
                            #         case 'results.csv':
                            #             print('inside results')
                            #         case 'lineItems.csv':
                            #             tempo = perform_upsert(sf_client, data_buffers[csv_name], external_settings)
                            #             #print('managin lineItems(for update main)')
                            #             # upsert 
                            #             # for item in tempo:
                            #             #     print(f'result from line items(main): {item}')
                                        
                            #     #perform_upsert(sf_client, data_buffers[csv_name], external_settings)
                            #     data_buffers[csv_name] = [] 
                        #print(f'data buffers for {csv_name}: {data_buffers[csv_name]}')
                        # if data_buffers[csv_name]:
                        #     # for record in data_buffers[csv_name]:
                        #     #             print(f'final results for {csv_name}:{record}')
                        #     match csv_name:
                        #         case 'results.csv':
                        #             # we should perform update here!
                                    
                        #             force.perform_update(sf_client, data_buffers[csv_name], external_settings)    
                        #         case 'lineItems.csv':
                        #             #force.perform_update(sf_client, data_buffers[csv_name], external_settings)    
                        #             tempo = perform_upsert(sf_client, data_buffers[csv_name], external_settings)
                        #             # print('managin lineItems(for update flush)')
                        #             # for item in tempo:
                        #             #     print(f'result from line items(flush): {item}')
                            etx = len(data_buffers.get(csv_target, []))  
                            if etx > stx:
                                new_range = common.ZipRange(stx, etx - 1, zip_name)
                                zip_storage[csv_target].append(new_range)       
                        os.remove(target_path)
                    except Exception as e:
                        print(f'from zip {e}')
                        raise
                
        try:
             os.remove(local_zip_path)
        except Exception as e:
            print(f'from main loop: {e}')
            raise
    # print('❗ performing main cycle ❗')

    flush_buffer(conns, data_buffers, mappings, zip_storage)
    return
  
def map_data(fields, row):
    # print(f'source row: {row}')
    mapped_one = {}
    for field in fields:
        target_field = field.get('target')
        field_type = field.get('type')
        source_key = field.get('source')
        raw_value = row.get(source_key, "")
        if field_type == 'CompositeKey':
            keys = field.get('compositeKeys', [])
            raw_value = ",".join([str(row.get(k, '')) for k in keys if row.get(k) is not None])
        rules = field.get('rules', {})
        if rules:
            reversed_rules = {v: k for k, v in rules.items()}
            processed_value = reversed_rules.get(raw_value, raw_value)
        else:
            processed_value = raw_value
        complex_rules = field.get('complexRules', {})
        if complex_rules:
            final_value = complex_rules.get(raw_value)
        else:
            final_value = processed_value
        if target_field:
            if isinstance(final_value, bool):
                final_value = str(final_value).lower()
            mapped_one[target_field] = final_value      
        # print(f'result: {mapped_one}')
    return mapped_one
def flush_buffer(clients, data_buffers, mappings, zip_storage):
    # print('❗ performing main cycle from flush❗')
    # print(f'data buffers: {data_buffers}')
    sf_client = clients['sf_client']
    sftp_client = clients['sftp_client']
    for csv_name, entry in data_buffers.items():
        # if not entry: continue
        # print(f'⚠️  entry {csv_name} ({os.path.basename(csv_name)})  ⚠️')
        csv_map = mappings.get(f'{os.path.basename(csv_name)}', {})
        work_storage = zip_storage.get('lineItems.csv')
        app_settings =  {
                    'extIdName': csv_map.get('header', {}).get('external_id_name'),
                    'entityApiName': csv_map.get('header', {}).get('object_name'),
                    # 'zip_map': work_storage
                } 
        # print(f'operation: ({csv_name}){csv_map.get('header', {}).get('operation')}')
        # continue
        match csv_map.get('header', {}).get('operation'):
            case 'update':
                # print('before update')
                # for item in entry:
                #     #print(f'line from update: {item}')
                # print('before update')
                # print(f'collected data: {data_buffers[csv_name]}')
                force.perform_update(sf_client, entry, app_settings)
            case 'upsert' :
                # print('before upsert(main cycle)')
                fault_zips = set()
                # print(f'entries from finish upsert:{entry}')
                # print(f'entry:{csv_name} / {entry}')
                tempo = perform_upsert(sf_client, entry, app_settings)
                # print(f'tempo:{tempo}')
                fault_zips = set()
                # work_storage = zip_storage.get('lineItems.csv')
                all_zips = {zr.zip_name for zr in work_storage}
                for zr in work_storage:
                    if any(zr.contains(f) for f in tempo):
                        fault_zips.add(zr.zip_name)
                # for z in fault_zips:
                #     print(f'fault:{z}')
                rest = all_zips - fault_zips
                for zip_name in fault_zips:
                    if settings.SSH_REMOVE_FAILED:
                        remote_file_path = f"{settings.SSH_REMOTE_UFOLDER}/{zip_name}"
                        sftp_client.remove(remote_file_path)
                        # print(f'failed: deleted {zip_name}')
                    else:    
                        sftp_client.rename(f"{settings.SSH_REMOTE_UFOLDER}/{zip_name}", 
                                        f"{settings.SSH_REMOTE_UFOLDER_FAILED}/{zip_name}")
                        # print(f"FAILED: Moved {zip_name} to faults.")
                for zip_name in rest:
                    if settings.SSH_REMOVE_SUCCESS:
                        remote_file_path = f"{settings.SSH_REMOTE_UFOLDER}/{zip_name}"
                        sftp_client.remove(remote_file_path)
                        # print(f' just delete {zip_name}: {settings.SSH_REMOVE_SUCCESS}')
                        
                    else:
                        try:
                            sftp_client.remove(f"{settings.SSH_REMOTE_UFOLDER_SUCCESS}/{zip_name}")
                            # print('removing only')
                        except IOError:
                            # print('new one')
                            pass
                        sftp_client.rename(f"{settings.SSH_REMOTE_UFOLDER}/{zip_name}", 
                                       f"{settings.SSH_REMOTE_UFOLDER_SUCCESS}/{zip_name}")
                        # print(f'just rename {settings.SSH_REMOTE_UFOLDER}/{zip_name}, {settings.SSH_REMOTE_UFOLDER_SUCCESS}/{zip_name}')
        # print(f'for {csv}: {entries}')














