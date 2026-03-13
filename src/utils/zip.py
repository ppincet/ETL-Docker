import zipfile
import itertools
import time
from datetime import datetime
from utils import force, constants
from collections import defaultdict
import os
from config import settings

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

def get_latest_workload(sftp_conn):
    '''
    2do - add watermark
    System_SFTP - api name
    '''
    # print(f'current_dir: {sftp_conn.normalize('.')}')
    all_files = sftp_conn.listdir_attr(settings.SSH_REMOTE_UFOLDER)
    zip_files = [f for f in all_files if f.filename.endswith('.zip')]
    zip_files.sort(key=lambda x: x.st_mtime, reverse=True)
    target_files = zip_files[:1]
    return [f.filename for f in target_files]

def perform_upsert(sf_client, data, settings):
    formatted_data = []
    for item in data:
        raw_name = item.get('Name')
        if raw_name and str(raw_name).strip():
            formatted_name = raw_name
        else:
            formatted_name = f'Learning - {datetime.now().strftime("%Y%m%d-%H%M%S")}'
        record = {
            'Name': formatted_name,
            'CIPCODE': item.get('external_source_id__c'),
            'Composite_Source__c': f"{formatted_name},{item.get('external_source_id__c')}",
            'isActive': item.get('Active_Custom__c'),
            'Type': 'LearningCourse'
        } 
        formatted_data.append(record)
    
    try:
        
        # print(f'upsert is ready with {formatted_data}')
        print(f'external settings:{settings}')
        external_id = 'Composite_Source__c' 
        sf_bulk_resource = getattr(sf_client.bulk, 'Learning')
        results = sf_bulk_resource.upsert(formatted_data, external_id)
        # print(f'from upsert:{results}')
        for i, item in enumerate(results):
            if item.get('success'): 
                data[i]['LearningId'] = item.get('id')
                data[i]['CourseNumber'] = data[i].get('external_source_id__c') 
            else:
                data[i]['LearningId'] = None
        #print(f'data from upsert:{data[0]}')
        sf_bulk_resource = getattr(sf_client.bulk, settings['entityApiName'])
        results = sf_bulk_resource.upsert(data, settings['extIdName'])

        print(f'final results:{results}')
        #return data
        # while True:

        
        # job = sf_bulk_resource.upsert(
        #     records=formatted_data, 
        #     external_id_field='Composite_Source__c'
        # )
        
        # if isinstance(job, list):
        #     job_id = job[0]['job_id']
        # else:
        #     job_id = job['job_id']

        # session = sf_client.bulk2.session
        # headers = sf_client.bulk2.headers
        # base_url = sf_client.base_url + "jobs/ingest/" + job_id      
        # failed_results_url = f"{sf_client.base_url}jobs/ingest/{job_id}/failedResults"
        # successfulResults_url = f"{sf_client.base_url}jobs/ingest/{job_id}/successfulResults"

        # while True:
        #     response = session.get(base_url, headers=headers)
        #     status_data = response.json()
        #     if status_data['state'] in ['JobComplete', 'Failed', 'Aborted']:
        #      break
        #     time.sleep(5)
        # if status_data['state'] == 'JobComplete':
        #     print('before getting the results')
        #     #response = sf_client.bulk2.session.get(failed_results_url, headers=sf_client.bulk2.headers)
        #     response = sf_client.bulk2.session.get(successfulResults_url, headers=sf_client.bulk2.headers)
        #     print(response.text)
        #     # results = sf_client.bulk2.get_upsert_results('Learning', job_id)
        #     # print('after getting the results')
        #     # for row in results:
        #     #     print(f'row:{row.get('success')}')
        #     #     if isinstance(row, dict) and not row.get('success'):
        #     #         print(f"Record failed: {row.get('errors')}")

    except Exception as e:
        print(f"Critical Upload Error: {e}")
        raise


def process_sftp_to_sf(sftp_client, sf_client, mappings):
    local_tmp = './tmp'
    remote_path = settings.SSH_REMOTE_UFOLDER
    os.makedirs(local_tmp, exist_ok=True)
    zip_files = get_latest_workload(sftp_client)
    #print(f'zip files:{zip_files}')
    data_buffers = {}
    for zip_name in zip_files:
        local_zip_path = os.path.join(local_tmp, zip_name)
        sftp_client.get(os.path.join(remote_path, zip_name), local_zip_path)
        
        with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
            zip_ref.extractall(local_tmp)
            csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv') and f not in constants.ETL_EXCLUDED_FILES]
            
            for csv_name in csv_files:
                csv_map = mappings.get(csv_name, {})
                external_settings = {
                    'extIdName': csv_map.get('header', {}).get('external_id_name'),
                    'entityApiName': csv_map.get('header', {}).get('object_name'),
                }
                print(f'csv map:{csv_map}')
                data_buffers[csv_name] = []
                details = csv_map.get('details', [])
                csv_path = os.path.join(local_tmp, csv_name)
                # getting natural keys to retreive sf ids
                # match csv_name:
                #     case 'lineItems.csv':
                #         continue
                #     case 'reluts.csv':
                #         continue
                #         #if csv_name != 'results.csv': continue
                ext_id_field = external_settings['extIdName']
                with open(csv_path, mode='r', encoding='utf-8') as f:
                    # here to provide confugirable
                    header_line = f.readline()
                    if not header_line: continue
                    headers = [h.strip() for h in header_line.split(',')]
                    unique_keys = set()
                    for line in f:
                        if not line.strip(): continue
                        row = dict(zip(headers, line.strip().split(',')))
                        #print(f'row for {csv_name}:{row}')
                        entity = str(map_data(details, row).get(ext_id_field) or '').strip()
                        if entity: unique_keys.add(entity)
                    # here to check upsert
                    if csv_map.get('header', {}).get('operation') == 'update':
                        results = force.get_existing_entries(sf_client, external_settings, unique_keys)
                    print(f'results (existing ones) from zip for {csv_name}:{results}')
                    #  used for updating each record with id (if found)
                    id_lookup = {str(rec.get(ext_id_field.lower())): rec.get('id') for rec in results}
                    #print(f'id lookup:{id_lookup}')
                    f.seek(0)
                    f.readline()
                    #with open(csv_path, mode='r', encoding='utf-8') as f:
                    # header_line = f.readline()
                    #     if not header_line: continue
                    #     headers = header_line.strip().split(',')
                        
                    for line in f:
                        if not line.strip(): continue 
                        
                        row = dict(zip(headers, line.strip().split(',')))
                        mapped_row = map_data(details, row)
                        #print(f'mapped one from {csv_name}: {mapped_row}')
                        lookup_key = str(mapped_row.get(external_settings['extIdName']))
                        lookup_value = id_lookup.get(lookup_key)
                        if lookup_value :
                            mapped_row['id'] = lookup_value
                        if csv_name != constants.ETL_UNE_RESULTS or lookup_value: 
                            data_buffers[csv_name].append(mapped_row)
                        
                        if len(data_buffers[csv_name]) >= settings.BUFFER_SIZE:
                            print('temp flush')
                            match csv_name:
                                case 'results.csv':
                                    print('inside results')
                                case 'lineItems.csv':
                                    tempo = perform_upsert(sf_client, data_buffers[csv_name], external_settings)
                                    #print('managin lineItems(for update main)')
                                    # upsert 
                                    # for item in tempo:
                                    #     print(f'result from line items(main): {item}')
                                    
                            #perform_upsert(sf_client, data_buffers[csv_name], external_settings)
                            data_buffers[csv_name] = [] 
                    #print(f'data buffers for {csv_name}: {data_buffers[csv_name]}')
                    if data_buffers[csv_name]:
                        # for record in data_buffers[csv_name]:
                        #             print(f'final results for {csv_name}:{record}')
                        match csv_name:
                            case 'results.csv':
                                # we should perform update here!
                                
                                force.perform_update(sf_client, data_buffers[csv_name], external_settings)    
                            case 'lineItems.csv':
                                #force.perform_update(sf_client, data_buffers[csv_name], external_settings)    
                                tempo = perform_upsert(sf_client, data_buffers[csv_name], external_settings)
                                print('managin lineItems(for update flush)')
                                for item in tempo:
                                    print(f'result from line items(flush): {item}')
                                
                    print(f"✅ {csv_name} processed ({len(data_buffers[csv_name])} records)")
                    data_buffers[csv_name] = []
                    #os.remove(csv_path)
        #os.remove(local_zip_path)
def map_data(fields, row):
    mapped_one = {}
    #gotta_simple = False
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
            #gotta_simple = True
            reversed_rules = {v: k for k, v in rules.items()}
            processed_value = reversed_rules.get(raw_value, raw_value)
            
            # print(f'simple rules:{processed_value}')
            # print(f'target:{target_field} with {field_type} type')
        else:
            processed_value = raw_value
            #gotta_simple = False
        complex_rules = field.get('complexRules', {})
        if complex_rules:
            # print(f'gotta simple:{gotta_simple}')
            # print(f'complex rules:{complex_rules}')
            # print(f'current proccessed value from complex:{processed_value} vs {raw_value}')
            # print(f'field:{field}')
            final_value = complex_rules.get(raw_value)
            # print(f'complex rules value:{final_value} vs {complex_rules.get(processed_value, processed_value)}')
        else:
            final_value = processed_value
        if target_field:
            if isinstance(final_value, bool):
                final_value = str(final_value).lower()
            mapped_one[target_field] = final_value      
    return mapped_one














