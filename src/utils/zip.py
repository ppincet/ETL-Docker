import zipfile
import itertools
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
    all_files = sftp_conn.listdir_attr(settings.SSH_REMOTE_UFOLDER)
    zip_files = [f for f in all_files if f.filename.endswith('.zip')]
    zip_files.sort(key=lambda x: x.st_mtime, reverse=True)
    target_files = zip_files[:1]
    return [f.filename for f in target_files]

def perform_upsert(sf_client, data, settings):
    try:
        #clean_data = [dict(record) for record in data if isinstance(record, dict)]
        # sf_bulk_resource = getattr(sf_client.bulk2, settings['entity_api_name'])
        sf_bulk_resource = getattr(sf_client.bulk2, 'Learning')
        print(data)
        results = sf_bulk_resource.upsert(
            records=data, 
            #external_id_field=settings['ext_id_name']
            external_id_field='Composite_Source__c'
        )
        for row in results:
            print(f'row:{row}')
            if isinstance(row, dict) and not row.get('success'):
                print(f"Record failed: {row.get('errors')}")

    except Exception as e:
        print(f"Critical Upload Error: {e}")
        raise


def process_sftp_to_sf(sftp_client, sf_client, mappings):
    local_tmp = './tmp'
    remote_path = settings.SSH_REMOTE_UFOLDER
    os.makedirs(local_tmp, exist_ok=True)
    zip_files = get_latest_workload(sftp_client)
    
    data_buffers = {}
    print(mappings)
    for zip_name in zip_files:
        local_zip_path = os.path.join(local_tmp, zip_name)
        sftp_client.get(os.path.join(remote_path, zip_name), local_zip_path)
        
        with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
            zip_ref.extractall(local_tmp)
            csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv') and f not in constants.ETL_EXCLUDED_FILES]
            
            for csv_name in csv_files:
                data_buffers[csv_name] = []
                csv_map = mappings.get(csv_name, {})
                details = csv_map.get('details', [])
                external_settings = {
                    'ext_id_name': csv_map.get('header', {}).get('external_id_name'),
                    'entity_api_name': csv_map.get('header', {}).get('object_name')
                }
                
                csv_path = os.path.join(local_tmp, csv_name)
                
                with open(csv_path, mode='r', encoding='utf-8') as f:
                    header_line = f.readline()
                    if not header_line: continue
                    headers = header_line.strip().split(',')
                    
                    for line in f:
                        if not line.strip(): continue 
                        
                        row = dict(zip(headers, line.strip().split(',')))
                        mapped_row = map_data(details, row)
                        
                        data_buffers[csv_name].append(mapped_row)
                        
                        if len(data_buffers[csv_name]) >= settings.BUFFER_SIZE:
                            print('temp flush')
                            # perform_upsert(sf_client, data_buffers[csv_name], external_settings)
                            data_buffers[csv_name] = [] 

                if data_buffers[csv_name]:
                    # perform_upsert(sf_client, data_buffers[csv_name], external_settings)
                    print(f"✅ {csv_name} processed ({len(data_buffers[csv_name])} records)")
                    data_buffers[csv_name] = []
                os.remove(csv_path)
        os.remove(local_zip_path)
def map_data(fields, row):
    mapped_one = {}
    #print(f'row before:{row}')
    for field in fields:
        target_field = field.get('target')
        field_type = field.get('type')
        if field_type == 'CompositeKey':
            keys = field.get('compositeKeys', [])
            raw_value = ",".join([str(row.get(k, '')) for k in keys if row.get(k) is not None])
        else:
            source_key = field.get('source')
            raw_value = row.get(source_key, "")
        rules = field.get('rules', {})
        if rules:
            reversed_rules = {v: k for k, v in rules.items()}
            processed_value = reversed_rules.get(raw_value, raw_value)
        else:
            processed_value = raw_value
        complex_rules = field.get('complexRules', {})
        if complex_rules:
            final_value = complex_rules.get(processed_value, processed_value)
        else:
            final_value = processed_value
        if target_field:
            if isinstance(final_value, bool):
                final_value = str(final_value).lower()
            mapped_one[target_field] = final_value
    #print(f'mapped one:{mapped_one}')            
    return mapped_one














