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
    all_files = sftp_conn.listdir_attr(settings.SSH_REMOTE_UFOLDER)
    zip_files = [f for f in all_files if f.filename.endswith('.zip')]
    zip_files.sort(key=lambda x: x.st_mtime, reverse=True)
    target_files = zip_files[:5]
    return [f.filename for f in target_files]
def process_sftp_to_sf(sftp_client, sf_client):
    buffer = [] 
    local_tmp='./tmp'
    remote_path = settings.SSH_REMOTE_UFOLDER
    os.makedirs(local_tmp, exist_ok=True)
    zip_files = get_latest_workload(sftp_client)
    for zip_name in zip_files:
        print(f'zip to process:{zip_name}')
        local_zip_path = os.path.join(local_tmp, zip_name)
        sftp_client.get(os.path.join(remote_path, zip_name), local_zip_path)
        with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
            zip_ref.extractall(local_tmp)
            csv_files = [f for f in zip_ref.namelist() if f.endswith('.csv') and f not in constants.ETL_EXCLUDED_FILES]
            
            for csv_name in csv_files:
                print(f'csv filename:{csv_name}')
                csv_path = os.path.join(local_tmp, csv_name)
                with open(csv_path, mode='r', encoding='utf-8') as f:
                    header_line = f.readline()
                    if not header_line:
                        continue
                    headers = header_line.strip().split(',')
                    for line in f:
                        if not line.strip(): 
                            continue 
                        values = line.strip().split(',')
                        
                        row = dict(zip(headers, values))
                        #print(f'row contents: {row}')
                        # mapped_row = map_data_to_sf(row) 
                        #buffer.append(mapped_row)

            
                        # if len(buffer) >= settings.BUFFER_SIZE:
                        #     perform_upsert(sf_client, buffer)
                        #     buffer.clear() # Memory-efficient clearing

                # CLEANUP: Remove CSV after reading
                os.remove(csv_path)

        # CLEANUP: Remove Local Zip & SFTP Zip after full processing
        os.remove(local_zip_path)
        #sftp_client.remove(os.path.join(remote_path, zip_name))
        print(f"Successfully processed and deleted: {zip_name}")

    # 4. THE "FLUSH": Handle leftovers (e.g., the last 300 records)
    # if buffer:
    #     perform_upsert(sf_client, buffer)
    #     print(f"Final flush of {len(buffer)} records complete.")

def perform_upsert(sf_client, data):
    """
    Wrapper for your Salesforce Bulk API call.
    Uses External_ID__c to prevent duplicates if a re-run occurs.
    """
    try:
        # Example using simple-salesforce bulk interface
        sf_client.bulk.Your_Object__c.upsert(data, 'External_ID__c', batch_size=2000)
    except Exception as e:
        print(f"Critical Upload Error: {e}")
        # In a real app, you'd want to log this to a DB to retry later
        raise















