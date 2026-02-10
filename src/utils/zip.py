import zipfile
import itertools
from utils import force, constants
from collections import defaultdict

def upload_file(sf, zip_filename,wm):
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
    #print(f'total:{total_size} vs header:{header_size}')
# except Exception as e:
    # print(f'exc: {e}')
    return constants.ETL_SUCCESS if total_size > header_size else constants.ETL_EMPTY
















