from typing import Dict, Iterator, Any, List
from collections import defaultdict
import itertools
def log(sf, message, trace=""):
    """
    Provides log message
    """
    sf.User_Provisioning_Evt__e.create(message)
def get_mappings(sf):
    """
        Returns mapping froms SF Metadata

        we dont need to implement generator - we are sure we have less than 2k recs
    """
    mapping_statement = """
        SELECT 
            ETL_Entities_Mapping__r.DeveloperName,
            ETL_Entities_Mapping__r.Entity_Api_Name__c, 
            ETL_Entities_Mapping__r.File_Name__c,
            ETL_Entities_Mapping__r.Where_Clause__c,
            ETL_Entities_Mapping__r.External_Id_Name__c,
            ETL_Entities_Mapping__r.Is_details_source__c,
            Target_Field_Name__c, 
            Source_Field_Name__c 
        FROM ETL_Fields_Mapping__mdt
        """
    results = sf.query(mapping_statement)
    schema_map = {}
    for rec in results['records']:
        parent = rec.get('ETL_Entities_Mapping__r')
        developer_name = parent.get('DeveloperName')
        if developer_name not in schema_map:
            schema_map[developer_name] = {"header" : {
                "file": parent.get('File_Name__c'),
                "where_cl" : parent.get('Where_Clause__c'),
                "external_id_name" : parent.get('External_Id_Name__c'),
                "is_details" : parent.get('Is_details_source__c'),
                "object_name" : parent.get('Entity_API_Name__c')
            },
                "details": []}
        if not parent: continue
        source_field = rec.get('Source_Field_Name__c')
        target_header = rec.get('Target_Field_Name__c')
        if developer_name and source_field:
            schema_map[developer_name]["details"].append({
                    "source": source_field,
                    "target": target_header
            })
    return schema_map
def get_junctions(sf):
    soql = '''
        SELECT 
            Master_source__r.developerName,
            details_source__r.developerName,
            Master_FK__c
        FROM ETL_Join__mdt
    '''
    junctions = {}
    for rec in sf.query(soql)['records']:
        master = rec['Master_Source__r']['DeveloperName']
        if  master not in junctions:
            junctions[master] = []
        junctions[master] = { 
            'source' : rec['Details_Source__r']['DeveloperName'],
            'fk' : rec['Master_FK__c']
        }
    return junctions
def get_watermarks(sf):
    """
    Return watermarks from sf
    """
    wm_statement = """
        SELECT 
            Entity_API_Name__c,
            Stamp__c
        FROM Watermark__c
    """
    results = sf.query(wm_statement)
    watermarks = {}
    for rec in results['records']:
        entity = rec.get('Entity_API_Name__c')
        if entity not in watermarks:
            watermarks[entity] = rec.get('Stamp__c')
    return watermarks
def get_results(sf):
    gen_map: Dict[tuple, List[Iterator[str]]] = defaultdict(list)
    gen_scaffolds = get_gen_scaffolds(sf)
    def generate_delta(source, contents):
        print('generate delta')
        master = {}
        id_field = contents["fk"].lower()
        # populating master

        watermark =  gen_scaffolds[source]['wm'] or "1900-01-01T00:00:00.000+0000"
        source_set = gen_scaffolds[source]['fields'].get('details',[])
        target_set = gen_scaffolds[contents["source"]]['fields'].get('details',[])
        fields = sorted(itertools.chain(source_set, target_set), key=lambda x: x['target'])
        # db_stream = lazy_loading(sf, gen_scaffolds[source]['soql'])
        # try:
        #     first_record = next(db_stream)
        # except StopIteration:
        #     return
        yield (','.join([*[f['target'] for f in fields], 'status']) + '\n', None)
        for rec in lazy_loading(sf, gen_scaffolds[source]['soql']):
            master_key = flatten_record(rec).get(id_field)
            master[master_key] = rec
        for rec in lazy_loading(sf, gen_scaffolds[contents["source"]]['soql']):
            # always id in terms of sf
            if rec.get('id') in master:
                yield (format_row(master[rec.get('id')] | rec, fields, watermark), 
                        master[rec['id']].get('systemmodstamp'))
                # print(f'rec:{rec.get("id")}')
                # print (format_row(master[rec.get('id')] | rec, fields, watermark), 
                #         master[rec['id']].get('systemmodstamp'))
    for dev_name, contents in get_junctions(sf).items():
        h = gen_scaffolds[dev_name]['fields'].get('header')
        entity_tuple = (h['file'] + '.csv', h['object_name'])
        gen_map[entity_tuple].append(generate_delta(dev_name, contents))
        h['is_details'] = True
    for dev_name, contents in gen_scaffolds.items():
        h = contents['fields']['header']
        if h.get('is_details'):
            continue
        # if dev_name != 'User_Student':
        #     continue
        watermark =  gen_scaffolds[dev_name]['wm'] or "1900-01-01T00:00:00.000+0000"
        soql = contents['soql']
        fields = contents['fields']['details']
        header = contents['fields']['header']
        composed_gen = itertools.chain(csv_row_generator(sf, soql, fields, watermark), 
                                       del_generator(sf, 
                                                     fields, 
                                                     header['object_name'], 
                                                     header['external_id_name'], 
                                                     watermark))
        entity_tuple = (h['file'] + '.csv', h['object_name'])
        gen_map[entity_tuple].append(composed_gen)
    return gen_map

def get_gen_scaffolds(sf):
    watermarks = get_watermarks(sf)
    gen_scaffolds = {}
    for developer_name, fields in get_mappings(sf).items():
        if not fields:
            continue
        custom_clause = fields['header'].get('where_cl')
        is_details = fields['header'].get('is_details')
        source_fields = [f['source'] for f in fields['details'] if f['source'] != '---']
        tech_fields = ['Id']
        if not is_details:
            tech_fields.extend(['SystemModStamp', 'CreatedDate'])
        final_fields = list(dict.fromkeys(f.lower() for f in source_fields + tech_fields))
        object_name = fields['header'].get('object_name')
        watermark = None if is_details else watermarks[object_name]
        filters = [f"SystemModStamp > {watermark}" if watermark and not is_details else None, f"({custom_clause})" if custom_clause else None]
        where_statement = "WHERE " + " AND ".join(filter(None, filters)) if any(filters) else ""
        soql = f"""
            SELECT {', '.join(final_fields)} 
            FROM {object_name}  
            {where_statement}
            ORDER BY SYSTEMMODSTAMP ASC
        """
        gen_scaffolds[developer_name] = {
            "soql" : soql,
            "fields" : fields,
            "wm" : watermark or "1900-01-01T00:00:00.000+0000"
        }

    return gen_scaffolds

def lazy_loading(sf, soql_statement):
    """
        Records generator
    """
    results = sf.query(soql_statement)
    done = results['done']
    for rec in results['records']:
        yield flatten_record(rec)
    while not done:
        next_records_url = results['nextRecordsUrl']
        results = sf.query_more(next_records_url, identifier_is_url=True)
        done = results['done']
        for rec in results['records']:
            yield flatten_record(rec)

def format_row(rec, fields, watermark):
    row = []
    for f in fields:
        row.append(rec.get(f['source'].lower()) if f['target'] != '---' else "")
    row.append('U' if watermark > rec.get('createddate') else 'C')
    return ",".join([str(x) if x is not None else "" for x in row]) + '\n'

def csv_row_generator(sf, soql_statement, fields, watermark, include_header = True):
    db_stream = lazy_loading(sf, soql_statement) 
    try:
        first_record = next(db_stream)
    except StopIteration:
        return
    ordered_fields = sorted(fields, key=lambda x: x['target'])

    if include_header is True:
        yield (",".join([*[f['target'] for f in ordered_fields], 'status']) + '\n', None)

    yield (format_row(first_record, ordered_fields, watermark), first_record['systemmodstamp'])
    for rec in db_stream:
        yield (format_row(rec, ordered_fields, watermark), rec.get('systemmodstamp'))

def del_generator(sf, fields, object_name, ext_field, watermark):
    soql_statement = f"""
        SELECT External_Id__c, createddate
        FROM Deletion_Log__c
        WHERE Entity_Api_Name__c = '{object_name}'
            and CreatedDate > {watermark}
    """
    ordered_fields = sorted(fields, key=lambda x: x['target'])
    # external_field = fields[0].get('external_id_name').lower()
    for rec in lazy_loading(sf, soql_statement):
        id = rec.get('external_id__c') 
        if not id: continue
        row = []
        for item in ordered_fields:
            row.append(id if item['source'].lower() ==  ext_field else '')
        row.append('D')
        #print((",".join(row) + '\n', rec.get('createddate')))
        yield (",".join(row) + '\n', rec.get('createddate'))

def flatten_record(record):
    """
    Flattens a nested dictionary iteratively (no recursion).
    Converts keys to lowercase and joins nested keys with '.'.
    Removes 'attributes' keys.
    """
    flat_record = {}
    stack = [(record, '')]
    while stack:
        current_dict, prefix = stack.pop()
        
        for k, v in current_dict.items():
            if k == 'attributes':
                continue
            key_name = f"{prefix}{k}".lower()
            if isinstance(v, dict):
                stack.append((v, f"{key_name}."))
            else:
                flat_record[key_name] = v         
    return flat_record

def upsert_wm(sf, wm):
    """
    Upserts watermark records one-by-one using the External ID.
    """      
    print(f'watermarks:{wm}')
    success_count = 0
    for entity_name, max_date in wm.items():
        if not max_date:
            continue
        ext_id_field = 'Entity_API_Name__c' 
        payload = {
            'Stamp__c': max_date
        }
        record_key = f"{ext_id_field}/{entity_name}"
        try:
            sf.Watermark__c.upsert(record_key, payload)
            success_count += 1
            print(f" [OK] {entity_name}: {max_date}")
            
        except Exception as e:
            print(f" [ERR] Failed to update {entity_name}: {e}")

    print(f"--- Watermark Sync Complete. Success: {success_count}/{len(wm)} ---")
