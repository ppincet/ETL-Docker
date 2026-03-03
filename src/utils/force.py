from typing import Dict, Iterator, Any, List
from collections import defaultdict
import itertools
import time
from datetime import datetime
from zoneinfo import ZoneInfo
from utils import constants
from config import settings
import sys


_CACHE = {"mappings" : {"data": None, "expires_at": 0},
          "manifest" : {"data": None, "expires_at": 0},
          "dictionary" : {"data": None, "expires_at": 0}
          }
def get_existing_entries(sf, settings, unique_keys):
    all_results = []
    chunk_size = 400
    keys_list = [str(k).strip() for k in unique_keys if k]
    
    if not keys_list:
        return []

    ext_id = settings.get('extIdName')
    entity = settings.get('entityApiName')

    for i in range(0, len(keys_list), chunk_size):
        chunk = keys_list[i : i + chunk_size]
        formatted_chunk = ", ".join([f"'{str(k).replace("'", "\\'")}'" for k in chunk]) 
        soql = f"SELECT Id, {ext_id} FROM {entity} WHERE {ext_id} IN ({formatted_chunk})"
        print(f'soql:{soql}')
        db_stream = lazy_loading(sf, soql) 
        for rec in db_stream:
            if rec:
                all_results.append(rec)
            
    return all_results
def get_dictionaries(sf):
    '''
        returns back complex 1:n dictionaries
    '''
    dict_statement = '''
        SELECT 
            Field_Lookup__c,
            source__c,
            target__c,
            ETL_Entities_Mapping__r.DeveloperName
        FROM ETL_Dictionary__mdt
    '''
    result = defaultdict(dict)
    
    for rec in sf.query(dict_statement)['records']:
        entity_ref = rec.get('ETL_Entities_Mapping__r')
        entity = entity_ref.get('DeveloperName') if entity_ref else 'Global'
        lookup = rec['Field_Lookup__c']
        source = rec['source__c']
        target = rec['target__c']
        if lookup and source is not None:
            result[(entity,lookup)][source] = target
    return result
def get_manifest(sf):
    """
        Returns manifest scaffold
        sf configurable only
    """
    manifest_scaffold_statement = """
        SELECT
            Prefix__c,
            Property_Name__c,
            Value_text__c
        FROM ETL_manifest_fields__mdt 
        """
    system_results = {}
    results = []
    for rec in sf.query(manifest_scaffold_statement)['records']:
        entry = {
            'prefix': rec.get('Prefix__c'),
            'property': rec.get('Property_Name__c'),
            'value' : rec.get('Value_text__c')
        }
        if entry.get('prefix') == 'sf_system' :
            system_results[entry['property']] = entry['value']
        else:
            results.append(entry)
    return {'systemResultsInternal': system_results,
            'results' : results}

def log(sf, message, trace=""):
    """
    Provides log message
    """
    sf.User_Provisioning_Evt__e.create(message)
def get_mappings(sf):
    complex_dict = get_dictionaries(sf)
    """
        Returns mapping froms SF Metadata
        we dont need to implement generator - we are sure we have less than 2k recs
        todo - work with json as well
        
        cmdt doesn't work with the rels in where clause

    """

    if _CACHE['mappings']['data'] and (time.time() - _CACHE['mappings']['expires_at'] > settings.MAPPINGS_TTL):
        print('cached')
        return _CACHE["mappings"]["data"]
    mapping_statement = f"""
        SELECT 
            ETL_Entities_Mapping__r.DeveloperName,
            ETL_Entities_Mapping__r.Entity_Api_Name__c, 
            ETL_Entities_Mapping__r.File_Name__c,
            ETL_Entities_Mapping__r.Where_Clause__c,
            ETL_Entities_Mapping__r.External_Id_Name__c,
            ETL_Entities_Mapping__r.Is_details_source__c,
            ETL_Dictionary__r.Label_True__c,
            ETL_Dictionary__r.Label_False__c,
            ETL_Dictionary__r.JSON__c,
            ETL_Entities_Mapping__r.Operation_Type__c,
            ETL_Entities_Mapping__r.Direction__c,
            Source_Field_Type__c,
            Target_Field_Name__c, 
            Source_Field_Name__c,
            Excluded_from_header__c,
            Excluded_from_recordset__c,
            (select Source_Name__c, Order__c from ETL_Composite_keys__r order by Order__c )
        FROM ETL_Fields_Mapping__mdt
        """
    results = sf.query(mapping_statement)
    schema_map = {}
    for rec in results['records']:
        tempo_composite_keys = set()
        parent = rec.get('ETL_Entities_Mapping__r')
        if not parent: continue
        dictionary_ref = rec.get('ETL_Dictionary__r')
        composite_ref = rec.get('ETL_Composite_Keys__r')
        developer_name = parent.get('DeveloperName')

        direction = parent.get('Direction__c')
        if direction not in schema_map:
            schema_map[direction] = {}

        if developer_name not in schema_map[direction]:
            schema_map[direction][developer_name] = {"header" : {
                "file": parent.get('File_Name__c'),
                "where_cl" : parent.get('Where_Clause__c'),
                "external_id_name" : parent.get('External_Id_Name__c'),
                "is_details" : parent.get('Is_details_source__c'),
                "object_name" : parent.get('Entity_API_Name__c'),
                "rules" : {},
                "operation" : parent.get('Operation_Type__c'),
                "direction" : parent.get('Direction__c')
            },
                "details": []}
        source_field = rec.get('Source_Field_Name__c')
        source_field_type = rec.get('Source_Field_Type__c')
        target_header = rec.get('Target_Field_Name__c')
        is_recordset_only = rec.get('Excluded_from_header__c')
        is_header_only = rec.get('Excluded_from_recordset__c')

        if developer_name and source_field:
            field_rules ={}
            if dictionary_ref:
                field_rules = {
                    True: dictionary_ref.get('Label_True__c'),
                    False: dictionary_ref.get('Label_False__c')
                }
            complex_rules = complex_dict.get((developer_name,source_field), {})
            schema_map[direction][developer_name]["details"].append({
                    "source": source_field,
                    "target": target_header,
                    "type": source_field_type,
                    "is_recordset_only": is_recordset_only,
                    "is_header_only": is_header_only,
                    "compositeKeys": [],
                    "rules" : field_rules,
                    "complexRules" : complex_rules,
            })
            if dictionary_ref:
                schema_map[direction][developer_name]["header"]["rules"][source_field] = {
                    True: dictionary_ref.get('Label_True__c'),
                    False: dictionary_ref.get('Label_False__c'),
                    dictionary_ref.get('Label_True__c'): True,
                    dictionary_ref.get('Label_False__c') : False,
                }
            if composite_ref:
                raw_keys = composite_ref.get('records', [])
                raw_keys.sort(key=lambda x: float(x.get('Order__c') or 0))
                tempo_composite_keys = [key.get('Source_Name__c') for key in raw_keys]
                schema_map[direction][developer_name]["details"][-1]["compositeKeys"] = tempo_composite_keys
    _CACHE['mappings']['data'] = schema_map
    _CACHE['mappings']['expires_at'] = time.time() + settings.MAPPINGS_TTL
    return _CACHE['mappings']['data']

def get_junctions(sf):
    '''
        we should provide Master FK as `Excluded from header` if we don't wanna reflect it
        but need to get joined recordset
    ''' 
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
            'source' : rec['Details_Source__r'].get('DeveloperName'),
            'fk' : rec.get('Master_FK__c')
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
        stamp = rec.get('Stamp__c')
        if entity not in watermarks:
            watermarks[entity] = stamp if stamp else '1970-01-01T00:00:00.000+0000'
    return watermarks
def get_results(sf):
    gen_map: Dict[tuple, List[Iterator[str]]] = defaultdict(list)
    gen_scaffolds = get_gen_scaffolds(sf)

    def generate_delta(source, contents):
        master = {}
        id_field = contents["fk"].lower()
        
        # populating master
        #watermark =  gen_scaffolds[source]['wm'] or "1900-01-01T00:00:00.000+0000"
        #print('inside master')
        source_set = gen_scaffolds[source]['fields'].get('details',[])
        target_set = gen_scaffolds[contents["source"]]['fields'].get('details',[])
        #dict_ref = gen_scaffolds[source]['fields']['header'].get('rules')
        fields = sorted(itertools.chain(source_set, target_set), key=lambda x: x['target'])
   
        db_stream = lazy_loading(sf, gen_scaffolds[source]['soql'])
        context = {
           "fields": fields,
            "rules": gen_scaffolds[source]['fields']['header'].get('rules'),
            "watermark": gen_scaffolds[source]['wm'] or "1900-01-01T00:00:00.000+0000"
        
        }
   
        try:
            first_record = next(db_stream)
        except StopIteration:
            return
        # header
        # yield (','.join([*[f['target'] for f in fields], 'status']) + '\n', None)
        yield (','.join([*[f['target'] for f in fields], 'dateLastModified\n']), None)
        
        for rec in itertools.chain([first_record], db_stream):
            master_key = flatten_record(rec).get(id_field) #full record
            master[master_key] = rec
             
        for rec in lazy_loading(sf, gen_scaffolds[contents["source"]]['soql']):
            # always id in terms of sf
            if rec.get('id') in master:
                # here to add createdDate field
                yield (format_row(master[rec.get('id')] | rec, context), 
                        master[rec['id']].get('systemmodstamp'))
                # print(f'rec:{rec.get("id")}')
                # print (format_row(master[rec.get('id')] | rec, fields, watermark), 
                #         master[rec['id']].get('systemmodstamp'))
    for dev_name, contents in get_junctions(sf).items():
        h = gen_scaffolds[dev_name]['fields'].get('header')
        entity_tuple = (h['file'], h['object_name'])
        gen_map[entity_tuple].append(generate_delta(dev_name, contents))
        h['is_details'] = True
    for dev_name, contents in gen_scaffolds.items():
        h = contents['fields']['header']
        if h.get('is_details'):
            continue
        watermark =  gen_scaffolds[dev_name]['wm'] or "1900-01-01T00:00:00.000+0000"
        fields = contents['fields']['details']
        header = contents['fields']['header']
        main_context = {
            "soql" :  contents['soql'],
            "fields" : contents['fields']['details'],
            "header" : contents['fields']['header'],
            "watermark" : watermark,
            "rules" : gen_scaffolds[dev_name]['fields']['header'].get('rules')
        }
        del_context = {
            "fields" : fields,
            "object_name" : header['object_name'], 
            "external_id" : header['external_id_name'],
            "watermark" : watermark,
            "rules" : gen_scaffolds[dev_name]['fields']['header'].get('rules')
        }
        composed_gen = itertools.chain(csv_row_generator(sf, main_context), 
                                       del_generator(sf, 
                                                     fields, 
                                                     header['object_name'], 
                                                     header['external_id_name'], 
                                                     watermark))
        entity_tuple = (h['file'], h['object_name'])
        gen_map[entity_tuple].append(composed_gen)
    return gen_map

def get_gen_scaffolds(sf):
    try:
        watermarks = get_watermarks(sf)
        gen_scaffolds = {}
        # print('scaffs before')
        # print(f'mappings:{get_mappings(sf)['Forth']}')
        for developer_name, fields in get_mappings(sf)['Forth'].items():
            if not fields:
                continue
            # print(f'after:{developer_name}')
            custom_clause = fields['header'].get('where_cl')
            is_details = fields['header'].get('is_details')
            source_fields = [f['source'] for f in fields['details'] if not f.get('is_header_only')]
            tech_fields = ['Id']
            if not is_details:
                tech_fields.extend(['SystemModStamp', 
                                    'CreatedDate', 
                                    'CreatedBy.TimeZoneSidKey',
                                    'LastModifiedDate',
                                    ])
            final_fields = list(dict.fromkeys(f.lower() for f in source_fields + tech_fields))
            object_name = fields['header'].get('object_name')
            watermark = None if is_details else watermarks[object_name]
            filters = [f"SystemModStamp > {watermark}" if watermark and not is_details else None, 
                        f"({custom_clause})" if custom_clause else None,
                        f"(LastModifiedById != '{settings.INTEGRATION_USER_ID}')"]
            where_statement = "WHERE " + " AND ".join(filter(None, filters)) if any(filters) else ""
            soql = f"""
                SELECT {', '.join(final_fields)} 
                FROM {object_name}  
                {where_statement}
                ORDER BY SYSTEMMODSTAMP ASC
            """
            fields['details'] = [f for f in fields['details'] if not f.get('is_recordset_only')]
            gen_scaffolds[developer_name] = {
                "soql" : soql,
                "fields" : fields,
                "wm" : watermark or "1900-01-01T00:00:00.000+0000"
            }
    except Exception as e :
        print(f'❌from scaffs:{e}')
    
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


def format_row(rec, context):
    row = []
    last_modified_date = rec.get('lastmodifieddate') # lowered hardly
    
    if last_modified_date and last_modified_date.endswith('+0000'):
        last_modified_date = last_modified_date.replace("+0000", "Z")
    adjusted_rec = adjust_date(rec.copy(),['createddate']) # we need to adjust only created date not last modified
    rules = context['rules'] # rules reflect true/false for active/inactive
    for f in context['fields']:  
        raw_val = adjusted_rec.get(f['source'].lower())
        if f['type'] == constants.ETL_BOOLEAN_TYPE:
            field_rule = rules.get(f['source'], {})
            if raw_val is not None:
                raw_val = field_rule.get(raw_val, raw_val)
        #here to replace with cmdt solution
        row.append(raw_val if f['target'] != '---' else "")
    row.append(last_modified_date)
    # do not delete - tempo solution
    # row.append('U' if watermark > created_date else 'C')
    return ",".join([str(x) if x is not None else "" for x in row]) + '\n'

def adjust_date(rec, fields):
    '''    
    :param rec: record to get adjusted
    :param fields: list of fields to get adjusted according to timezone
    '''
    # we should provide at least one field with timezonesidkey
    tz = rec.get('createdby.timezonesidkey')
    
    for field in fields:
        if tz and rec.get(field):
            try:
                utc_dt = datetime.fromisoformat(rec[field])
                local_dt = utc_dt.astimezone(ZoneInfo(tz))
                rec[field] = local_dt.strftime('%Y-%m-%d')       
            except Exception as e:
                # replace with default from .env
                print(f"Skipping conversion for {rec.get('id')}: {e}")
    return rec

def csv_row_generator(sf, context, include_header = True):
    db_stream = lazy_loading(sf, context["soql"]) 
    try:
        first_record = next(db_stream)
    except StopIteration:
        return
    ordered_fields = sorted(context["fields"], key=lambda x: x['target'])

    if include_header is True:
        # as per jan,14 2026
        #yield (",".join([*[f['target'] for f in ordered_fields], 'status']) + '\n', None)
        yield (','.join(f['target'] for f in ordered_fields) + ',dateLastModified\n', None)
        ctx = {
           "fields": ordered_fields,
            "rules": context["rules"],
            "watermark": context["watermark"]
        }
    yield (format_row(first_record, ctx), first_record['systemmodstamp'])
    for rec in db_stream:
        yield (format_row(rec, ctx), rec.get('systemmodstamp'))

def del_generator(sf, fields, object_name, ext_field, watermark):
    return
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
        # print(f'test from del gen:{rec}:{(id)}')
        if not id: continue
        row = []
        for item in ordered_fields:
            row.append(id if item['source'].lower() ==  ext_field else '')
        row.append('D')
        print((",".join(row) + '\n', rec.get('createddate')))
        yield (",".join(row) + '\n', rec.get('createddate'))
  #  print('------ etx del ------')
def flatten_record(record):
    """
    Flattens a nested dictionary iteratively (no recursion).
    Converts keys to lowercase and joins nested keys with '.'.
    Removes 'attributes' keys.
    """
    #print(f'before flat one:{record}')
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
    updates - we need to manage with both CU & D updates
    """      

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

def get_ids(sf, unique_keys, object_name, field_name):
    '''
        returns map with chunks to avois sf soql statement size limit 1
    '''
    id_map = {}
    keys_list = list(unique_keys)
    chunk_size = 500 
    for i in range(0, len(keys_list), chunk_size):
        chunk = keys_list[i : i + chunk_size]
        formatted_keys = "('" + "','".join(chunk) + "')"
        query = f"SELECT Id, {field_name} FROM {object_name} WHERE {field_name} IN {formatted_keys}"      
        try:
            results = sf.query(query)
            for record in results['records']:
                key_value = record[field_name].lower()
                id_map[key_value] = record['Id']
        except Exception as e:
            print(f"Error querying chunk starting at {i}: {e}")
            raise
    return id_map