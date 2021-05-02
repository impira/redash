from collections import defaultdict
import logging
import re
import concurrent.futures

from redash.query_runner import *
from redash.utils import json_dumps, json_loads
from redash.utils.requests_session import requests

logger = logging.getLogger(__name__)
IQL_BASE_URL = "https://app.impira.com"

TYPES_MAP = {
    'BOOL': TYPE_BOOLEAN,
    'NUMBER': TYPE_INTEGER,
    'STRING': TYPE_STRING,
    'TIMESTAMP': TYPE_DATETIME,
    'INFERRED_FLOAT': TYPE_FLOAT,
    'INFERRED_STRING': TYPE_STRING,
    'FT_DNE': TYPE_STRING,
    None: TYPE_STRING,
}

class Field(object):
    def __init__(self, path, iql_type, all_names):
        self.path = path
        self.iql_type = iql_type
        self.name = '.'.join(['`' + x + '`' if ' ' in x else x for x in self.path])
        if self.name in all_names:
            all_names[self.name] += 1
            self.name = '%s (%d)' (self.name, all_names[self.name])

    def retrieve(self, data):
        try:
            curr = data
            for p in self.path:
                if not isinstance(curr, dict) or p not in curr:
                    return None
                curr = curr[p]
            if self.iql_type == "NUMBER" and isinstance(curr, float):
                self.iql_type = "INFERRED_FLOAT"

            if self.iql_type == "INFERRED_STRING":
                curr = json_dumps(curr)

            return curr
        except Exception:
            logger.info("Field: %s", json_dumps(self.path))
            logger.info("Data: %s", json_dumps(data))
            raise

    def type(self):
        return TYPES_MAP[self.iql_type]

def parse_comment(comment_string):
    return [x.strip() for x in comment_string.strip().lstrip('/*').rstrip('*/').split(',')]

def execute_iql_query(base_url, user, params, query):
    logger.info("about to execute  query (user='{}', params={}): {}".format(user, json_dumps(params), query))

    resp = requests.post(base_url, params=params, json={'query': query})
    if not resp.ok:
        return None, "Query failed (%d): %s" % (resp.status_code, resp.content)

    logger.info("finished executing query (user='{}', params={}): {}".format(user, json_dumps(params), query))

    return resp.json()

class IQL(BaseQueryRunner):
    noop_query = "@`__system::ecs` limit:0"

    @classmethod
    def name(cls):
        return "Impira Query Language (IQL)"

    @classmethod
    def configuration_schema(cls):
        return {
            "type": "object",
            "properties": {
                "org": {"type": "string", "title": "Organization name"},
                "api_key": {"type": "string", "title": "Impira API key"},
                "base_url": {"type": "string", "title": "(Optional) override the default base URL (including org)"},
            },
            "order": ["org", "api_key", "base_url"],
            "required": ["org", "api_key"],
            "secret": ["api_key"],
        }

    def __init__(self, configuration):
        super(IQL, self).__init__(configuration)
        self.syntax = "json"

    def get_base_url(self):
        if self.configuration.get('base_url', None) is not None:
            return self.configuration['base_url']
        else:
            return IQL_BASE_URL + '/o/' + requests.utils.quote(self.configuration['org']) + '/api/v2/iql'

    def _flatten_schema(self, schema, path, all_names):
        fields = []
        for field in schema:
            field_type = field.get('fieldType', None)
            full_path = path + [field['name']]
            if field_type in TYPES_MAP:
                fields.append(Field(full_path, field_type, all_names))
            elif field_type == 'ENTITY' and field['multiplicity'] == 'ONE_TO_MANY':
                fields.append(Field(full_path, "INFERRED_STRING", all_names))
            elif field_type == 'ENTITY':
                try:
                    fields.extend(self._flatten_schema(field.get('children', []), full_path, all_names))
                except Exception:
                    logger.info("Field: %s", json_dumps(field))
                    logger.info("Keys: %s", json_dumps([x for x in field.keys()]))
                    raise
            else:
                raise Exception("Unknown field type %s: %s" % (field_type, json_dumps(field)))
        return fields

    def flatten_schema(self, schema):
        all_names = defaultdict(lambda: 0)
        return self._flatten_schema(schema.get('children', []), [], all_names)

    @property
    def supports_auto_limit(self):
        return True

    def apply_auto_limit(self, query_text, should_apply_auto_limit):
        if should_apply_auto_limit:
            comment = '/* Limit: 1000 */'
            return comment + ' ' + query_text
        else:
            return query_text

    def parse_and_remove_comments(self, string):
        query = string
        metadata = {}
        while True:
            comment_index = query.find('*/')
            if comment_index < 0:
                break
            comment = query[:comment_index+2]
            query = query[comment_index+2:]

            for item in parse_comment(comment):
                if ':' in item:
                    key, value = item.split(':', 1)
                    metadata[key.strip()] = value.strip()

        return metadata, query.strip().rstrip(';')

    def build_params(self, metadata={}):
        params = {'token': self.configuration['api_key']}
        if 'Limit' in metadata:
            params['default_limit'] = metadata['Limit']
        return params

    def _execute_query(self, user, query, metadata={}):
        base_url = self.get_base_url()
        params = self.build_params(metadata)

        return execute_iql_query(base_url, user, params, query)

    def run_query(self, query, user):
        metadata, query = self.parse_and_remove_comments(query)

        data = self._execute_query(user, query, metadata)
        fields = self.flatten_schema(data['schema'])

        rows = []
        for row in data['data']:
            flattened = {}
            for field in fields:
                flattened[field.name] = field.retrieve(row)
            rows.append(flattened)

        columns = []
        for field in fields:
            columns.append(
                {'name': field.name, 'friendly_name': field.name, 'type': field.type()}
            )

        return json_dumps({'columns': columns, 'rows': rows}), None
    
    def get_schema(self, get_stats=False):
        ecs = [x['name'] for x in self._execute_query('get_schema', '@`__system::ecs`')['data']]

        base_url = self.get_base_url()
        params = self.build_params()

        def task(query):
            return execute_iql_query(base_url, 'get_schema', params, query)

        ret = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            future_to_ec = {executor.submit(task, '@`' + ec + '` limit:0'): ec for ec in ecs}
            for future in concurrent.futures.as_completed(future_to_ec):
                ec = future_to_ec[future]
                data = future.result()
                fields = self.flatten_schema(data['schema'])
                ret.append({'name': ec, 'columns': [f.name for f in fields]})
        logger.info("returning: %s", len(json_dumps(ret)))
        return ret

register(IQL)
