import logging
import re
from collections import defaultdict

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
                    logger.info("FIELD: %s", json_dumps(field))
                    logger.info("KEYS: %s", json_dumps([x for x in field.keys()]))
                    raise
            else:
                raise Exception("Unknown field type %s: %s" % (field_type, json_dumps(field)))
        return fields

    def flatten_schema(self, schema):
        all_names = defaultdict(lambda: 0)
        return self._flatten_schema(schema, [], all_names)

    def remove_comments(self, string):
        return string[string.index("*/") + 2 :].strip().rstrip(";")

    def run_query(self, query, user):
        base_url = self.get_base_url()
        error = None
        query = self.remove_comments(query)
        logger.info("about to execute query (user='{}'): {}".format(user, query))
        resp = requests.post(base_url, params={'token': self.configuration['api_key'], 'default_limit': 1000},
                json={'query': query})
        if not resp.ok:
            return None, "Query failed (%d): %s" % (resp.status_code, resp.content)

        data = resp.json()
        fields = self.flatten_schema(data['schema']['children'])

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


register(IQL)
