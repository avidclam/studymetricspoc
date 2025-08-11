import jsonpath_ng

def extract_json_path(doc, json_path):
    json_path_parsed = jsonpath_ng.parse(json_path)
    if '*' not in json_path:
        matched = [match.value for match in json_path_parsed.find(doc)]
        result = matched[0] if matched else None
    else:
        result = [match.value for match in json_path_parsed.find(doc)]
    return result


class GeneralConfig:
    def __init__(self, data, default=None, alias_section='alias'):
        self.data = data
        self.alias = {}
        self.default = default if isinstance(default, dict) else {}
        self.alias_section = alias_section
        self._rebuild_alias()

    def get(self, alias_or_path=None, other=None):
        json_path = self.alias.get(alias_or_path, alias_or_path)
        if json_path is not None:
            result = extract_json_path(self.data, json_path)
        else:
            result = self.data
        if result is None:
            result = extract_json_path(self.default, json_path)
        return result if result is not None else other

    def get_dict(self, json_path):
        result = {}
        for dictionary in self.get(json_path, {}):
            for key, value in dictionary.items():
                result[key] = value
        return result

    def _rebuild_alias(self):
        if isinstance(self.data, dict):
            self.alias = {}
            if self.alias_section in self.default and isinstance(self.default[self.alias_section], dict):
                self.alias.update(self.default[self.alias_section])
            if self.alias_section in self.data and isinstance(self.data[self.alias_section], dict):
                self.alias.update(self.data[self.alias_section])

    def replace_data(self, data):
        self.data = data
        self._rebuild_alias()