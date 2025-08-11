from pathlib import Path
import yaml
from .generalconfig import GeneralConfig


YAML_NAME = 'process.yaml'
DEFAULT_CONFIG = {
    'config': {
        'cache': {
            'yaml': True,
            'alias': True,
            'query': True,
            'metric': True
        }
    }
}


def find_yaml_path(yaml_name):
    yaml_path = Path(yaml_name).absolute()
    while True:
        if yaml_path.exists():
            break
        elif yaml_path.parent.parent == yaml_path.parent:
            yaml_path = None
            break
        yaml_path = yaml_path.parent.parent / yaml_name
    return yaml_path


class ProcessConfig(GeneralConfig):
    def __init__(self, yaml_path=None):
        super().__init__(data={}, default=DEFAULT_CONFIG)
        if yaml_path is None:
            self.yaml_path = find_yaml_path(YAML_NAME)
            if self.yaml_path is None:
                raise OSError(f"Could not find YAML path: {YAML_NAME}")
        else:
            self.yaml_path = Path(yaml_path)
        if not self.yaml_path.exists():
            raise OSError(f"YAML path does not exist: {self.yaml_path}")
        self.update()
        self.config_cache_yaml = super().get('CACHE_YAML')

    def _get_without_update(self, alias_or_path=None, other=None):
        result = super().get(alias_or_path, other)
        return result if result is not None else other

    def _get_with_update(self, alias_or_path=None, other=None):
        self.update()
        return self._get_without_update(alias_or_path, other)

    def get(self, alias_or_path=None, other=None):
        if self.config_cache_yaml is None:
            return self._get_with_update(alias_or_path, other)
        else:
            return self._get_without_update(alias_or_path, other)

    def get_path(self, json_path):
        path = Path(self.get(json_path))
        return path if path.is_absolute() else self.yaml_path.parent / path

    def update(self):
        if self.yaml_path.is_dir():
            yaml_files = self.yaml_path.glob('*.yaml')
            content = '\n'.join(f.read_text(encoding='utf-8').strip() for f in yaml_files)
        else:
            content = self.yaml_path.read_text(encoding='utf-8')
        self.replace_data(yaml.safe_load(content))
        return self
