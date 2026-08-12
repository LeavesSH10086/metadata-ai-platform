import json
from abc import abstractmethod
from typing import List, Dict, Callable, Union, Any, Type
from typing_extensions import Protocol
from pathlib import Path
from pydantic import BaseModel
from pydantic.dataclasses import dataclass
from ruamel.yaml import YAML

yaml = YAML()

# **********************************************************************************************************************
# Helper
# **********************************************************************************************************************


def parse_single_doc(obj: dict, model: Type[BaseModel]) -> BaseModel:
    return model.model_validate(obj)


def parse_multi_doc(obj: List[dict], model: Type[BaseModel]) -> List[BaseModel]:
    return [model.model_validate(s) for s in obj]


def read_json(path: Path):
    with open(path) as f:
        return json.load(f)

# **********************************************************************************************************************
# Interface, Base Class
# **********************************************************************************************************************


class FileParser(Protocol):
    @abstractmethod
    def read(self, path: Path): ...

    @abstractmethod
    def parse(self, obj, model: BaseModel): ...


@dataclass
class DocumentHandler:
    reader: Callable[[Path], Any]
    parser: Union[Callable[[dict], BaseModel], Callable[[List[dict]], List[BaseModel]]]


class DocumentParser:
    handlers: Dict[str, DocumentHandler] = {

    }

    def __init__(self, doc_mode: str):
        self.doc_mode = doc_mode

        try:
            self.handler = self.handlers[self.doc_mode]
        except KeyError:
            raise KeyError(f'Unsupported doc mode: {self.doc_mode}')

    def read(self, path: Path):
        return self.handler.reader(path)

    def parse(self, obj, model: BaseModel):
        return self.handler.parser(obj, model)


# **********************************************************************************************************************
# Concrete Class
# **********************************************************************************************************************


class YAMLDocumentParser(DocumentParser):
    handlers = {
        'single': DocumentHandler(reader=yaml.load, parser=parse_single_doc),
        'multiple': DocumentHandler(reader=yaml.load_all, parser=parse_multi_doc)
    }


class JSONDocumentParser(DocumentParser):
    handlers = {
        'single': DocumentHandler(reader=read_json, parser=parse_single_doc),
    }


json_single_doc_parser = JSONDocumentParser(doc_mode='single')
yaml_single_doc_parser = YAMLDocumentParser(doc_mode='single')
yaml_multi_doc_parser = YAMLDocumentParser(doc_mode='multiple')
