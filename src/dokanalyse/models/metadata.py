from datetime import datetime
from typing import Any, Dict, Self
from ..utils.helpers.common import parse_date_string


class Metadata:
    def __init__(
        self,
        dataset_id: str,
        title: str,
        description: str,
        owner: str,
        updated: datetime | None,
        dataset_description_uri: str
    ) -> None:
        self.dataset_id = dataset_id
        self.title = title
        self.description = description
        self.owner = owner
        self.updated = updated
        self.dataset_description_uri = dataset_description_uri

    def to_dict(self) -> Dict[str, Any]:
        return {
            'datasetId': self.dataset_id,
            'title': self.title,
            'description': self.description,
            'owner': self.owner,
            'updated': datetime.isoformat(self.updated) if self.updated is not None else None,
            'datasetDescriptionUri': self.dataset_description_uri
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> Self:
        dataset_id = data['datasetId']
        title = data['title']
        description = data['description']
        owner = data['owner']
        updated = data['updated']
        dataset_description_uri = data['datasetDescriptionUri']

        return cls(dataset_id, title, description, owner, parse_date_string(updated), dataset_description_uri)
