from socketio import SimpleClient
from enum import Enum


class StateStatus(str, Enum):
    STARTING_UP = 1
    ANALYZING_DATASETS = 2
    DATASET_ANALYZED = 3
    CREATING_FACT_SHEET = 4
    CREATING_MAP_IMAGES = 5
    MAP_IMAGE_CREATED = 6,
    CREATING_REPORT = 7,
    NONE = -1


class StateEmitter():
    def __init__(self, correlation_id: str | None, sio_client: SimpleClient | None) -> None:
        self.__correlation_id = correlation_id
        self.__sio_client = sio_client
        self.__status: StateStatus = StateStatus.NONE
        self.__analysis_count = 0
        self.__map_image_count = 0
        self.analyses_total = 0
        self.map_images_total = 0

    def send_message(self, status: StateStatus) -> None:
        if not self.__correlation_id or not self.__sio_client:
            return

        self.__set_status(status)

        data = {
            'status': self.__status.name,
            'analysesTotal': self.analyses_total,
            'analysisCount': self.__analysis_count,
            'mapImagesTotal': self.map_images_total,
            'mapImageCount': self.__map_image_count,
            'recipient': self.__correlation_id
        }

        self.__sio_client.emit('state_updated_api', data)

    def __set_status(self, status: StateStatus) -> None:
        self.__status = status

        if status == StateStatus.DATASET_ANALYZED:
            self.__increase_analysis_count()
        elif status == StateStatus.MAP_IMAGE_CREATED:
            self.__increase_map_image_count()

    def __increase_analysis_count(self) -> None:
        self.__analysis_count += 1

    def __increase_map_image_count(self) -> None:
        self.__map_image_count += 1
