from socketio import SimpleClient
from enum import Enum


class AnalysisStatus(str, Enum):
    STARTUP = 1
    ANALYZING_DATASETS = 2
    DATASET_ANALYZED = 3
    CREATING_FACT_SHEET = 4
    CREATING_MAP_IMAGES = 5
    MAP_IMAGE_CREATED = 6
    CREATING_REPORT = 7


class AnalysisState():
    def __init__(self, correlation_id: str | None, sio_client: SimpleClient | None) -> None:
        self.__correlation_id = correlation_id
        self.__sio_client = sio_client
        self.__status: AnalysisStatus = AnalysisStatus.STARTUP
        self.__step_count = 0
        self.__analysis_count = 0
        self.__map_image_count = 0
        self.steps_total = 0
        self.analyses_total = 0
        self.map_images_total = 0

    def set_status(self, status: AnalysisStatus) -> None:
        self.__status = status

        statuses = [
            AnalysisStatus.DATASET_ANALYZED,
            AnalysisStatus.CREATING_FACT_SHEET,
            AnalysisStatus.CREATING_MAP_IMAGES,
            AnalysisStatus.CREATING_REPORT
        ]

        if status == AnalysisStatus.ANALYZING_DATASETS:
            self.steps_total += self.analyses_total
        elif status == AnalysisStatus.DATASET_ANALYZED:
            self.increase_analysis_count()
        elif status == AnalysisStatus.MAP_IMAGE_CREATED:
            self.increase_map_image_count

        if status in statuses:
            self.increase_step_count()

    def increase_analysis_count(self) -> None:
        self.__analysis_count += 1

    def increase_map_image_count(self) -> None:
        self.__map_image_count += 1

    def increase_step_count(self) -> None:
        self.__step_count += 1

    def send_message(self) -> None:
        if not self.__correlation_id or not self.__sio_client:
            return

        data = {
            'status': self.__status.name,
            'stepsTotal': self.steps_total,
            'stepCount': self.__step_count,
            'analysesTotal': self.analyses_total,
            'analysisCount': self.__analysis_count,
            'mapImagesTotal': self.map_images_total,
            'mapImageCount': self.__map_image_count,
            'recipient': self.__correlation_id
        }

        self.__sio_client.emit('state_updated_api', data)
