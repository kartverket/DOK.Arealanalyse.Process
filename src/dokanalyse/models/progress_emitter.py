from enum import Enum
from typing import Dict
from socketio import SimpleClient


class ProgressStatus(str, Enum):
    STARTING_UP = 1
    ANALYZING_DATASETS = 2
    CREATING_FACT_SHEET = 3
    CREATING_MAP_IMAGES = 4
    CREATING_REPORT = 5,
    NONE = -1


class TaskStatus(str, Enum):
    NOT_STARTED = 1,
    IN_PROGRESS = 2,
    DONE = 3


class ProgressEmitter:
    def __init__(
        self,
        correlation_id: str | None,
        sio_client: SimpleClient | None,
        include_facts: bool,
        create_binaries: bool
    ) -> None:
        self.__correlation_id = correlation_id
        self.__sio_client = sio_client
        self.__status: ProgressStatus = ProgressStatus.NONE
        self.__analysis_count = 0
        self.__map_image_count = 0
        self.__include_facts = include_facts
        self.__create_binaries = create_binaries
        self.__tasks = self.__create_tasks()
        self.analyses_total = 0
        self.map_images_total = 0

    def send_message(self, status: ProgressStatus, task_done: bool = False) -> None:
        if not self.__correlation_id or not self.__sio_client:
            return

        self.__set_status(status, task_done)

        data = {
            'status': self.__status.name,
            'analysesTotal': self.analyses_total,
            'analysisCount': self.__analysis_count,
            'mapImagesTotal': self.map_images_total,
            'mapImageCount': self.__map_image_count,
            'recipient': self.__correlation_id,
            'tasks': self.__tasks
        }

        self.__sio_client.emit('state_updated_api', data)

    def __create_tasks(self) -> Dict[str, str]:
        tasks = {
            ProgressStatus.STARTING_UP.name: TaskStatus.NOT_STARTED.name,
            ProgressStatus.ANALYZING_DATASETS.name: TaskStatus.NOT_STARTED.name
        }

        if self.__include_facts:
            tasks[ProgressStatus.CREATING_FACT_SHEET.name] = TaskStatus.NOT_STARTED.name

        if self.__create_binaries:
            tasks[ProgressStatus.CREATING_MAP_IMAGES.name] = TaskStatus.NOT_STARTED.name
            tasks[ProgressStatus.CREATING_REPORT.name] = TaskStatus.NOT_STARTED.name

        return tasks

    def __set_status(self, status: ProgressStatus, task_done: bool) -> None:
        self.__status = status

        if status == ProgressStatus.ANALYZING_DATASETS and not task_done:
            self.__increase_analysis_count()
        elif status == ProgressStatus.CREATING_MAP_IMAGES and not task_done:
            self.__increase_map_image_count()

        self.__tasks[status.name] = TaskStatus.DONE.name if task_done else TaskStatus.IN_PROGRESS.name

    def __increase_analysis_count(self) -> None:
        self.__analysis_count += 1

    def __increase_map_image_count(self) -> None:
        self.__map_image_count += 1
