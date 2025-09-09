import itertools
from abc import ABC, abstractmethod
from configparser import ConfigParser
import os
import sys
from lib.utils import get_current_data, get_root_path, run_command


class Logger(ABC):
    @abstractmethod
    def _get_file_name_param(self) -> str: ...

    @abstractmethod
    def _get_mode(self) -> str: ...

    def __init__(self, settings: ConfigParser) -> None:
        self.settings = settings
        self._fio_file_path = None
        self._logs_dir_path = None

    def start_logger(self) -> None:
        pass

    def free_logger(self) -> None:
        pass

    def _create_config_for_fio(self, no_value=True) -> ConfigParser:
        fio_file = ConfigParser(allow_no_value=no_value)
        file_name_param = self._get_file_name_param()

        set_no_value = None if no_value else "1"

        fio_file["global"] = {}
        if not self.settings.has_option("global", "time_based"):
            fio_file.set("global", "time_based", set_no_value)
        fio_file.set("global", "group_reporting", set_no_value)
        fio_file.set("global", "direct", "1")

        for section in self.settings.sections():
            for key, value in self.settings.items(section):
                if key not in ["rw", "dev", "path_to_spdk_repo"]:
                    fio_file[section][key] = value

        for dev, rw in itertools.product(
            [i.strip() for i in self.settings["global"]["dev"].split(",")],
            [i.strip() for i in self.settings["global"]["rw"].split(",")],
        ):
            section_name = f"{rw}-{dev}-{self.settings['global']['bs']}"
            fio_file[section_name] = {}
            fio_file[section_name]["filename"] = dev
            fio_file[section_name]["rw"] = rw

            mode = self._get_mode()
            fio_file[section_name]["write_bw_log"] = (
                f"{self._logs_dir_path}/{self.settings['global']['bs']}-{dev}-{rw}-{mode}.results"
            )
            fio_file[section_name]["write_iops_log"] = (
                f"{self._logs_dir_path}/{self.settings['global']['bs']}-{dev}-{rw}-{mode}.results"
            )
            fio_file[section_name]["write_lat_log"] = (
                f"{self._logs_dir_path}/{self.settings['global']['bs']}-{dev}-{rw}-{mode}.results"
            )

        return fio_file

    def _write_fio_to_file(self, fio_config: ConfigParser) -> None:
        try:
            with open(self._fio_file_path, "w") as fio_file:
                fio_config.write(fio_file, space_around_delimiters=False)
        except IOError:
            print("Failed to write temporary Fio job file at tmpjobfile")
            sys.exit(3)

    def generate_fio_file(self) -> None:
        current_time = get_current_data()
        root_path = get_root_path()
        self._fio_file_path = f"{root_path}/tmp/tmpfile-{current_time}.fio"
        self._logs_dir_path = f"{root_path}/tmp/logs-dir-{current_time}"
        os.makedirs(self._logs_dir_path, exist_ok=True)

        fio_config = self._create_config_for_fio()
        self._write_fio_to_file(fio_config)

    def run_fio(self) -> None:
        command = ["fio", self._fio_file_path]
        run_command(command)
