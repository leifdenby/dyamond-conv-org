import luigi
import luigi.contrib.ssh
import datetime
from pathlib import Path

from .. import data as dyamond_data


class DyamondFile(luigi.Task):
    """
    Represents a DYAMOND model output file either on a local or remote file
    system
    """

    date = luigi.DateSecondParameter(
        default=datetime.datetime(year=2020, month=2, day=23)
    )
    model = luigi.Parameter(default="ICON-5km")
    time_resolution = luigi.Parameter(default="15min")
    variable = luigi.Parameter(default="rlut")
    data_path = luigi.Parameter(default=".")

    remote_hostname = luigi.Parameter(default=None)

    def run(self):
        if not Path(self.output().fn).exists():
            raise Exception(f"Couldn't find source file: `{self.output().fn}`")

    def output(self):
        fn = dyamond_data.make_path(
            date=self.date,
            model=self.model,
            time_resolution=self.time_resolution,
            variable=self.variable,
        )

        full_path = self.data_path / fn
        if self.remote_hostname is not None:
            return luigi.contrib.ssh.RemoteTarget(
                path=full_path, host=self.remote_hostname
            )
        else:
            return luigi.LocalTarget(full_path)


class FetchDyamondFile(luigi.Task):
    date = luigi.DateSecondParameter(
        default=datetime.datetime(year=2020, month=2, day=23)
    )
    model = luigi.Parameter(default="UM-5km")
    time_resolution = luigi.Parameter(default="15min")
    variable = luigi.Parameter(default="rlut")
    local_data_path = luigi.Parameter(default=".")
    remote_hostname = luigi.Parameter()

    def requires(self):
        return DyamondFile(
            date=self.date,
            model=self.model,
            time_resolution=self.time_resolution,
            variable=self.variable,
            remote_hostname=self.remote_hostname,
        )

    def run(self):
        input = self.input()
        raise NotImplementedError
