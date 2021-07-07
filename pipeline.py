import luigi
import luigi.contrib.ssh
import datetime

import dyamond_data


class RemoteDyamondFile(luigi.Task):
    date = luigi.DateSecondParameter(
        default=datetime.datetime(year=2020, month=2, day=23)
    )
    model = luigi.Parameter(default="ICON-5km")
    time_resolution = luigi.Parameter(default="15min")
    variable = luigi.Parameter(default="rlut")
    local_data_path = luigi.Parameter(default=".")

    def output(self):
        data_root_remote = dyamond_data.DATA_ROOT_MISTRAL
        fn = dyamond_data.make_path(
            date=self.date,
            model=self.model,
            time_resolution=self.time_resolution,
            variable=self.variable,
        )

        path_remote = data_root_remote / fn
        return luigi.contrib.ssh.RemoteTarget(
            path=path_remote, host=dyamond_data.HOSTNAME_MISTRAL
        )


class FetchDyamondFile(luigi.Task):
    date = luigi.DateSecondParameter(
        default=datetime.datetime(year=2020, month=2, day=23)
    )
    model = luigi.Parameter(default="UM-5km")
    time_resolution = luigi.Parameter(default="15min")
    variable = luigi.Parameter(default="rlut")
    local_data_path = luigi.Parameter(default=".")

    def requires(self):
        return RemoteDyamondFile(
            date=self.date,
            model=self.model,
            time_resolution=self.time_resolution,
            variable=self.variable,
        )

    def run(self):
        input = self.input()
        import ipdb

        ipdb.set_trace()
