import luigi
import subprocess
import signal
from pathlib import Path

import cmip6


class CMIPFile(luigi.Task):
    model = luigi.Parameter()
    experiment = luigi.Parameter()
    variable = luigi.Parameter()
    time_range = luigi.Parameter()
    time_frequency = luigi.Parameter(default="day")
    data_path_root = luigi.Parameter(default=cmip6.DEFAULT_DATA_ROOT)

    def output(self):
        return luigi.LocalTarget(
            cmip6.find_cmip6_file(
                model=self.model,
                variable=self.variable,
                experiment=self.experiment,
                time_frequency=self.time_frequency,
                time_range=self.time_range,
                data_root=self.data_path_root,
            )
        )


def _execute(cmd):
    # https://stackoverflow.com/a/4417735
    popen = subprocess.Popen(cmd, stdout=subprocess.PIPE, universal_newlines=True)
    for stdout_line in iter(popen.stdout.readline, ""):
        yield stdout_line
    popen.stdout.close()
    return_code = popen.wait()

    if return_code:
        raise subprocess.CalledProcessError(return_code, cmd)


def _call_cdo(args, verbose=True):
    try:
        cmd = ["cdo"] + args.split(" ")
        print("$$", " ".join(cmd))
        for output in _execute(cmd):
            if verbose:
                print((output.strip()))

    except subprocess.CalledProcessError as ex:
        return_code = ex.returncode
        error_extra = ""
        if -return_code == signal.SIGSEGV:
            error_extra = ", cdo segfaulted "

        raise Exception(
            "There was a problem when calling cdo "
            "(errno={}): {} {}".format(error_extra, return_code, ex)
        ) from ex


class CMIPFileGeoSubset(luigi.Task):
    bbox = luigi.ListParameter()
    experiment = luigi.Parameter()
    model = luigi.Parameter()
    variable = luigi.Parameter()
    time_range = luigi.Parameter()
    time_frequency = luigi.Parameter(default="day")
    src_data_path_root = luigi.Parameter(default=cmip6.DEFAULT_DATA_ROOT)
    dst_data_path_root = luigi.Parameter()

    def requires(self):
        if len(self.bbox) != 4:
            raise Exception(
                "`bbox` should be a list with [lon_min, lon_max, lat_min, lat_max]"
            )
        return CMIPFile(
            model=self.model,
            variable=self.variable,
            experiment=self.experiment,
            time_frequency=self.time_frequency,
            time_range=self.time_range,
            data_path_root=self.src_data_path_root,
        )

    def run(self):
        p_in = self.input().fn
        p_out = self.output().fn

        Path(p_out).parent.mkdir(exist_ok=True, parents=True)

        # cdo -f nc -sellonlatbox,-20,20,-10,30
        bbox_s = ",".join([f"{v:f}" for v in self.bbox])
        _call_cdo(f"-f nc -sellonlatbox,{bbox_s} {p_in} {p_out}")

    def output(self):
        if not self.input().exists():
            # if we don't have a soure file to read we don't know what the
            # output filename should be
            return luigi.LocalTarget("__fake_filename__")

        p_in_full = Path(self.input().fn)

        # we want to keep the same folder structure in the directory where we
        # have processed the data, so we just strip the root from the path
        p_in_rel = p_in_full.relative_to(self.src_data_path_root)

        p_out = Path(self.dst_data_path_root) / p_in_rel
        return luigi.LocalTarget(str(p_out))


class CMIPFileGeoSubsetAllTimes(luigi.Task):
    bbox = luigi.ListParameter()
    experiment = luigi.Parameter()
    model = luigi.Parameter()
    variable = luigi.Parameter()
    time_frequency = luigi.Parameter(default="day")
    src_data_path_root = luigi.Parameter(default=cmip6.DEFAULT_DATA_ROOT)
    dst_data_path_root = luigi.Parameter()

    def requires(self):
        # find all files that cover the entire time range
        filepaths = cmip6.find_cmip6_file(
            model=self.model,
            variable=self.variable,
            experiment=self.experiment,
            time_frequency=self.time_frequency,
            time_range="all",
            data_root=self.src_data_path_root,
        )

        def _find_time_range_from_filepath(fp):
            return fp.name.split("_")[-1].replace(".nc", "")

        time_ranges = [_find_time_range_from_filepath(fp) for fp in filepaths]

        return [
            CMIPFileGeoSubset(
                model=self.model,
                variable=self.variable,
                experiment=self.experiment,
                time_frequency=self.time_frequency,
                time_range=time_range,
                src_data_path_root=self.src_data_path_root,
                dst_data_path_root=self.dst_data_path_root,
                bbox=self.bbox,
            )
            for time_range in time_ranges
        ]


class CMIPFileGeoSubsetAllTimesAllModels(luigi.WrapperTask):
    bbox = luigi.ListParameter()
    experiment = luigi.Parameter()
    variable = luigi.Parameter()
    time_frequency = luigi.Parameter(default="day")
    src_data_path_root = luigi.Parameter(default=cmip6.DEFAULT_DATA_ROOT)
    dst_data_path_root = luigi.Parameter()

    def requires(self):
        models = cmip6.Models

        return [
            CMIPFileGeoSubsetAllTimes(
                model=model,
                variable=self.variable,
                experiment=self.experiment,
                time_frequency=self.time_frequency,
                src_data_path_root=self.src_data_path_root,
                dst_data_path_root=self.dst_data_path_root,
                bbox=self.bbox,
            )
            for model in models
        ]
