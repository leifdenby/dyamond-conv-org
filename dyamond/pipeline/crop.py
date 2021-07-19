import luigi
import datetime
from pathlib import Path

from .base import DyamondFile
from .. import data as dyamond_data
from .cdoalt import cdo


GRID_FILES_PATH = Path("/work/ka1081/DYAMOND/PostProc/GridsAndWeights")

MODEL_GRIDFILE_IDS = {
    "ICON-5km": "ICON_R2B09_2",
    "UM-5km": "UM-5km",
    "GOES-3km": "GEOS-3.25km"
}


def _make_model_gridfile_path(resolution, model):
    grid_id = MODEL_GRIDFILE_IDS.get(model)
    if grid_id is None:
        raise NotADirectoryError(model)
    filename = f"{grid_id}_{resolution:.02f}_grid_wghts.nc"
    return GRID_FILES_PATH / filename


def _make_latlon_gridfile_path(resolution):
    """resolution in degrees"""
    return GRID_FILES_PATH / f"{resolution:.02f}_grid.nc"


class CropDyamondFile(luigi.Task):
    """
    Crop DYAMOND file to latlon bounding-box (with reprojection from the native
    grid if needed)
    """

    date = luigi.DateParameter()
    model = luigi.Parameter()
    time_resolution = luigi.Parameter()
    variable = luigi.Parameter()
    bbox = luigi.ListParameter()

    dst_grid_latlon_resolution = luigi.FloatParameter(default=None)

    src_data_path_root = luigi.Parameter(default=dyamond_data.DATA_ROOT_MISTRAL)
    dst_data_path_root = luigi.Parameter()

    def requires(self):
        if len(self.bbox) != 4:
            raise Exception(
                "`bbox` should be a list with [lon_min, lon_max, lat_min, lat_max]"
            )
        return DyamondFile(
            date=self.date,
            model=self.model,
            time_resolution=self.time_resolution,
            variable=self.variable,
            data_path=self.src_data_path_root,
        )

    def run(self):
        p_in = self.input().fn
        p_out = self.output().fn

        Path(p_out).parent.mkdir(exist_ok=True, parents=True)

        cdo_pl = cdo(verbose=True).selname(self.variable, infile=str(p_in))

        if self.dst_grid_latlon_resolution is not None:
            grid_filename = _make_latlon_gridfile_path(
                resolution=self.dst_grid_latlon_resolution
            )
            weights_filename = _make_model_gridfile_path(
                resolution=self.dst_grid_latlon_resolution, model=self.model
            )
            if self.model not in MODEL_GRIDFILE_IDS:
                raise NotImplementedError(self.model)
            cdo_pl = cdo_pl.remap(grid_filename, weights_filename)

        cdo_pl.sellonlatbox(*list(self.bbox)).execute(p_out)

    def output(self):
        resolution = self.dst_grid_latlon_resolution
        if resolution is None:
            res_id = "native_grid"
        else:
            res_id = f"{resolution:.02f}x{resolution:.02f}deg"

        bbox_id = "".join([f"{d}{v:.02f}" for (d, v) in zip("WESN", list(self.bbox))])

        dset_id = f"{res_id}_{bbox_id}"

        import ipdb
        with ipdb.launch_ipdb_on_exception():
            f_path = dyamond_data.make_path(
                date=self.date,
                model=self.model,
                time_resolution=self.time_resolution,
                variable=self.variable,
                data_root=Path(self.dst_data_path_root) / dset_id,
            )

        return luigi.LocalTarget(f_path)


class CropDyamondFiles(luigi.Task):
    start_date = luigi.DateParameter()
    end_date = luigi.DateParameter()
    variables = luigi.Parameter()

    model = luigi.Parameter()
    time_resolution = luigi.Parameter()
    bbox = luigi.ListParameter()

    dst_grid_latlon_resolution = luigi.FloatParameter(default=None)

    src_data_path_root = luigi.Parameter(default=dyamond_data.DATA_ROOT_MISTRAL)
    dst_data_path_root = luigi.Parameter()

    def requires(self):
        tasks = []
        for variable in list(self.variables):
            date = self.start_date
            while date <= self.end_date:
                task = CropDyamondFile(
                    date=date,
                    variable=variable,
                    model=self.model,
                    time_resolution=self.time_resolution,
                    bbox=self.bbox,
                    dst_grid_latlon_resolution=self.dst_grid_latlon_resolution,
                    src_data_path_root=self.src_data_path_root,
                    dst_data_path_root=self.dst_data_path_root,
                )
                tasks.append(task)
                date += datetime.timedelta(days=1)
        return tasks

    def output(self):
        return self.input()
