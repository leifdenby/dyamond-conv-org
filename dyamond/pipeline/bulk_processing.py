import luigi
import datetime

from . import CropDyamondFiles, cdoalt
from pathlib import Path

DST_DATA_PATH = Path(
    "/mnt/lustre02/work/bb1153/DYAMOND_USER_DATA/ANALYSIS/b380984/DYAMOND_WINTER"
)


class ProcessModelFile(luigi.Task):
    model = luigi.Parameter()
    variable = luigi.Parameter(default="rlut")

    def requires(self):
        d0 = datetime.datetime(year=2020, month=1, day=20)
        d1 = d0 + datetime.timedelta(days=39)

        # list of models that need regridding
        model_res = {
            "ICON-5km": 0.1,
            "ICON-SAP-5km": 0.1,
            "UM-5km": 0.1,
            "GEOS-3km": 0.1,
        }

        model = self.model
        model_latlon_resolution = model_res.get(model)

        kwargs = dict(
            start_date=d0,
            end_date=d1,
            variables=[self.variable],
            model=model,
            time_resolution="15min",
            bbox=[-63, -33, 9, 19],
            dst_grid_latlon_resolution=model_latlon_resolution,
            dst_data_path_root=DST_DATA_PATH,
        )

        task = CropDyamondFiles(**kwargs)
        return task

    def run(self):
        cdo = cdoalt.cdo()
        filenames = [t.fn for t in self.input()]
        Path(self.output().fn).parent.mkdir(exist_ok=True, parents=True)
        cdo.mergetime(infiles=filenames).execute(self.output().fn)

    def output(self):
        dst_path_merge = DST_DATA_PATH.parent / "DYAMOND_WINTER_merged"
        p = dst_path_merge / f"{self.variable}_{self.model}.nc"
        return luigi.LocalTarget(str(p))


class ProcessAllMerged(luigi.WrapperTask):
    def requires(self):

        # models = ["UM-5km", "ICON-5km", "GEOS-3km"]
        models = [
            "UM-5km",
            # "ICON-5km",
            "ICON-SAP-5km",
            "GEOS-3km",
        ]

        variables = [
            "rlut",
            "vas", # surface_northward_wind
            "uas", # surface_eastward_wind
            "hfls", # surface_upward_latent_heat_flux
            "hfss", # surface_upward_sensible_heat_flux
            "clwvi", # atmosphere_mass_content_of_cloud_condensed_water
        ]

        tasks = []

        for variable in variables:
            for model in models:
                task = ProcessModelFile(model=model, variable=variable)
                tasks.append(task)

        return tasks
