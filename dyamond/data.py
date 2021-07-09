"""
Utilities for working with DYAMOND data filepaths and filenames

2021/6/18 Leif Denby GPL-3 License
"""

from pathlib import Path
import datetime

INSTITUTE_FOR_MODEL = {"ICON-5km": "MPI-M", "UM-5km": "MetOffice"}

PHYSICS_CONFIGURATION_FOR_MODEL = {"ICON-5km": "dpp0029", "UM-5km": "r1i1p1f1"}

COUPLING_CONFIGURATION_FOR_MODEL = {"ICON-5km": "DW-CPL", "UM-5km": "DW-ATM"}

DATA_ROOT_MISTRAL = Path("/pf/b/b380984/dyamond/DYAMOND_WINTER/")

HOSTNAME_MISTRAL = "mistralpp.dkrz.de"

DATETIME_FORMAT = "%Y%m%d%H%M%S"


def make_path(
    date=datetime.datetime(year=2020, month=1, day=20),
    model="ICON-5km",
    time_resolution="15min",
    variable="rlut",
    data_root=DATA_ROOT_MISTRAL,
):
    """
    Generate the full path to a netCDF file containing `variable` at `time_resolution` for a specific `model` on a given `date`.
    If you have copied data from mistral should provide the root data path with `data_root`.
    """
    institute = INSTITUTE_FOR_MODEL[model]
    physics_conf = PHYSICS_CONFIGURATION_FOR_MODEL[model]
    coupling_conf = COUPLING_CONFIGURATION_FOR_MODEL[model]

    # make sure we have a datetime.datetime rather than just a datetime.date
    if isinstance(date, datetime.date):
        date = datetime.datetime.combine(date, datetime.time(hour=0, minute=0))

    if time_resolution == "15min":
        if model == "ICON-5km":
            t_start = date
            t_end = (
                t_start + datetime.timedelta(days=1) - datetime.timedelta(minutes=15)
            )
        elif model == "UM-5km":
            t_start = date + datetime.timedelta(minutes=30)
            t_end = date + datetime.timedelta(days=1) - datetime.timedelta(minutes=30)
        else:
            raise NotImplementedError(model)
    else:
        raise NotImplementedError(time_resolution)

    t_start_s = t_start.strftime(DATETIME_FORMAT)
    t_end_s = t_end.strftime(DATETIME_FORMAT)

    data_path = (
        Path(data_root)
        / institute
        / model
        / coupling_conf
        / "atmos"
        / time_resolution
        / variable
        / physics_conf
        / "ml"
        / "gn"
    )
    filename = f"{variable}_{time_resolution}_{model}_{coupling_conf}_{physics_conf}_ml_gn_{t_start_s}-{t_end_s}.nc"

    return data_path / filename
