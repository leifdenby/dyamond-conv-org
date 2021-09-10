"""Simplistic interface for calling cdo from python"""
import subprocess
import signal


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
        cmd = args.split(" ")
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


class CDO:
    """Simplistic interface for calling cdo from python"""

    def __init__(self, cdo_path="cdo", operations=[], parameters={}):
        self.operations = operations
        self.parameters = parameters
        self.cdo_path = cdo_path

    def _add_op(self, new_op):
        return CDO(operations=self.operations + [new_op], parameters=self.parameters)

    def remap(self, grid_file, weights_file, infile=None):
        """
        Remap from one grid to another
        """
        params = {k: v for (k, v) in locals().items() if k != "self"}
        new_op = dict(name="remap", params=params)
        if infile is not None:
            new_op["infile"] = infile
        return self._add_op(new_op=new_op)

    def sellonlatbox(self, lon_west, lon_east, lat_south, lat_north, infile=None):
        """
        Select data within a lat/lon bounding box
        """
        params = {k: v for (k, v) in locals().items() if k != "self"}
        new_op = dict(name="sellonlatbox", params=params)
        if infile is not None:
            new_op["infile"] = infile
        return self._add_op(new_op=new_op)

    def selname(self, variable, infile=None):
        """
        Select variable(s), `variable` may be a list variable names
        """
        params = {k: v for (k, v) in locals().items() if k != "self"}
        new_op = dict(name="selname", params=params)
        if infile is not None:
            new_op["infile"] = infile
        return self._add_op(new_op=new_op)

    def mergetime(self, infiles):
        """
        Merge multiple files together
        """
        params = {k: v for (k, v) in locals().items() if k != "self"}
        new_op = dict(name="mergetime", params=params)
        assert len(infiles) > 1
        return self._add_op(new_op=new_op)

    def _build_cmd(self, out_filename):
        params_cdo = []
        for param, value in self.parameters.items():
            if param == "time_axis":
                if value == "relative":
                    params_cdo.append("-r")
                elif value == "absolute":
                    params_cdo.append("-r")
                else:
                    raise NotImplementedError(param, value)

            if param == "verbose" and value:
                params_cdo.append("-v")

            if param == "fout_format" and value:
                params_cdo.append(f"-f {value}")

        # cdo runs operations from reverse, but we'll chain them left to right here
        ops_cdo = []
        for op in self.operations[::-1]:
            op_name = op["name"]
            op_params = op["params"]

            if op_name == "selname":
                if type(op_params["variable"]) != list:
                    arglist = [op_params["variable"]]
                else:
                    arglist = op_params["variable"]
            elif op_name == "sellonlatbox":
                arglist = [
                    str(op_params[v])
                    for v in ["lon_west", "lon_east", "lat_south", "lat_north"]
                ]
            elif op_name == "remap":
                arglist = [str(op_params[v]) for v in ["grid_file", "weights_file"]]
            elif op_name == "mergetime":
                arglist = [" ".join(op_params["infiles"])]
            else:
                raise NotImplementedError(op_name)

            op_str = f"-{op_name},{','.join(arglist)}"
            if op_params.get("infile") is not None:
                infile = op_params["infile"]
                op_str += f" {infile}"
            if op_name == "mergetime":
                ### XXX: quick hack
                op_str = op_str.replace(",", " ")
            ops_cdo.append(op_str)

        cdo_cmd_parts = params_cdo + ops_cdo

        return f"{self.cdo_path} {' '.join(cdo_cmd_parts)} {out_filename}"

    def __repr__(self):
        ops_s = " -> ".join([op["name"] for op in self.operations])
        return f"cdo {ops_s}"

    def execute(self, out_filename):
        cdo_cmd = self._build_cmd(out_filename=out_filename)

        _call_cdo(cdo_cmd)


def cdo(time_axis="relative", verbose=False, fout_format="nc4"):
    """
    Start a cdo call
    """
    parameters = dict(time_axis=time_axis, verbose=verbose, fout_format=fout_format)
    return CDO(parameters=parameters)
