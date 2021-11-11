# luigi-based pipeline for extraction of DYAMOND data

This directory contains routines for remapping and extracting DYAMOND data


# G weights

Most grids I will need are in /work/ka1081/DYAMOND/PostProc/GridsAndWeights/

but the GEOS-3km file didn't work so I had to generate my own weights for the 0.1deg grid

# GEOS model grid weights

```bash
cdo -P 16 -gendis,/work/ka1081/DYAMOND/PostProc/GridsAndWeights/0.10_grid.nc <GEOS-output-filepath> <output-weights-filename>
```

# ICON model grid weights

```bash
cdo -gennn,r360x180 -selvar,cell_area /pool/data/ICON/grids/public/mpim/0015/icon_grid_0015_R02B09_G.nc weigths.nc
```
