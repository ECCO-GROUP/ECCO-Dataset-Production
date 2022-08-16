FROM python:3.10.4

# set a directory for the app
ARG FUNCTION_DIR="/home/app/"
WORKDIR ${FUNCTION_DIR}


# ========== <Copy files> =========================================================================
# copy grids
RUN mkdir ./ecco_grids
COPY ./ecco_grids/V4r4 ./ecco_grids

# copy ecco_code (ecco_v4_py) files
RUN mkdir ./ecco_code
COPY ./src/ecco_code ./ecco_code

# copy metadata files
RUN mkdir ./metadata
COPY ./metadata/V4r4 ./metadata

# copy all mapping factors needed
# this is just the land_masks, sparse matrices, and latlon_grid
RUN mkdir ./mapping_factors
RUN mkdir ./mapping_factors/land_mask
RUN mkdir ./mapping_factors/sparse
RUN mkdir ./mapping_factors/latlon_grid
COPY ./mapping_factors/V4r4/sparse ./mapping_factors/sparse
COPY ./mapping_factors/V4r4/land_mask ./mapping_factors/land_mask
COPY ./mapping_factors/V4r4/latlon_grid ./mapping_factors/latlon_grid

# copy lambda_code files
COPY ./src/lambda_code/V4r4/Dockerfile .
COPY ./src/lambda_code/V4r4/requirements.txt .
COPY ./src/lambda_code/V4r4/app.py .

# copy main code files (ecco_gen_for_podaac_cloud.py and gen_netcdf_utils.py)
COPY ./src/ecco_gen_for_podaac_cloud.py .
COPY ./src/utils/gen_netcdf_utils.py .
# ========== </Copy files> ========================================================================


# install dependencies (upgrade pip, and install requirements.txt and awslambdaric)
RUN python3 -m pip install --upgrade pip setuptools wheel
RUN python3 -m pip install -r ${FUNCTION_DIR}requirements.txt --target ${FUNCTION_DIR}
RUN python3 -m pip install awslambdaric --target ${FUNCTION_DIR}


# setup lambda entry and function to call when starting
COPY ./src/lambda_code/V4r4/entry.sh /
RUN chmod 755 /entry.sh
ENTRYPOINT [ "/entry.sh" ]
CMD [ "app.handler" ]