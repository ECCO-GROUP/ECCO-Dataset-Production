FROM python:3.10.4

# set a directory for the app
ARG FUNCTION_DIR="/home/app/"

WORKDIR ${FUNCTION_DIR}

# copy necessary files
# RUN mkdir ./ecco_cloud_utils
# COPY ./ecco_cloud_utils ./ecco_cloud_utils
RUN mkdir ./ecco_grids
COPY ./aws/ecco_grids ./ecco_grids
RUN mkdir ./ecco_v4_py
COPY ./aws/src/ecco_v4_py ./ecco_v4_py
RUN mkdir ./metadata
COPY ./aws/metadata ./metadata
RUN mkdir ./mapping_factors
RUN mkdir ./mapping_factors/land_mask
RUN mkdir ./mapping_factors/sparse
RUN mkdir ./mapping_factors/latlon_grid
COPY ./aws/mapping_factors/sparse ./mapping_factors/sparse
COPY ./aws/mapping_factors/land_mask ./mapping_factors/land_mask
COPY ./aws/mapping_factors/latlon_grid ./mapping_factors/latlon_grid

COPY ./aws/src/lambda_code/Dockerfile .
COPY ./aws/src/lambda_code/requirements.txt .
COPY ./aws/src/lambda_code/app.py .
COPY ./aws/src/ecco_gen_for_podaac_cloud.py .
COPY ./aws/src/utils/gen_netcdf_utils.py .

# install dependencies
RUN python3 -m pip install --upgrade pip setuptools wheel
RUN python3 -m pip install -r ${FUNCTION_DIR}requirements.txt --target ${FUNCTION_DIR}
RUN python3 -m pip install awslambdaric --target ${FUNCTION_DIR}

# setup lambda entry and cmd
COPY ./aws/src/lambda_code/entry.sh /
RUN chmod 755 /entry.sh
ENTRYPOINT [ "/entry.sh" ]
CMD [ "app.handler" ]