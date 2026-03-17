"""ECCO Dataset Production Package.

This package provides tools for generating PO.DAAC/ESDIS-ready NetCDF granules
from ECCO (Estimating the Circulation and Climate of the Ocean) model output.

Architecture Overview
---------------------

The following diagram illustrates the high-level architecture and data flow
through the pipeline:

.. mermaid::

    %%{init: {'theme': 'neutral', 'themeVariables': { 'edgeLabelBackground':'#ffffff'}}}%%
    flowchart TD
        subgraph inputs["Input Sources"]
            A[Job File]
            B[ECCO MDS Files]
            C[Grid Files]
            D[Mapping Factors]
            E[Metadata JSON]
            F[Configuration YAML]
        end

        subgraph task_gen["Task Generation"]
            G[create_job_task_list]
        end

        subgraph processing["Granule Processing"]
            H[generate_datasets]
            I[ecco_make_granule]
            J[ECCOMDSDataset]
            K[set_granule_ancillary_data]
            L[set_granule_metadata]
        end

        subgraph resources["Shared Resources"]
            M[ECCOGrid]
            N[ECCOMappingFactors]
            O[ECCOMetadata]
            P[ECCOTask]
        end

        subgraph output["Output"]
            Q[NetCDF Granules]
        end

        A --> G
        B --> G
        G --> H
        F --> H
        H --> I
        I --> J
        J --> K
        K --> L
        L --> Q
        C --> M
        D --> N
        E --> O
        M --> J
        N --> J
        O --> L
        P --> I

"""
from . import apps
from . import aws
from . import ecco_dataset
from . import ecco_file
from . import ecco_grid
from . import ecco_mapping_factors
from . import ecco_metadata
from . import ecco_podaac_metadata
from . import ecco_task
from . import ecco_time
