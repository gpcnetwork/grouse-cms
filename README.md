# GPC Reusable Observable Unified Study Environment
# -- Transforming Medicare and Medicaid Research Identifiable Files into PCORnet CDM

## Greater Plain Collaborative (GPC)
GPC is a network of 12 leading medical centers covering a diverse population of over 19 million patients across 9 states as part of the National Patient-Centered Clinical Research Network (PCORnet). http://gpcnetwork.org/

## GPC Reusable Observable Study Environment (GROUSE)
GROUSE is a GPC project (as well as name of the data enclave) to obtain health insurance claims from the Center for Medicare and Medicaid Services through the Research Data Assistance Center (ResDAC) at the University of Minnesota. We currently have 2011-2017 Medicare data and 2011-2012 Medicaid data from 9 states in the GPC. To acknowlege the usage of GROUSE data, please include the following citation in your publication: 

*Lemuel R Waitman, Xing Song, Dammika Lakmal Walpitage, Daniel C Connolly, Lav P Patel, Mei Liu, Mary C Schroeder, Jeffrey J VanWormer, Abu Saleh Mosa, Ernest T Anye, Ann M Davis, Enhancing PCORnet Clinical Research Network data completeness by integrating multistate insurance claims with electronic health records in a cloud environment aligned with CMS security and privacy requirements, Journal of the American Medical Informatics Association, 2021;, ocab269, https://doi.org/10.1093/jamia/ocab269*

## Medicare Research Identifiable Files (RIF)
Currently, the GPC coordinating center (GPC CC) recieves Medicare RIF files via windows compatible delivery media (i.e. USB hard drive, DVD, CD) from CMS chronic condition warehouse (CCW), or NewWave-GDIT, by mail. The raw files are in a compressed and encrypted format, called [Self-Decrypting Archives (SDAs)](https://innovation.cms.gov/files/x/bundled-payments-for-care-improvement-learning-area-size-info-doc.pdf). SDAs are stand-along executables that can only be decrypted and decompressed with encryption keys sent from CMS to GPC CC in separate secured email. After decryption and decompression each SDA executable, the actual data file (`.dat`) and the metadata file (`.fts`) and two additional (`.sas`) files were made available for downstream processing. GPC CC has implementated an ETL process leveraging the following key resources: AWS S3 bucket, AWS IAM, AWS Secret Manager, and Snowflake database. 

### Extract and Load 
As shown in the architecture below:

[res/ETL_workload.png]

- A: [load source] The source SDAs files were first uploaded to a designated, encrypted S3 bucket via secured upload (TLS/SSL) 
- B: [configure development environment] Properly configure the chosen developer environment (e.g., local laptop, AWS cloud9 IDE, EC2 instance) to be accessible to source S3 bucket and S3 Secret Manager
- C: [install dependencies] Run `bash ./dep/setup.py` to install all required dependency libraries specified in the `./dep/requirement.txt` file
- D: [create config file] Open `src/config.json` and fill out or modify required configuration information needed for running all the staging and transformation scripts
- E: [decrypt and decompress] Run `staging/decrypt.py` on the configured developer environment  
- F: [extract and load] Run `staging/stage_cms_care.py` on the configured developer environment  

### Transformation to PCORnet CDM
To improve interoperatability, we have implemented a process of transforming source Medicare RIF schema into PCORnet Common Data Model schema. Current transformation process is specific to Snowflake database, which, however can be easily adopted to PostGresql or MangoDB database backend (which supports stored procedure in javascript). The following data lineage diagram demystifies how transformation is implemented associating the `.sql` scripts with source, intermediate, and target tables. 

[res/c2p_transformation_diagram.png]

#### Option A: stepwise transformation
Run parts of the `c2p/transform_step.py` on the configured developer environment. You may want to start with running the transformation step by step to identify and fix any bugs should there be any. The script consist of three parts: 
1) create table shells by running the DDL (data definition lanugaue) scripts in `./src/ddl`; 
2) load reference concept mapping tables pre-loaded in `./ref/` folder; 
3) run stored procedures in `./src/stored_procedures` for staging and transformation; 
3) stage source CMS tables in the staging area in a 1-to-1 fashion (i.e. 1 source table to 1 target table), including applying all the mapping tables and creating de-duplication indices (`./src/dml`); 
4) perform the final transformation step and write to target CDM table (`./src/dml`).  

#### Option B: one-time transformation
For fully automated transformation, you can also run `c2p/transform_full.py` on the configured developer environment, which runs all the steps mentioned above without requiring any human intervention. However, we would recommend running the stepwise transformation at least once to validate the underlying sql scripts.     


---------------------------------------------------------------------------------------------------
References: 
- [CMS to PCORnet CDM](https://github.com/PCORnet-DRN-OC/Medicare-Data-Transformation)
- [CMS to OMOP CDM](https://github.com/OHDSI/ETL-CMS)
- [CMS to Sentinel CDM](https://dev.sentinelsystem.org/projects/DCMS/repos/cms_medicare_ffs_datamart/browse?at=CMS_FFS_SCDMv8)



---------------------------------------------------------------------------------------------------
*Copyright (c) 2021 The Curators of University of Missouri* <br/>
*Share and Enjoy according to the terms of the MIT Open Source License* <br/>             
*Repo structure style follows [GitHub Repository Structure Best Practices](https://soulaimanghanem.medium.com/github-repository-structure-best-practices-248e6effc405) (Ghanem, 2021)*