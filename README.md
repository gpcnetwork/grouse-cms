# GPC Reusable Observable Study Environment (GROUSE)
The Greater Plains Collaborative Clinical Data Research Networks (GPC CDRNs), is one of the 11 CDRNs included in PCORnet to further goals of the learning Health System and help to answer questions that are important to patient, clinician, and health system stakeholders. Current GPC includes 12 leading medical centers in 11 states (Kansas, Missouri, Iowa, Wisconsin, Nebraska, Minnesota, Texas, Utah, South Dakota, North Dakota and Indiana) committed to a shared vision of improving healthcare delivery through ongoing learning, adoption of evidence - based practices, and active research dissemination.

In order to understand all types of care a patient receives without being restricted to specific health systems, the GPC Reusable Observable Unified Study Environment (GROUSE) – a de-identified data resource, is created by merging CMS claims (covering the entire 11 states) with GPC site EMR data. GPC CDRN selected three types of conditions - one rare disease (amyotrophic lateral sclerosis), one common disease (breast cancer), and obesity, to a) quantify completeness of the health system-derived data repositories; and b) evaluate the distributions of health and care processes for the patients within the GPC versus the larger Medicare and Medicaid populations in our region to understand how studies of the GPC population generalize to the broader populations in our states. To acknowlege the usage of GROUSE data, please include the following citation in your publication: 

```
Lemuel R Waitman, Xing Song, Dammika Lakmal Walpitage, Daniel C Connolly, Lav P Patel, Mei Liu, 
Mary C Schroeder, Jeffrey J VanWormer, Abu Saleh Mosa, Ernest T Anye, Ann M Davis, 
Enhancing PCORnet Clinical Research Network data completeness by integrating multistate 
insurance claims with electronic health records in a cloud environment aligned with CMS security 
and privacy requirements, Journal of the American Medical Informatics Association, 2021;
ocab269, https://doi.org/10.1093/jamia/ocab269*
```

For more details on GROUSE CMS DUA protocol, security policy and procedures, as well as other technical documents, please refer to the following resources: 
- [GROUSE Master Protocol]()
- [GROUSE CMS Executive Summary](doc/CMS_Executive_Summary.pdf)
- [GROUSE Data Management Plan Approval](doc/CMS_DPSP_DMP_Approval.pdf)
- [System Security Policy Deck](doc/SSP_Policy_Deck.pdf)
- [GROUSE Private Github Repository](https://github.com/gpcnetwork/GROUSE): _this private github repo contains more sensitive information about the environment, please reach out to ask-umbmi@umsystem.edu for access_


# Medicare Research Identifiable Files (RIF)
Currently, the GPC coordinating center (GPC CC) recieves Medicare RIF files via windows compatible delivery media (i.e. USB hard drive, DVD, CD) from CMS chronic condition warehouse (CCW), or NewWave-GDIT, by mail. The raw files are in a compressed and encrypted format, called [Self-Decrypting Archives (SDAs)](https://innovation.cms.gov/files/x/bundled-payments-for-care-improvement-learning-area-size-info-doc.pdf). SDAs are stand-along executables that can only be decrypted and decompressed with encryption keys sent from CMS to GPC CC in separate secured email. After decryption and decompression each SDA executable, the actual data file (`.dat`) and the metadata file (`.fts`) and two additional (`.sas`) files were made available for downstream processing. GPC CC has implementated an ETL process leveraging the following key resources: AWS S3 bucket, AWS IAM, AWS Secret Manager, and Snowflake database. 

# Transforming Medicare and Medicaid Research Identifiable Files into PCORnet CDM
The extract, load and transform (ELT) process can be summarised in the diagram below

![workflow](res/elt-workflow.png)

### Extract and Load 
- A: [load source] The source SDAs files were first uploaded to a designated, encrypted S3 bucket via secured upload (TLS/SSL) 
- B: [configure development environment] Properly configure the chosen developer environment (e.g., local laptop, AWS cloud9 IDE, EC2 instance) to be accessible to source S3 bucket and S3 Secret Manager
- C: [install dependencies] Run `bash ./dep/setup.py` to install all required dependency libraries specified in the `./dep/requirement.txt` file
- D: [create config file] Open `./src/config.json` and fill out or modify required configuration information needed for running all the staging and transformation scripts
- E: [decrypt and decompress] Run `./src/stage/decrypt.py` in the configured developer environment  
- F: [extract and load] Run `./src/stage/stage_cms_care.py` in the configured developer environment  

### Transformation to PCORnet CDM
To improve interoperatability, we have implemented a process of transforming source Medicare RIF schema into PCORnet Common Data Model schema. Current transformation process is specific to Snowflake database. However, it can be easily adopted to PostGresql or MangoDB database backend (which supports stored procedure in javascript) but will require some adaptation to equivalent function objects in other types of databases. The following data lineage diagram demystifies how transformation is implemented associating the `.sql` scripts with source, intermediate, and target tables. The following list of entity relation diagrams (ERD) provides full data lineage details from source CMS RIF files to target CDM table (i.e., c2p transform).

- [ERD - c2p transform - enrollment](res/c2p_transform_enrollment.png)
- [ERD - c2p transform - demographic](res/c2p_transform_demographic.png)
- [ERD - c2p transform - death](res/c2p_transform_death.png)
- [ERD - c2p transform - lds_address_history](res/c2p_transform_lds_address_history.png)
- [ERD - c2p transform - encounter](res/c2p_transform_encounter.png)
- [ERD - c2p transform - diagnosis](res/c2p_transform_diagnosis.png)
- [ERD - c2p transform - procedures](res/c2p_transform_procedures.png)
- [ERD - c2p transform - dispensing](res/c2p_transform_dispensing.png)
- [ERD - c2p transform - obs_comm](res/c2p_transform_obs_comm.png)
- [ERD - c2p transform - payer_plan_period](res/c2p_transform_payer_plan_period.png)
- [ERD - c2p transform - cost](res/c2p_transform_cost.png)

G: Run parts of the `c2p/transform_step.py` on the configured developer environment. You may want to start with running the transformation step by step to identify and fix any bugs should there be any. The script consist of three parts: 
1) create table shells by running the DDL (data definition lanugaue) scripts in `./src/ddl`; 
2) load reference concept mapping tables pre-loaded in `./ref/` folder; 
3) run stored procedures in `./src/stored_procedures` for staging and transformation; 
3) stage source CMS tables in the staging area in a 1-to-1 fashion (i.e. 1 source table to 1 target table), including applying all the mapping tables and creating de-duplication indices (`./src/dml`); 
4) perform the final transformation step and write to target CDM table (`./src/dml`).  

For fully automated transformation, you can run `c2p/transform_full.py` on the configured developer environment without commenting out any steps, which runs all the steps mentioned above without requiring any human intervention. However, we would recommend running the stepwise transformation at least once to validate the underlying sql scripts.     

# Linkage and Deidentification
As described in full details in the GROUSE paper mentioned above, the deterministic data linkage between CMS claims and GPC CDM is provided by the CMS contractor, NewWave-GDIT, leveraging finder file and CMS referential database. 

Abide by current GROUSE protocol, research data is required to be fully de-identified. We implemented the following [safe harbor rules](https://www.hhs.gov/hipaa/for-professionals/privacy/special-topics/de-identification/index.html):

1. Remove all the following [HIPAA-recognized identifiers](https://www.hhs.gov/hipaa/for-professionals/privacy/special-topics/de-identification/index.html#standard) from the data (note that many of them were not provided in source files): 
- (A) Names
- (D) Telephone Numbers
- (L) Vehicle identifiers and serial numbers, including license plate numbers
- (E) Fax numbers
- (M) Device identifiers and serial numbers
- (F) Email addresses
- (N) Web Universal Resource Locators (URLs)
- (G) Social security numbers
- (O) Internet Protocol (IP) addresses
- (H) Medical record numbers
- (P) Biometric identifiers, including finger and voice prints
- (I) Health plan beneficiary numbers
- (Q) Full-face photographs and any comparable images
- (J) Account numbers
- (R) Any other unique identifying number, characteristic, or code, except as permitted (e.g., fully deidentified identifier with no possibility of reidentifying an individual even linking to other data resources)
- (K) Certificate/license numbers

2. Date Obfuscation
- Randomly shift all real dates by a number between 1 and 365 at individual level. 
- At the time of data release, if the individual reaches the age of 90, his/her birth date will be masked as 1900-01-01. This rule is in consistency with [PCORnet CDM general guidance](https://pcornet.org/wp-content/uploads/2022/01/PCORnet-Common-Data-Model-v60-2020_10_221.pdf).

3. Location Obfuscation
- All geographic subdivisions smaller than a state, including street address, city, county, precinct, ZIP code, and their equivalent geocodes, except for the initial **three digits of the ZIP code** if, according to the current publicly available data from the Bureau of the Census

The linkage and deidentification process can be summarised in the diagram below

![linkage-deid](res/linkage-deid-workflow.png)

- A: [load source] The source SDAs files were first uploaded to a designated, encrypted S3 bucket via secured upload (TLS/SSL) 
- B: [configurattion and preparation] The same as steps B to D from the ELT process above
- C: [decrypt and decompress] Run `./src/stage/decrypt.py` in the configured developer environment  
- D: [extract and load] Run `./src/stage/stage_cms_care.py`, `./src/stage/stage_xwalk.py`, and `./src/stage/stage_cdm.py` to stage all needed source files onto snowflake
- E-F: [match and align] Run stored procedures `./src/link_deid/stored_procedures/cdm_link_deid_stg.sql` and `./src/link_deid/dml/cdm_link_deid_stg.sql`to create intermediate tables specifying all de-identification parameters
- G-I: [deidentify and secure share] Run stored procedures `./src/link_deid/stored_procedures/cdm_link_deid.sql` and `./src/link_deid/dml/cdm_link_deid.sql` and create LDS and De-identified tables and views of all the CDM data. 

# PCORnet Common Data Model

# Concept Ontology Metadata

# Geocoded Data


---------------------------------------------------------------------------------------------------
References: 
- [CMS to PCORnet CDM](https://github.com/PCORnet-DRN-OC/Medicare-Data-Transformation)
- [CMS to OMOP CDM](https://github.com/OHDSI/ETL-CMS)
- [CMS to Sentinel CDM](https://dev.sentinelsystem.org/projects/DCMS/repos/cms_medicare_ffs_datamart/browse?at=CMS_FFS_SCDMv8)


---------------------------------------------------------------------------------------------------
*Copyright (c) 2021 The Curators of University of Missouri* <br/>
*Share and Enjoy according to the terms of the MIT Open Source License* <br/>
*Repo structure style follows [GitHub Repository Structure Best Practices](https://soulaimanghanem.medium.com/github-repository-structure-best-practices-248e6effc405) (Ghanem, 2021)*