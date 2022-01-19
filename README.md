# SFMC Glue Jobs

SFMC Glue Job Codes

## Introduction

This project is a reference for Python source code projects used in AWS Glue jobs. The aim of this project is to be used as an example for other projects. The content of the project is the following:

* *.gitlab-ci.yml* file containing the implementation of the CI pipelines used to validate, test and upload the code to the corresponding AWS S3 buckets
* A folder per Glue job implementation
* Files and folders to define and store Python dependencies
* Folders to store additional files
* Folders to store JAR files

All the previous files are considered when uploading the code that will be used by the AWS Glue jobs to the S3 buckets from where it will be retrieved.

## CI pipeline

The CI pipeline is used to perform different operations against the current project:

* Validate code
* Optionally test
* Generate Python dependencies automatically
* Upload the code to AWS

This is done in the following way:

* In *feature* branches, the code is uploaded as a SNAPSHOT. The Python scripts are uploaded to a folder that will have a timestamp in the name. In this way it will be easier to update the code in the AWS Glue jobs
* In the *master* branch, the code is uploaded as a RELEASE. It is possible to set a version number for the release, either editing the .gitlab-ci.yml or running the pipeline manually and using the VERSION variable to set a new version. It is recommended to use a standard version convention like *major.minor.patch*  

## Python requirements file

The requirements.txt file contains the additional Python modules used by the Python script and that will ba automatically uploaded together with the rest of the code so they be available wihtin the S3 bucket locations.

## Python code

The Python scripts have to be stored within the *src* folder, inside each Glue job implementation.

## Additional files

Any additional file apart of the previously mentioned will be stored under the *files* folder. It is possible to locate files and folders, everything beneath the *files* location will be uploaded with the same folder structure.

### Jar files

It is also possible to upload JAR files by adding them to the *jars* folder.
