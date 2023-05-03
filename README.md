# Team Phoenix Databricks

This repository contains all Team Phoenix Databricks notebooks used in jobs that are deployed by Azure DevOps.

## New Job Checklist

1. Clone this repo, fetch/pull latest, and checkout new branch from master e.g.
```
git fetch
git checkout master
git pull
git checkout -b new-branch-name
```
2. Add any new/modified notebook/s, create new commit, push new branch to remote
```
git add notebooks/python/new-directory/new_notebook.py
git add databricks/jobs/jobs.yml
git commit -m "add new_notebook.py"
git push origin new-branch-name
```
3. If creating a new job, add new job name to relevant job files to the team-phoenix-databricks-deployment-pipeline repo following a similar approach e.g. for dev: https://github.azc.ext.hp.com/supplies-bd/team-phoenix-databricks-deployment-pipeline/blob/master/dev/jobs/jobs.yml, itg: https://github.azc.ext.hp.com/supplies-bd/team-phoenix-databricks-deployment-pipeline/blob/master/itg/jobs/jobs.yml, prod: https://github.azc.ext.hp.com/supplies-bd/team-phoenix-databricks-deployment-pipeline/blob/master/prod/jobs/jobs.yml
```
---
Jobs:
  - example-job
  - hardware-ltf
  - new-job-name
```
4. Additionally, before filing a PR in this repo, job configuration files will need to be added to the [team-phoenix-databricks-jobs-config repo](https://github.azc.ext.hp.com/supplies-bd/team-phoenix-databricks-jobs-config/). These files define the Databricks jobs including cluster node types and quantities, workspace configuration, default runtime parameters, structure of the workspace, etc. In general, a new directory, base.yml, itg.yml, and prod.yml will need to be created for each new job. For more details on writing these yml files, [DataOS has provided a guide](https://pages.github.azc.ext.hp.com/hp-data-platform/dataos-ops-docs/#_databricks_e2) and many working examples currently exist in the repo.
6. When ready for review, file a Pull Request (PR) with new branch merging to master in all repos, and work with a Team Phoenix developer to approve the PRs. Ideally the jobs-config PR will be approved and merged first as merging to master in that repo does not trigger a new deployment. Upon merge to master in this repo, a new release tag will be created and the prod Azure DevOps deplopyment pipeline will automatically re-deploy all jobs noted in their respective jobs.yml files and in accordance with the configurations noted for each job in its respective jobs-config set of yml files.

## General Workflow

<img src="https://media.github.azc.ext.hp.com/user/24293/files/5c98102d-c93f-4f0d-8fc6-c18f5f6a23ad" width=50% height=50%>

In general, coders should work on notebooks in the Databricks web workspace in their repo. When ready to formally deploy, the general workflow is as follows:
1. Functionality testing should be completed in the "dataos-dev-internal" Databricks workspace.
3. A pull request (PR) should be opened to merge the relevant working branch to "master".
4. Pending peer review, another team-phoenix member will comment and/or approve the PR.
5. Upon merge, jobs in the "prod-internal" will be updated.

## Databricks URLs

dataos-dev-internal: https://dataos-dev-internal.cloud.databricks.com/  
dataos-itg-internal: https://dataos-itg-internal.cloud.databricks.com/  
dataos-prod-internal: https://dataos-prod-internal.cloud.databricks.com/   
dataos-dev: https://dataos-dev.cloud.databricks.com/    
dataos-prod: https://dataos-prod.cloud.databricks.com/    

## Azure DevOps Pipeline URLs

dev-internal: https://dev.azure.com/hpcodeway/dataos/_build?definitionId=8937    
itg-internal: https://dev.azure.com/hpcodeway/dataos/_build?definitionId=9373    
prod-internal: https://dev.azure.com/hpcodeway/dataos/_build?definitionId=10475    

## Confluence
https://rndwiki.inc.hpicorp.net/confluence/display/SuppliesBigData/Team+Phoenix+Databricks  

## Informative Links
DataOS: Guide to the Cloud: https://pages.github.azc.ext.hp.com/hp-data-platform/dataos-ops-docs/    
Python Style Guide: https://www.python.org/dev/peps/pep-0008/  
Scala Style Guide: https://github.com/databricks/scala-style-guide  
Redshift Endpoints: https://rndwiki.inc.hpicorp.net/confluence/pages/viewpage.action?spaceKey=PSO&title=Redshift+Endpoints  
Databricks E2 repo: https://github.azc.ext.hp.com/hp-data-platform/databricks_e2  
Running jobs in Databricks: https://docs.databricks.com/jobs.html  
