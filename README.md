# Team Phoenix Databricks

This repository contains all Team Phoenix Databricks notebooks used in jobs that are deployed by Azure DevOps.

## New Job Checklist

1. Clone repo, fetch/pull latest, and checkout new branch from master e.g.
```
git fetch
git checkout master
git pull
git checkout -b new-branch-name
```
2. If creating a new job, add new job to relevant job files e.g. https://github.azc.ext.hp.com/supplies-bd/team-phoenix-databricks/blob/master/databricks/jobs/springboard.yml
```
---
Jobs:
  - example-job
  - hardware-ltf
  - new-job-name
```
3. Add new/modified notebook/s and yml files, create new commit, push new branch to remote
```
git add notebooks/python/new-directory/new_notebook.py
git add databricks/jobs/springboard.yml
git commit -m "add new_notebook.py"
git push origin new-branch-name
```
4. Additionally, job configs will need to be added to the [team-phoenix-databricks-jobs-config repo](https://github.azc.ext.hp.com/supplies-bd/team-phoenix-databricks-jobs-config/). These job configs define the Databricks jobs including cluster node types, workspace configuration, runtime parameters, etc. In general, a new directory, base.yml, itg.yml, and prod.yml will need to be created for each new job. For more details on writing these yml files, [DataOS has provided a guide](https://pages.github.azc.ext.hp.com/hp-data-platform/dataos-ops-docs/#_databricks_e2).
5. When ready for review, file a Pull Request (PR) with new branch merging to master in both repos, and work with a Team Phoenix developer to approve the PRs Ideally the jobs-config PR will be approved and merged first as the Azure DevOps pipeline is configured to automatically deploy jobs when the master branch is updated in the team-phoenix-databricks repo.

## General Workflow

<img src="https://media.github.azc.ext.hp.com/user/24293/files/5c98102d-c93f-4f0d-8fc6-c18f5f6a23ad" width=50% height=50%>

In general, coders should work on notebooks in the Databricks web workspace in their repo. When ready to formally deploy, the general workflow is as follows:
1. A PR should be opened to merge the relevant working branch to "dev".
2. The "dev" branch is unprotected, so the committer can immediately merge the changes. Subsequently, a deployment will launch and redeploy all notebooks to the "dev" workspace.
3. Pending functionality testing, another PR should be opened to merge "dev" to "master". This PR will require another team-phoenix member's review and approval.
4. Upon merge, jobs in the "itg" workspace will be updated.
5. For production/"prod" updates, a more formal release process will follow where builds are set up on a cadence and documented with a succint changelog.

## Databricks URLs

dataos-dev-internal: https://dataos-dev-internal.cloud.databricks.com/  
dataos-dev: https://dataos-dev.cloud.databricks.com   

## Azure DevOps Pipeline URLs

dev: https://dev.azure.com/hpcodeway/dataos/_build?definitionId=8937    
itg: https://dev.azure.com/hpcodeway/dataos/_build?definitionId=9373    

## Informative Links
DataOS: Guide to the Cloud: https://pages.github.azc.ext.hp.com/hp-data-platform/dataos-ops-docs/    
Scala Style Guide: https://github.com/databricks/scala-style-guide  
Redshift Endpoints: https://rndwiki.inc.hpicorp.net/confluence/pages/viewpage.action?spaceKey=PSO&title=Redshift+Endpoints  
Databricks E2 repo: https://github.azc.ext.hp.com/hp-data-platform/databricks_e2  
Running jobs in Databricks: https://docs.databricks.com/jobs.html  
