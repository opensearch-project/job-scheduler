## Version 3.10.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.10.0

### Features

* Deschedule orphaned jobs whose document was deleted from remote-store backed indices ([#1006](https://github.com/opensearch-project/job-scheduler/pull/1006))

### Bug Fixes

* Run scheduled jobs in a clean thread context to prevent inheriting the indexing writer's credentials ([#1028](https://github.com/opensearch-project/job-scheduler/pull/1028))
* Fix flaky tests in LockServiceIT ([#914](https://github.com/opensearch-project/job-scheduler/pull/914))

### Infrastructure

* Enable OpenSearch logger usage check for Job Scheduler plugin builds ([#933](https://github.com/opensearch-project/job-scheduler/pull/933))

### Maintenance

* Move @joshpalis to emeritus maintainer status ([#1012](https://github.com/opensearch-project/job-scheduler/pull/1012))
