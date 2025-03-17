## Version 3.0.0.0-alpha1

Compatible with OpenSearch 3.0.0-alpha1

### Maintenance
* Increment version to 3.0.0-alpha1 [#722](https://github.com/opensearch-project/job-scheduler/pull/722).
* Update shadow plugin to `com.gradleup.shadow` [#722](https://github.com/opensearch-project/job-scheduler/pull/722).
* Enable custom start commands and options to resolve GHA issues [#702](https://github.com/opensearch-project/job-scheduler/pull/702).
* Fix delete merged branch workflow [#693](https://github.com/opensearch-project/job-scheduler/pull/693).
* Update `PULL_REQUEST_TEMPLATE` to include an API spec change in the checklist [#649](https://github.com/opensearch-project/job-scheduler/pull/649).
* Fix checkout action failure [#650](https://github.com/opensearch-project/job-scheduler/pull/650).

### Bug Fixes
* Fix job-scheduler with OpenSearch Refactoring [#730](https://github.com/opensearch-project/job-scheduler/pull/730).
* Fetch certs from security repo and remove locally checked in demo certs [#713](https://github.com/opensearch-project/job-scheduler/pull/713).
* Only download demo certs when integTest run with `-Dsecurity.enabled=true` [#737](https://github.com/opensearch-project/job-scheduler/pull/737).
