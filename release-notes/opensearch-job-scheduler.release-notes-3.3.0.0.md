## Version 3.3.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.3.0

### Features
* Job History Service ([#814](https://github.com/opensearch-project/job-scheduler/pull/814))
* Create guice module to bind LockService interface from SPI to LockServiceImpl ([#833](https://github.com/opensearch-project/job-scheduler/pull/833))

### Enhancements
* Make LockService an interface and replace usages of ThreadContext.stashContext ([#714](https://github.com/opensearch-project/job-scheduler/pull/714))
* Introduce a configurable remote metadata client AND migrate LockService to the client ([#831](https://github.com/opensearch-project/job-scheduler/pull/831))

### Bug Fixes
* Fix: Update System.env syntax for Gradle 9 compatibility ([#821](https://github.com/opensearch-project/job-scheduler/pull/821))
* Revert "Introduce a configurable remote metadata client AND migrate LockService to the client (#831)" to avoid jarHell in downstream plugins. ([#836](https://github.com/opensearch-project/job-scheduler/pull/836))

### Infrastructure
* Run integ tests in the sample plugin with tests.security.manager set to true ([#809](https://github.com/opensearch-project/job-scheduler/pull/809))
* Update delete_backport_branch workflow to include release-chores branches ([#810](https://github.com/opensearch-project/job-scheduler/pull/810))

### Maintenance
* Dependabot: bump actions/download-artifact from 4 to 5 ([#811](https://github.com/opensearch-project/job-scheduler/pull/811))
* Dependabot: bump actions/checkout from 4 to 5 ([#818](https://github.com/opensearch-project/job-scheduler/pull/818))
* Dependabot: bump 1password/load-secrets-action from 2 to 3 ([#819](https://github.com/opensearch-project/job-scheduler/pull/819))
* Dependabot: bump actions/setup-java from 1 to 4 ([#825](https://github.com/opensearch-project/job-scheduler/pull/825))
* Dependabot: bump actions/github-script from 7 to 8 ([#829](https://github.com/opensearch-project/job-scheduler/pull/829))
* Dependabot: bump actions/setup-java from 4 to 5 ([#830](https://github.com/opensearch-project/job-scheduler/pull/830))