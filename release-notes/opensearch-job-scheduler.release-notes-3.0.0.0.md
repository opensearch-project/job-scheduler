## Version 3.0.0.0

Compatible with OpenSearch 3.0.0

### Maintenance
* Remove beta1 qualifier ([#767](https://github.com/opensearch-project/job-scheduler/pull/767) [#768](https://github.com/opensearch-project/job-scheduler/pull/768)).
* Increment version to 3.0.0-alpha1 ([#722](https://github.com/opensearch-project/job-scheduler/pull/722)).
* Increment version to 3.0.0-beta1 ([#752](https://github.com/opensearch-project/job-scheduler/pull/752)).
* Update shadow plugin to `com.gradleup.shadow` ([#722](https://github.com/opensearch-project/job-scheduler/pull/722)).
* Enable custom start commands and options to resolve GHA issues ([#702](https://github.com/opensearch-project/job-scheduler/pull/702)).
* Fix delete merged branch workflow ([#693](https://github.com/opensearch-project/job-scheduler/pull/693)).
* Update `PULL_REQUEST_TEMPLATE` to include an API spec change in the checklist ([#649](https://github.com/opensearch-project/job-scheduler/pull/649)).
* Fix checkout action failure ([#650](https://github.com/opensearch-project/job-scheduler/pull/650)).
* dependabot: bump com.google.guava:failureaccess from 1.0.2 to 1.0.3 ([#750](https://github.com/opensearch-project/job-scheduler/pull/750)).
* dependabot: bump com.google.googlejavaformat:google-java-format ([#753](https://github.com/opensearch-project/job-scheduler/pull/753)).
* dependabot: bump com.netflix.nebula.ospackage from 11.11.1 to 11.11.2 ([#754](https://github.com/opensearch-project/job-scheduler/pull/754)).

### Bug Fixes
* Fix job-scheduler with OpenSearch Refactoring ([#730](https://github.com/opensearch-project/job-scheduler/pull/730)).
* Fetch certs from security repo and remove locally checked in demo certs ([#713](https://github.com/opensearch-project/job-scheduler/pull/713)).
* Only download demo certs when integTest run with `-Dsecurity.enabled=true` ([#737](https://github.com/opensearch-project/job-scheduler/pull/737)).