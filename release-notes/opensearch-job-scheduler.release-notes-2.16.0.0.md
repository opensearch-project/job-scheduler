## Version 2.16.0.0

Compatible with OpenSearch 2.16.0

### Maintenance
* Increment version to 2.16.0 ([#638](https://github.com/opensearch-project/job-scheduler/pull/638)).

### Infrastructure
* Fix checkout action failure [(#650)](https://github.com/opensearch-project/job-scheduler/pull/650) [(#651)](https://github.com/opensearch-project/job-scheduler/pull/651).

### Enhancements
* Wrap interactions with `.opendistro-job-scheduler-lock` in `ThreadContext.stashContext` to ensure JS can read and write to the index [(#347)](https://github.com/opensearch-project/job-scheduler/pull/347) [(#647)](https://github.com/opensearch-project/job-scheduler/pull/647).