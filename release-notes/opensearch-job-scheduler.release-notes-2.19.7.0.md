## Version 2.19.7 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 2.19.7

### Bug Fixes

* Fix job metadata sweep query to sort by `_seq_no` instead of `_id` ([#912](https://github.com/opensearch-project/job-scheduler/pull/912))

### Infrastructure

* Guard `eclipse()` to spotless tasks and pin P2 mirror version ([#990](https://github.com/opensearch-project/job-scheduler/pull/990))
