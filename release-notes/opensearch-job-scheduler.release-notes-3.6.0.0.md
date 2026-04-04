## Version 3.6.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.6.0

### Bug Fixes

* Fix sort field in job metadata sweep query to use `_seq_no` instead of `_id` ([#896](https://github.com/opensearch-project/job-scheduler/pull/896))

### Infrastructure

* Fix integration tests with security plugin by providing FIPS build parameter ([#887](https://github.com/opensearch-project/job-scheduler/pull/887))
* Update shadow plugin usage to replace deprecated API ([#884](https://github.com/opensearch-project/job-scheduler/pull/884))

### Maintenance

* Bump `actions/download-artifact` from 7 to 8 ([#886](https://github.com/opensearch-project/job-scheduler/pull/886))
* Bump `actions/upload-artifact` from 6 to 7 ([#885](https://github.com/opensearch-project/job-scheduler/pull/885))
* Bump `aws-actions/configure-aws-credentials` from 5 to 6 ([#883](https://github.com/opensearch-project/job-scheduler/pull/883))
* Bump `de.undercouch.download` from 5.6.0 to 5.7.0 ([#882](https://github.com/opensearch-project/job-scheduler/pull/882))
* Bump `gradle-wrapper` from 9.2.0 to 9.4.0 ([#891](https://github.com/opensearch-project/job-scheduler/pull/891))
* Bump `gradle-wrapper` from 9.4.0 to 9.4.1 ([#894](https://github.com/opensearch-project/job-scheduler/pull/894))
* Bump `release-drafter/release-drafter` from 6 to 7 ([#893](https://github.com/opensearch-project/job-scheduler/pull/893))
