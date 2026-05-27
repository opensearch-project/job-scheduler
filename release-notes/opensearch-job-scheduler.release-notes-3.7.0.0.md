## Version 3.7.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.7.0

### Bug Fixes

* Fix typo in log message: "occured" → "occurred" ([#915](https://github.com/opensearch-project/job-scheduler/pull/915))
* Fix typo in log message: "occured" → "occurred" ([#917](https://github.com/opensearch-project/job-scheduler/pull/917))

### Infrastructure

* Add Maven cache mirror before mavenCentral to reduce HTTP 429 throttling errors in CI builds ([#918](https://github.com/opensearch-project/job-scheduler/pull/918))
* Add issues write permission to untriaged label workflow to fix 403 errors ([#919](https://github.com/opensearch-project/job-scheduler/pull/919))
* Fix Codecov configuration by removing duplicate sections and validating .codecov.yml ([#898](https://github.com/opensearch-project/job-scheduler/pull/898))
* Pin GitHub Actions to commit SHAs to prevent supply chain attacks ([#921](https://github.com/opensearch-project/job-scheduler/pull/921))
* Pin actions/github-script to exact commit SHA for improved security ([#920](https://github.com/opensearch-project/job-scheduler/pull/920))

### Maintenance

* Bump actions/github-script from 8 to 9 ([#908](https://github.com/opensearch-project/job-scheduler/pull/908))
* Bump gradle-wrapper from 9.4.1 to 9.5.0 ([#913](https://github.com/opensearch-project/job-scheduler/pull/913))
