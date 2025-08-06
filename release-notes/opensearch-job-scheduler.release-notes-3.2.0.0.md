## Version 3.2.0 Release Notes

Compatible with OpenSearch and OpenSearch Dashboards version 3.2.0

### Features
* Adds REST API to list jobs with an option to list them per node ([#786](https://github.com/opensearch-project/job-scheduler/pull/786))
* Support defining IntervalSchedule in seconds ([#796](https://github.com/opensearch-project/job-scheduler/pull/796))
* Rest API to list all locks with option to get a specific lock ([#802](https://github.com/opensearch-project/job-scheduler/pull/802))

### Enhancements
* Make Lock service not final ([#792](https://github.com/opensearch-project/job-scheduler/pull/792))
* Move info about delay to the the schedule portion in List Jobs API ([#801](https://github.com/opensearch-project/job-scheduler/pull/801))

### Bug Fixes
* Ensure that dates are serialized in TransportGetScheduledInfoAction.nodeOperation ([#793](https://github.com/opensearch-project/job-scheduler/pull/793))

### Infrastructure
* Add new Github workflow to run sample plugin tests in cluster with multiple nodes ([#795](https://github.com/opensearch-project/job-scheduler/pull/795))
* Add test that disables watcher job and verifies that it stops running, but metadata exists ([#797](https://github.com/opensearch-project/job-scheduler/pull/797))
* Bump gradle to 8.14 and use jdk 24 in ci workflow ([#798](https://github.com/opensearch-project/job-scheduler/pull/798))

### Refactoring
* Use Text Blocks when defining multi-line strings ([#790](https://github.com/opensearch-project/job-scheduler/pull/790))