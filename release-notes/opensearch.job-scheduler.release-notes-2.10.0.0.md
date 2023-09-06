## Version 2.10.0.0

Compatible with OpenSearch 2.10.0

### Added
* Setting JobSweeper search preference against primary shard ([#483](https://github.com/opensearch-project/job-scheduler/pull/483)) ([#485](https://github.com/opensearch-project/job-scheduler/pull/485))
* Converts .opendistro-job-scheduler-lock index into a system index ([#478](https://github.com/opensearch-project/job-scheduler/pull/478))
* Public snapshots on all release branches ([#475](https://github.com/opensearch-project/job-scheduler/pull/475)) ([#476](https://github.com/opensearch-project/job-scheduler/pull/476))

### Fixed
* Call listner.onFailure when lock creation failed ([#435](https://github.com/opensearch-project/job-scheduler/pull/435)) ([#443](https://github.com/opensearch-project/job-scheduler/pull/443))

### Maintenance
* Update packages according to a change in OpenSearch core ([#422](https://github.com/opensearch-project/job-scheduler/pull/422)) ([#431](https://github.com/opensearch-project/job-scheduler/pull/431))
* Xcontent changes to ODFERestTestCase ([#440](https://github.com/opensearch-project/job-scheduler/pull/440))
* Update LifecycleListener import ([#445](https://github.com/opensearch-project/job-scheduler/pull/445))
* Bump slf4j-api to 2.0.7, ospackage to 11.4.0, google-java-format to 1.17.0, guava to 32.1.2-jre and spotless to 6.20.0 ([#453](https://github.com/opensearch-project/job-scheduler/pull/453))
* Fixing Strings import ([#459](https://github.com/opensearch-project/job-scheduler/pull/459))
* bump com.cronutils:cron-utils from 9.2.0 to 9.2.1 ([#458](https://github.com/opensearch-project/job-scheduler/pull/458))
* React to changes in ActionListener and ActionFuture ([#467](https://github.com/opensearch-project/job-scheduler/pull/467))
* bump com.diffplug.spotless from 6.20.0 to 6.21.0 ([#484](https://github.com/opensearch-project/job-scheduler/pull/484))