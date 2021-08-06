[![Test and Build Workflow](https://github.com/opensearch-project/job-scheduler/workflows/Build%20and%20Test/badge.svg)](https://github.com/opensearch-project/job-scheduler/actions)
[![codecov](https://codecov.io/gh/opensearch-project/job-scheduler/branch/main/graph/badge.svg)](https://codecov.io/gh/opensearch-project/job-scheduler)
![PRs welcome!](https://img.shields.io/badge/PRs-welcome!-success)

<img src="https://opensearch.org/assets/brand/SVG/Logo/opensearch_logo_default.svg" height="64px"/>

- [OpenSearch Job Scheduler](#opensearch-job-scheduler)
- [Contributing](#contributing)
- [Getting Help](#getting-help)
- [Code of Conduct](#code-of-conduct)
- [Security](#security)
- [License](#license)
- [Copyright](#copyright)

# OpenSearch Job Scheduler

OpenSearch JobScheduler plugin provides a framework for OpenSearch plugin
developers to schedule periodical jobs running within OpenSearch nodes. You can schedule jobs
by specify an interval, or using Unix Cron expression to define more flexible schedule to execute
your job.

OpenSearch plugin developers can easily extend JobScheduler plugin to schedule jobs like running
aggregation query against raw data and save the aggregated data into a new index every hour, or keep
monitoring the shard allocation by calling OpenSearch API and post the output to a Webhook.

## Contributing

See [developer guide](DEVELOPER_GUIDE.md) and [how to contribute to this project](CONTRIBUTING.md).

## Getting Help

If you find a bug, or have a feature request, please don't hesitate to open an issue in this repository.

For more information, see [project website](https://opensearch.org/) and [documentation](https://opensearch.org/docs/). If you need help and are unsure where to open an issue, try [forums](https://discuss.opendistrocommunity.dev/).

## Code of Conduct

This project has adopted the [Amazon Open Source Code of Conduct](CODE_OF_CONDUCT.md). For more information see the [Code of Conduct FAQ](https://aws.github.io/code-of-conduct-faq), or contact [opensource-codeofconduct@amazon.com](mailto:opensource-codeofconduct@amazon.com) with any additional questions or comments.

## Security

If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public GitHub issue.

## License

This project is licensed under the [Apache v2.0 License](./LICENSE)

## Copyright

Copyright 2020-2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.