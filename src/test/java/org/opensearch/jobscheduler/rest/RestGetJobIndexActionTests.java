/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.jobscheduler.rest;

//
// public class RestGetJobIndexActionTests extends OpenSearchTestCase {
//
// private RestGetJobIndexAction action;
//
// public Map<String, JobDetails> indexToJobDetails;
//
// private String getJobIndexPath;
//
// @Before
// public void setUp() throws Exception {
// super.setUp();
// indexToJobDetails = new HashMap<>();
// action = new RestGetJobIndexAction(indexToJobDetails);
// getJobIndexPath = String.format(Locale.ROOT, "%s/%s", JobSchedulerPlugin.JS_BASE_URI, "_get/_job_index");
// }
//
// public void testGetNames() {
// String name = action.getName();
// assertEquals(action.GET_JOB_INDEX_ACTION, name);
// }
//
// public void testGetRoutes() {
// List<RestHandler.Route> routes = action.routes();
//
// assertEquals(getJobIndexPath, routes.get(0).getPath());
// }
//
// public void testPrepareRequest() throws IOException {
//
// String content =
// "{\"job_index\":\"demo_job_index\",\"job_runner_action\":\"action\",\"job_parser_action\":\"parser_action\",\"extension_id\":\"extension_id\"}";
// Map<String, String> params = new HashMap<>();
// FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
// .withPath(getJobIndexPath)
// .withParams(params)
// .withContent(new BytesArray(content), XContentType.JSON)
// .build();
//
// final FakeRestChannel channel = new FakeRestChannel(request, true, 0);
// action.prepareRequest(request, mock(NodeClient.class));
//
// assertThat(channel.responses().get(), equalTo(0));
// assertThat(channel.errors().get(), equalTo(0));
// }
// }
