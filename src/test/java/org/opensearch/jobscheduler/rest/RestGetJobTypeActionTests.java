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
// public class RestGetJobTypeActionTests extends OpenSearchTestCase {
//
// private RestGetJobTypeAction action;
//
// public Map<String, JobDetails> indexToJobDetails;
//
// private String getJobTypePath;
//
// @Before
// public void setUp() throws Exception {
// super.setUp();
// indexToJobDetails = new HashMap<>();
// action = new RestGetJobTypeAction(indexToJobDetails);
// getJobTypePath = String.format(Locale.ROOT, "%s/%s", JobSchedulerPlugin.JS_BASE_URI, "_get/_job_type");
// }
//
// public void testGetNames() {
// String name = action.getName();
// assertEquals(action.GET_JOB_TYPE_ACTION, name);
// }
//
// public void testGetRoutes() {
// List<RestHandler.Route> routes = action.routes();
//
// assertEquals(getJobTypePath, routes.get(0).getPath());
// }
//
// public void testPrepareRequest() throws IOException {
//
// String content = "{\"job_type\":\"demo_job_type\",\"extension_id\":\"extension_id\"}";
// Map<String, String> params = new HashMap<>();
// FakeRestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.PUT)
// .withPath(getJobTypePath)
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
