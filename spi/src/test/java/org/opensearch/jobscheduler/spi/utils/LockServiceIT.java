/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jobscheduler.spi.utils;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.collect.ImmutableOpenMap;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.jobscheduler.spi.JobDocVersion;
import org.opensearch.jobscheduler.spi.JobExecutionContext;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.schedule.Schedule;
import org.opensearch.action.ActionListener;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.Before;
import org.junit.Ignore;
import org.mockito.Mockito;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public class LockServiceIT extends OpenSearchIntegTestCase {

    static final String JOB_ID = "test_job_id";
    static final String JOB_INDEX_NAME = "test_job_index_name";
    static final long LOCK_DURATION_SECONDS = 60;
    static final ScheduledJobParameter TEST_SCHEDULED_JOB_PARAM = new ScheduledJobParameter() {

        @Override public XContentBuilder toXContent(final XContentBuilder builder, final Params params) {
            return builder;
        }

        @Override public String getName() {
            return null;
        }

        @Override public Instant getLastUpdateTime() {
            return null;
        }

        @Override public Instant getEnabledTime() {
            return null;
        }

        @Override public Schedule getSchedule() {
            return null;
        }

        @Override public boolean isEnabled() {
            return false;
        }

        @Override public Long getLockDurationSeconds() {
            return LOCK_DURATION_SECONDS;
        }
    };

    private ClusterService clusterService;

    @Before
    public void setup() {
        // the test cluster is an external cluster instead of internal cluster in new test framework,
        // thus the OpenSearchIntegTestCase.clusterService() will throw exception.
        this.clusterService = Mockito.mock(ClusterService.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(this.clusterService.state().routingTable().hasIndex(".opendistro-job-scheduler-lock"))
                .thenReturn(false)
                .thenReturn(true);
        try {
            /*
                This sample index will be what is read in by the indexMetadata.mapping().sourceAsMap() call in
                SchemaUtils.shouldUpdateIndex. Once this is read -- the shouldUpdate ought to return true.
                This will cause the check and update to create a put request to update the mapping to the new mapping.
                If the put request fails, then there is a problem with the way checkAndUpdate is working and that should
                be investigated.

                All this will run when any of the tests that acquire a lock run because the acquisition of the lock
                triggers the call the LockService.checkAndUpdateMappings.

             */
            String indexContent = "{\"testIndex\":{\"settings\":{\"index\":{\"creation_date\":\"1558407515699\"," +
                    "\"number_of_shards\":\"1\",\"number_of_replicas\":\"1\",\"uuid\":\"t-VBBW6aR6KpJ3XP5iISOA\"," +
                    "\"version\":{\"created\":\"6040399\"},\"provided_name\":\"data_test\"}},\"mapping_version\":123," +
                    "\"settings_version\":123,\"mappings\":{\"_doc\":{\"_meta\":{\"schema_version\":1},\"properties\":" +
                    "{\"name\":{\"type\":\"keyword\"}}}}}}";
            XContentParser parser = createParser(XContentType.JSON.xContent(), indexContent);
            IndexMetadata index = IndexMetadata.fromXContent(parser);
            IndexMetadata indexMetadata = Mockito.mock(IndexMetadata.class, Mockito.RETURNS_DEEP_STUBS);
            Mockito.when(indexMetadata.mapping().sourceAsMap()).thenReturn(index.mapping().sourceAsMap());

            // create an indexMetdata map with the index Content
            Map<String, IndexMetadata> indexMetadataHashMap = new HashMap<>();
            indexMetadataHashMap.put(".opendistro-job-scheduler-lock", indexMetadata);
            ImmutableOpenMap.Builder<String, IndexMetadata> mapBuilder = new ImmutableOpenMap.Builder<>();
            mapBuilder.putAll(indexMetadataHashMap);
            ImmutableOpenMap<String, IndexMetadata> indexMetadataMap = mapBuilder.build();
            Mockito.when(this.clusterService.state().metadata().indices())
                    .thenReturn(indexMetadataMap);
        }
        catch (IOException e) {
            fail(e.getMessage());
        }
    }

    public void testSanity() throws Exception {
        String uniqSuffix = "_sanity";
        CountDownLatch latch = new CountDownLatch(1);
        LockService lockService = new LockService(client(), this.clusterService);
        final JobExecutionContext context = new JobExecutionContext(Instant.now(), new JobDocVersion(0, 0, 0),
                lockService, JOB_INDEX_NAME + uniqSuffix, JOB_ID + uniqSuffix);
        Instant testTime = Instant.now();
        lockService.setTime(testTime);
        lockService.acquireLock(TEST_SCHEDULED_JOB_PARAM, context, ActionListener.wrap(
                lock -> {
                    // assertEquals("", lock.toString());
                    assertNotNull("Expected to successfully grab lock.", lock);
                    assertEquals("job_id does not match.", JOB_ID + uniqSuffix, lock.getJobId());
                    assertEquals("job_index_name does not match.", JOB_INDEX_NAME + uniqSuffix, lock.getJobIndexName());
                    assertEquals("lock_id does not match.", LockModel.generateLockId(JOB_INDEX_NAME + uniqSuffix,
                            JOB_ID + uniqSuffix), lock.getLockId());
                    assertEquals("lock_duration_seconds does not match.", LOCK_DURATION_SECONDS, lock.getLockDurationSeconds());
                    assertEquals("lock_time does not match.", testTime.getEpochSecond(), lock.getLockTime().getEpochSecond());
                    assertFalse("Lock should not be released.", lock.isReleased());
                    assertFalse("Lock should not expire.", lock.isExpired());
                    lockService.release(lock, ActionListener.wrap(
                            released -> {
                                assertTrue("Failed to release lock.", released);
                                lockService.deleteLock(lock.getLockId(), ActionListener.wrap(
                                        deleted -> {
                                            assertTrue("Failed to delete lock.", deleted);
                                            latch.countDown();
                                        },
                                        exception -> fail(exception.getMessage())
                                ));
                            },
                            exception -> fail(exception.getMessage())
                    ));
                },
                exception -> fail(exception.getMessage())
        ));
        latch.await(5L, TimeUnit.SECONDS);
    }

    public void testSanityResourceLock() throws Exception {
        String uniqSuffix = "_sanity";
        CountDownLatch latch = new CountDownLatch(1);
        LockService lockService = new LockService(client(), this.clusterService);
        String jobIndexName = JOB_INDEX_NAME + uniqSuffix;
        final JobExecutionContext context = new JobExecutionContext(Instant.now(), new JobDocVersion(0, 0, 0),
                lockService, jobIndexName, JOB_ID + uniqSuffix);
        Instant testTime = Instant.now();
        lockService.setTime(testTime);
        Map<String, Object> resource = new HashMap<>();
        resource.put("node_name", "node");
        String expectedLockName = LockModel.generateResourceLockId(jobIndexName, "shrink", resource);
        lockService.acquireLockOnResource(context, (long) 1000, "shrink", resource, ActionListener.wrap(
                lock -> {
                    assertNotNull("Expected to successfully grab lock.", lock);
                    assertEquals("lock name does not match", expectedLockName, lock.getLockId());
                    assertEquals("lock_time does not match.", testTime.getEpochSecond(), lock.getLockTime().getEpochSecond());
                    assertFalse("Lock should not be released.", lock.isReleased());
                    assertFalse("Lock should not expire.", lock.isExpired());
                    lockService.release(lock, ActionListener.wrap(
                            released -> {
                                assertTrue("Failed to release lock.", released);
                                lockService.deleteLock(lock.getLockId(), ActionListener.wrap(
                                        deleted -> {
                                            assertTrue("Failed to delete lock.", deleted);
                                            latch.countDown();
                                        },
                                        exception -> {
                                            fail(exception.getMessage());
                                        }
                                ));
                            },
                            exception -> {
                                fail(exception.getMessage());
                            }
                    ));
                },
                exception -> {
                    fail(exception.getMessage());
                }
        ));
        latch.await(5L, TimeUnit.SECONDS);
    }

    public void testSecondAcquireResourceLockFail() throws Exception {
        String uniqSuffix = "_second_acquire";
        CountDownLatch latch = new CountDownLatch(1);
        LockService lockService = new LockService(client(), this.clusterService);
        final JobExecutionContext context = new JobExecutionContext(Instant.now(), new JobDocVersion(0, 0, 0),
                lockService, JOB_INDEX_NAME + uniqSuffix, JOB_ID + uniqSuffix);
        Map<String, Object> resource = new HashMap<>();
        resource.put("node_name", "node");
        lockService.acquireLockOnResource(context, (long) 1000, "shrink", resource, ActionListener.wrap(
                lock -> {
                    assertNotNull("Expected to successfully grab lock", lock);
                    lockService.acquireLockOnResource(context, (long) 1000, "shrink", resource, ActionListener.wrap(
                            lock2 -> {
                                assertNull("Expected to failed to get lock.", lock2);
                                lockService.release(lock, ActionListener.wrap(
                                        released -> {
                                            assertTrue("Failed to release lock.", released);
                                            lockService.deleteLock(lock.getLockId(), ActionListener.wrap(
                                                    deleted -> {
                                                        assertTrue("Failed to delete lock.", deleted);
                                                        latch.countDown();
                                                    },
                                                    exception -> fail(exception.getMessage())
                                            ));
                                        },
                                        exception -> fail(exception.getMessage())
                                ));
                            },
                            exception -> fail(exception.getMessage())
                    ));
                },
                exception -> fail(exception.getMessage())
        ));
        latch.await(10L, TimeUnit.SECONDS);
    }

    public void testSecondAcquireLockFail() throws Exception {
        String uniqSuffix = "_second_acquire";
        CountDownLatch latch = new CountDownLatch(1);
        LockService lockService = new LockService(client(), this.clusterService);
        final JobExecutionContext context = new JobExecutionContext(Instant.now(), new JobDocVersion(0, 0, 0),
                lockService, JOB_INDEX_NAME + uniqSuffix, JOB_ID + uniqSuffix);

        lockService.acquireLock(TEST_SCHEDULED_JOB_PARAM, context, ActionListener.wrap(
                lock -> {
                    assertNotNull("Expected to successfully grab lock", lock);
                    lockService.acquireLock(TEST_SCHEDULED_JOB_PARAM, context, ActionListener.wrap(
                            lock2 -> {
                                assertNull("Expected to failed to get lock.", lock2);
                                lockService.release(lock, ActionListener.wrap(
                                        released -> {
                                            assertTrue("Failed to release lock.", released);
                                            lockService.deleteLock(lock.getLockId(), ActionListener.wrap(
                                                    deleted -> {
                                                        assertTrue("Failed to delete lock.", deleted);
                                                        latch.countDown();
                                                    },
                                                    exception -> fail(exception.getMessage())
                                            ));
                                        },
                                        exception -> fail(exception.getMessage())
                                ));
                            },
                            exception -> fail(exception.getMessage())
                    ));
                },
                exception -> fail(exception.getMessage())
        ));
        latch.await(10L, TimeUnit.SECONDS);
    }

    public void testLockReleasedAndAcquired() throws Exception {
        String uniqSuffix = "_lock_release+acquire";
        CountDownLatch latch = new CountDownLatch(1);
        LockService lockService = new LockService(client(), this.clusterService);
        final JobExecutionContext context = new JobExecutionContext(Instant.now(), new JobDocVersion(0, 0, 0),
                lockService, JOB_INDEX_NAME + uniqSuffix, JOB_ID + uniqSuffix);

        lockService.acquireLock(TEST_SCHEDULED_JOB_PARAM, context, ActionListener.wrap(
                lock -> {
                    assertNotNull("Expected to successfully grab lock", lock);
                    lockService.release(lock, ActionListener.wrap(
                            released -> {
                                assertTrue("Failed to release lock.", released);
                                lockService.acquireLock(TEST_SCHEDULED_JOB_PARAM, context, ActionListener.wrap(
                                        lock2 -> {
                                            assertNotNull("Expected to successfully grab lock2", lock2);
                                            lockService.release(lock2, ActionListener.wrap(
                                                    released2 -> {
                                                        assertTrue("Failed to release lock2.", released2);
                                                        lockService.deleteLock(lock2.getLockId(), ActionListener.wrap(
                                                                deleted -> {
                                                                    assertTrue("Failed to delete lock2.", deleted);
                                                                    latch.countDown();
                                                                },
                                                                exception -> fail(exception.getMessage())
                                                        ));
                                                    },
                                                    exception -> fail(exception.getMessage())
                                            ));
                                        },
                                        exception -> fail(exception.getMessage())
                                ));
                            },
                            exception -> fail(exception.getMessage())
                    ));
                },
                exception -> fail(exception.getMessage())
        ));
        latch.await(5L, TimeUnit.SECONDS);
    }

    public void testResourceLockReleasedAndAcquired() throws Exception {
        String uniqSuffix = "_resource_lock_release+acquire";
        CountDownLatch latch = new CountDownLatch(1);
        LockService lockService = new LockService(client(), this.clusterService);
        final JobExecutionContext context = new JobExecutionContext(Instant.now(), new JobDocVersion(0, 0, 0),
                lockService, JOB_INDEX_NAME + uniqSuffix, JOB_ID + uniqSuffix);
        Map<String, Object> resource = new HashMap<>();
        resource.put("node_name", "node");
        lockService.acquireLockOnResource(context, (long) 1000, "shrink", resource, ActionListener.wrap(
                lock -> {
                    assertNotNull("Expected to successfully grab lock", lock);
                    lockService.release(lock, ActionListener.wrap(
                            released -> {
                                assertTrue("Failed to release lock.", released);
                                lockService.acquireLockOnResource(context, (long) 1000, "shrink", resource, ActionListener.wrap(
                                        lock2 -> {
                                            assertNotNull("Expected to successfully grab lock2", lock2);
                                            lockService.release(lock2, ActionListener.wrap(
                                                    released2 -> {
                                                        assertTrue("Failed to release lock2.", released2);
                                                        lockService.deleteLock(lock2.getLockId(), ActionListener.wrap(
                                                                deleted -> {
                                                                    assertTrue("Failed to delete lock2.", deleted);
                                                                    latch.countDown();
                                                                },
                                                                exception -> fail(exception.getMessage())
                                                        ));
                                                    },
                                                    exception -> fail(exception.getMessage())
                                            ));
                                        },
                                        exception -> fail(exception.getMessage())
                                ));
                            },
                            exception -> fail(exception.getMessage())
                    ));
                },
                exception -> fail(exception.getMessage())
        ));
        latch.await(5L, TimeUnit.SECONDS);
    }

    public void testLockExpired() throws Exception {
        String uniqSuffix = "_lock_expire";
        CountDownLatch latch = new CountDownLatch(1);
        LockService lockService = new LockService(client(), this.clusterService);
        // Set lock time in the past.
        lockService.setTime(Instant.now().minus(Duration.ofSeconds(LOCK_DURATION_SECONDS + LOCK_DURATION_SECONDS)));
        final JobExecutionContext context = new JobExecutionContext(Instant.now(), new JobDocVersion(0, 0, 0),
                lockService, JOB_INDEX_NAME + uniqSuffix, JOB_ID + uniqSuffix);


        lockService.acquireLock(TEST_SCHEDULED_JOB_PARAM, context, ActionListener.wrap(
                lock -> {
                    assertNotNull("Expected to successfully grab lock", lock);
                    // Set lock back to current time to make the lock expire.
                    lockService.setTime(null);
                    lockService.acquireLock(TEST_SCHEDULED_JOB_PARAM, context, ActionListener.wrap(
                            lock2 -> {
                                assertNotNull("Expected to successfully grab lock", lock2);
                                lockService.release(lock, ActionListener.wrap(
                                        released -> {
                                            assertFalse("Expected to fail releasing lock.", released);
                                            lockService.release(lock2, ActionListener.wrap(
                                                    released2 -> {
                                                        assertTrue("Expecting to successfully release lock.", released2);
                                                        lockService.deleteLock(lock.getLockId(), ActionListener.wrap(
                                                                deleted -> {
                                                                    assertTrue("Failed to delete lock.", deleted);
                                                                    latch.countDown();
                                                                },
                                                                exception -> fail(exception.getMessage())
                                                        ));
                                                    },
                                                    exception -> fail(exception.getMessage())
                                            ));
                                        },
                                        exception -> fail(exception.getMessage())
                                ));
                            },
                            exception -> fail(exception.getMessage())
                    ));
                },
                exception -> fail(exception.getMessage())
        ));
        latch.await(5L, TimeUnit.SECONDS);
    }

    public void testResourceLockExpired() throws Exception {
        String uniqSuffix = "_lock_expire";
        CountDownLatch latch = new CountDownLatch(1);
        LockService lockService = new LockService(client(), this.clusterService);
        // Set lock time in the past.
        lockService.setTime(Instant.now().minus(Duration.ofSeconds(LOCK_DURATION_SECONDS + LOCK_DURATION_SECONDS)));
        final JobExecutionContext context = new JobExecutionContext(Instant.now(), new JobDocVersion(0, 0, 0),
                lockService, JOB_INDEX_NAME + uniqSuffix, JOB_ID + uniqSuffix);

        Map<String, Object> resource = new HashMap<>();
        resource.put("node_name", "node");

        lockService.acquireLockOnResource(context, LOCK_DURATION_SECONDS, "shrink", resource, ActionListener.wrap(
                lock -> {
                    assertNotNull("Expected to successfully grab lock", lock);
                    // Set lock back to current time to make the lock expire.
                    lockService.setTime(null);
                    lockService.acquireLockOnResource(context, LOCK_DURATION_SECONDS, "shrink", resource, ActionListener.wrap(
                            lock2 -> {
                                assertNotNull("Expected to successfully grab lock", lock2);
                                lockService.release(lock, ActionListener.wrap(
                                        released -> {
                                            assertFalse("Expected to fail releasing lock.", released);
                                            lockService.release(lock2, ActionListener.wrap(
                                                    released2 -> {
                                                        assertTrue("Expecting to successfully release lock.", released2);
                                                        lockService.deleteLock(lock.getLockId(), ActionListener.wrap(
                                                                deleted -> {
                                                                    assertTrue("Failed to delete lock.", deleted);
                                                                    latch.countDown();
                                                                },
                                                                exception -> fail(exception.getMessage())
                                                        ));
                                                    },
                                                    exception -> fail(exception.getMessage())
                                            ));
                                        },
                                        exception -> fail(exception.getMessage())
                                ));
                            },
                            exception -> fail(exception.getMessage())
                    ));
                },
                exception -> fail(exception.getMessage())
        ));
        latch.await(5L, TimeUnit.SECONDS);
    }

    public void testDeleteLockWithOutIndexCreation() throws Exception {
        ImmutableOpenMap<String, IndexMetadata> emptyMap = new ImmutableOpenMap.Builder<String, IndexMetadata>().build();
        Mockito.when(this.clusterService.state().metadata().indices()).thenReturn(emptyMap);
        CountDownLatch latch = new CountDownLatch(1);
        LockService lockService = new LockService(client(), this.clusterService);
        lockService.deleteLock("NonExistingLockId", ActionListener.wrap(
                deleted -> {
                    assertTrue("Failed to delete lock.", deleted);
                    latch.countDown();
                },
                exception -> fail(exception.getMessage())
        ));
        latch.await(5L, TimeUnit.SECONDS);
    }

    public void testDeleteNonExistingLock() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        LockService lockService = new LockService(client(), this.clusterService);
        lockService.createLockIndex(ActionListener.wrap(
                created -> {
                    if (created) {
                        lockService.deleteLock("NonExistingLockId", ActionListener.wrap(
                                deleted -> {
                                    assertTrue("Failed to delete lock.", deleted);
                                    latch.countDown();
                                },
                                exception -> fail(exception.getMessage())
                        ));

                    } else {
                        fail("Failed to create lock index.");
                    }
                },
                exception -> fail(exception.getMessage())
        ));
        latch.await(5L, TimeUnit.SECONDS);
    }

    private static volatile AtomicInteger multiThreadCreateLockCounter = new AtomicInteger(0);

    @Ignore
    public void testMultiThreadCreateLock() throws Exception {
        String uniqSuffix = "_multi_thread_create";
        CountDownLatch latch = new CountDownLatch(1);
        final LockService lockService = new LockService(client(), this.clusterService);
        final JobExecutionContext context = new JobExecutionContext(Instant.now(), new JobDocVersion(0, 0, 0),
                lockService, JOB_INDEX_NAME + uniqSuffix, JOB_ID + uniqSuffix);


        lockService.createLockIndex(ActionListener.wrap(
                created -> {
                    if (created) {
                        ExecutorService executor = Executors.newFixedThreadPool(3);
                        final AtomicReference<LockModel> lockModelAtomicReference = new AtomicReference<>(null);
                        Callable<Boolean> callable = () -> {
                            CountDownLatch callableLatch = new CountDownLatch(1);
                            lockService.acquireLock(TEST_SCHEDULED_JOB_PARAM, context, ActionListener.wrap(
                                    lock -> {
                                        if (lock != null) {
                                            lockModelAtomicReference.set(lock);
                                            multiThreadCreateLockCounter.getAndAdd(1);
                                        }
                                        callableLatch.countDown();
                                    },
                                    exception -> fail(exception.getMessage())
                            ));
                            callableLatch.await(5L, TimeUnit.SECONDS);
                            return true;
                        };

                        List<Callable<Boolean>> callables = Arrays.asList(
                                callable,
                                callable,
                                callable
                        );

                        executor.invokeAll(callables)
                                .forEach(future -> {
                                    try {
                                        future.get();
                                    } catch (Exception e) {
                                        fail(e.getMessage());
                                    }
                                });
                        executor.shutdown();
                        executor.awaitTermination(10L, TimeUnit.SECONDS);

                        assertEquals("There should be only one that grabs the lock.", 1, multiThreadCreateLockCounter.get());

                        final LockModel lock = lockModelAtomicReference.get();
                        assertNotNull("Expected to successfully grab lock", lock);
                        lockService.release(lock, ActionListener.wrap(
                                released -> {
                                    assertTrue("Failed to release lock.", released);
                                    lockService.deleteLock(lock.getLockId(), ActionListener.wrap(
                                            deleted -> {
                                                assertTrue("Failed to delete lock.", deleted);
                                                latch.countDown();
                                            },
                                            exception -> fail(exception.getMessage())
                                    ));
                                },
                                exception -> fail(exception.getMessage())
                        ));
                    } else {
                        fail("Failed to create lock index.");
                    }
                },
                exception -> fail(exception.getMessage())
        ));
        assertTrue("Test timed out - possibly leaked into other tests", latch.await(30L, TimeUnit.SECONDS));
    }

    private static volatile AtomicInteger multiThreadAcquireLockCounter = new AtomicInteger(0);

    @Ignore
    public void testMultiThreadAcquireLock() throws Exception {
        String uniqSuffix = "_multi_thread_acquire";
        CountDownLatch latch = new CountDownLatch(1);
        final LockService lockService = new LockService(client(), this.clusterService);
        final JobExecutionContext context = new JobExecutionContext(Instant.now(), new JobDocVersion(0, 0, 0),
                lockService, JOB_INDEX_NAME + uniqSuffix, JOB_ID + uniqSuffix);

        lockService.createLockIndex(ActionListener.wrap(
                created -> {
                    if (created) {
                        // Set lock time in the past.
                        lockService.setTime(Instant.now().minus(Duration.ofSeconds(LOCK_DURATION_SECONDS + LOCK_DURATION_SECONDS)));
                        lockService.acquireLock(TEST_SCHEDULED_JOB_PARAM, context, ActionListener.wrap(
                                createdLock -> {
                                    assertNotNull(createdLock);
                                    // Set lock back to current time to make the lock expire.
                                    lockService.setTime(null);

                                    ExecutorService executor = Executors.newFixedThreadPool(3);
                                    final AtomicReference<LockModel> lockModelAtomicReference = new AtomicReference<>(null);
                                    Callable<Boolean> callable = () -> {
                                        CountDownLatch callableLatch = new CountDownLatch(1);
                                        lockService.acquireLock(TEST_SCHEDULED_JOB_PARAM, context, ActionListener.wrap(
                                                lock -> {
                                                    if (lock != null) {
                                                        lockModelAtomicReference.set(lock);
                                                        Integer test = multiThreadAcquireLockCounter.getAndAdd(1);
                                                    }
                                                    callableLatch.countDown();
                                                },
                                                exception -> fail(exception.getMessage())
                                        ));
                                        callableLatch.await(5L, TimeUnit.SECONDS);
                                        return true;
                                    };

                                    List<Callable<Boolean>> callables = Arrays.asList(
                                            callable,
                                            callable,
                                            callable
                                    );

                                    executor.invokeAll(callables);
                                    executor.shutdown();
                                    executor.awaitTermination(10L, TimeUnit.SECONDS);

                                    assertEquals("There should be only one that grabs the lock.", 1, multiThreadAcquireLockCounter.get());

                                    final LockModel lock = lockModelAtomicReference.get();
                                    assertNotNull("Expected to successfully grab lock", lock);
                                    lockService.release(lock, ActionListener.wrap(
                                            released -> {
                                                assertTrue("Failed to release lock.", released);
                                                lockService.deleteLock(lock.getLockId(), ActionListener.wrap(
                                                        deleted -> {
                                                            assertTrue("Failed to delete lock.", deleted);
                                                            latch.countDown();
                                                        },
                                                        exception -> fail(exception.getMessage())
                                                ));
                                            },
                                            exception -> fail(exception.getMessage())
                                    ));
                                },
                                exception -> fail(exception.getMessage())
                        ));
                    } else {
                        fail("Failed to create lock index.");
                    }
                },
                exception -> fail(exception.getMessage())
        ));
        assertTrue("Test timed out - possibly leaked into other tests", latch.await(30L, TimeUnit.SECONDS));
    }

    public void testRenewLock() throws Exception {
        String uniqSuffix = "_lock_renew";
        CountDownLatch latch = new CountDownLatch(1);
        LockService lockService = new LockService(client(), this.clusterService);
        final JobExecutionContext context = new JobExecutionContext(Instant.now(), new JobDocVersion(0, 0, 0),
                lockService, JOB_INDEX_NAME + uniqSuffix, JOB_ID + uniqSuffix);

        lockService.acquireLock(TEST_SCHEDULED_JOB_PARAM, context, ActionListener.wrap(
                lock -> {
                    assertNotNull("Expected to successfully grab lock", lock);
                    // Set the time of LockService (the 'lockTime' of acquired locks) to a fixed time.
                    Instant now = Instant.now();
                    lockService.setTime(now);
                    lockService.renewLock(lock, ActionListener.wrap(
                            renewedLock -> {
                                assertNotNull("Expected to successfully renew lock", renewedLock);
                                assertEquals("lock_time is expected to be the renewal time.", now, renewedLock.getLockTime());
                                assertEquals("lock_duration is expected to be unchanged.",
                                        lock.getLockDurationSeconds(), renewedLock.getLockDurationSeconds());
                                lockService.release(lock, ActionListener.wrap(
                                        released -> {
                                            assertTrue("Failed to release lock.", released);
                                            lockService.deleteLock(lock.getLockId(), ActionListener.wrap(
                                                    deleted -> {
                                                        assertTrue("Failed to delete lock.", deleted);
                                                        latch.countDown();
                                                    },
                                                    exception -> fail(exception.getMessage())
                                            ));
                                        },
                                        exception -> fail(exception.getMessage())
                                ));
                            },
                            exception -> fail(exception.getMessage())
                    ));
                },
                exception -> fail(exception.getMessage())
        ));
        latch.await(5L, TimeUnit.SECONDS);
    }
}
