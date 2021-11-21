# Service control

Scylla starts numerous sharded service that depend on each
other for proper operation.

The `service_ctl` subsystem defines a `base_controller` class
that controls the lifecycle of a service, handling state
transitions like `start` -> `serve` -> `drain` -> `stop`, and
tracks the inter-dependencies between services by keeping
a list of dependencies that the service depends on
and a list of dependants that depend on the controlled service.

The `services_controller` object tracks all service controllers
and in particular it maintains a `top` and `bottom` list of
service that either have no other service depend on them (top)
or depend on no other services (bottom).

These are use to initiate state transition that need to be
executed either top-down or bottom-up.

## Service lifecycle states

Here is a list of the high-level service states:

   * "initialized" - is the initial state of the service controller.

     `start()` moves the service to the `started` state in bottom-up order.

   * `started` - services are constructed and become ready for service.

     `serve()` moves the service to the `serving` state in bottom-up order.

   * `serving` - the service starts serving.  This may involve e.g. registering
     rpc message handlers.

     `drain()` moves the service either back to the `started` state, or to
     the `drained` state, if called on `shutdown`.  Both ways are done in top-down order.

   * `drained` - the service is no longer serving and is ready for shutdown.

     `shutdown()` moves the service into the `shutdown` state in top-down order.

   * `shutdown` - the service is shut down and is ready to be stopped.

     `stop()` moves the service into the `stopped` state in top-down order.

   * `stopped` - the service is ready to be destroyed.

Note that `drain()` and `shutdown()` are called implictly as needed during `stop()`.

## Sharded service_ctl

`sharded_service_ctl<Service>` holds a `sharded<service>` object.
It is used for most services in scylla, hence its importance.

Starting a `sharded_service_ctl` is typically done by calling
the `sharded<Service>::start()` function to instatiate the Service
on all shards.

Then, some services implement a `Service::start()` method that
may be called either by the `start_func` or by the `serve_func`,
depending if the perform additional service initialization,
or start the service function (that may be later be stopped by the `drain_func`)

