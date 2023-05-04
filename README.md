# Brod

A small wrapper library which provides functionality for executing multiple
Kafka producers in parallel. The underlying Kafka functionality is provided
by the [rust-rdkafka](https://github.com/fede1024/rust-rdkafka) library.
Furthermore, in order to support the use of asynchronous *FutureProducer*s,
the [tokio](https://tokio.rs/) runtime is used.

(Named after [Max Brod](https://en.wikipedia.org/wiki/Max_Brod), a close friend 
of Franz Kafka, who saved Kafka's literary work.)

## Documentation
If Cargo, the Rust package manager, is installed, local documentation can be generated 
by executing the command `cargo docs --open`, which will build the documentation
and open them in the system's default web browser.

## Features
This library enables developers to write regular, sequential worker functions 
utilizing Kafka producers to send messages, and have these function execute in
parallel, using the functionality provided by the library.

Both the synchronous *BaseProducer* and the asynchronous *FutureProducer* can
be used, with the corresponding functionality provided by separate modules,
*sync_prod* and *async_prod*, respectively.

## Usage
The intended use case of the library is that one writes a function which utilizes 
a Kafka producer to send some messages, optionally carrying out some computational
work in order to generate such messages. This function is then passed as a function
pointer to one of the functions provided by the library which have functionality for 
executing the passed worker function in parallel.

It should be noted that in order to be as generic as possible, and in order to provide 
functionality which is as flexible as possible, the library utilizies the mechanics of
*type erasure* provided by Rust. As such, some additional type-checking related work
falls to the developer. Furthermore, the developer must also ensure that the worker
functions passed to the library are fit to be executed in parallel. For most large-volume
data processing use-cases, which are arguably the most likely to benefit from the library,
this should be fairly simple to ensure.

For detailed information, see the [examples](https://github.com/suarvid/kafkaesque/tree/master/examples).

### Future Work
Currently, the library only provides the core functionality required to enable parallel execution
of worker functions in a flexible manner, along with a limited amount of utility functions.
As such, there are some possible improvement areas to increase the quality of life for developers
using the library. Some of these are listed below.

* Remove the need for wrapping asynchronous functions in a closure in order to pass them to the production function
* Improve code-reuse for the type-erased version of the synchronous production function
* Add a similar type-erased function for the asynchronous production function.
