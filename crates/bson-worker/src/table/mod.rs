//! Table (LATERAL / glob fan-out) functions exposed by the bson worker.

pub mod dump;
pub mod seq;

use vgi::Worker;

/// Register every table function on the worker.
pub fn register(worker: &mut Worker) {
    worker.register_table(seq::BsonSeq);
    worker.register_table(dump::MongodumpRead);
}
