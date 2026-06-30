//! Scalar functions exposed by the bson worker, registered under `bson.main`.

#[macro_use]
pub mod common;
pub mod codec;
pub mod objectid;
pub mod timestamp;
pub mod version;

use vgi::Worker;

/// Register every scalar function on the worker.
pub fn register(worker: &mut Worker) {
    worker.register_scalar(version::BsonVersion);

    // Core codec. Optional-arg functions ship a 1-arg and a 2-arg arity overload
    // because DuckDB binds a const argument as required.
    worker.register_scalar(codec::Decode { with_mode: false });
    worker.register_scalar(codec::Decode { with_mode: true });
    worker.register_scalar(codec::ToJson { with_mode: false });
    worker.register_scalar(codec::ToJson { with_mode: true });
    worker.register_scalar(codec::FromJson);
    worker.register_scalar(codec::Encode);
    worker.register_scalar(codec::IsValid);
    worker.register_scalar(codec::WellFormed);
    worker.register_scalar(codec::Keys);
    worker.register_scalar(codec::Field { with_as: false });
    worker.register_scalar(codec::Field { with_as: true });
    worker.register_scalar(codec::TypeOf { with_path: false });
    worker.register_scalar(codec::TypeOf { with_path: true });

    // ObjectId helpers.
    worker.register_scalar(objectid::ObjectIdTimestamp);
    worker.register_scalar(objectid::ObjectIdHex);
    worker.register_scalar(objectid::ObjectIdFromHex);

    // Timestamp helpers.
    worker.register_scalar(timestamp::TimestampToTs);
    worker.register_scalar(timestamp::TimestampParts);
}
