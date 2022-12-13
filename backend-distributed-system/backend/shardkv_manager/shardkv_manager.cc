#include <grpcpp/grpcpp.h>

#include "shardkv_manager.h"

/**
 * This method is analogous to a hashmap lookup. A key is supplied in the
 * request and if its value can be found, we should either set the appropriate
 * field in the response Otherwise, we should return an error. An error should
 * also be returned if the server is not responsible for the specified key
 *
 * @param context - you can ignore this
 * @param request a message containing a key
 * @param response we store the value for the specified key here
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status ShardkvManager::Get(::grpc::ServerContext* context,
                                  const ::GetRequest* request,
                                  ::GetResponse* response) {
  return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Not implemented yet");
}

/**
 * Insert the given key-value mapping into our store such that future gets will
 * retrieve it
 * If the item already exists, you must replace its previous value.
 * This function should error if the server is not responsible for the specified
 * key.
 *
 * @param context - you can ignore this
 * @param request A message containing a key-value pair
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status ShardkvManager::Put(::grpc::ServerContext* context,
                                  const ::PutRequest* request,
                                  Empty* response) {
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Not implemented yet");
}

/**
 * Appends the data in the request to whatever data the specified key maps to.
 * If the key is not mapped to anything, this method should be equivalent to a
 * put for the specified key and value. If the server is not responsible for the
 * specified key, this function should fail.
 *
 * @param context - you can ignore this
 * @param request A message containngi a key-value pair
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>"
 */
::grpc::Status ShardkvManager::Append(::grpc::ServerContext* context,
                                     const ::AppendRequest* request,
                                     Empty* response) {
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Not implemented yet");
}

/**
 * Deletes the key-value pair associated with this key from the server.
 * If this server does not contain the requested key, do nothing and return
 * the error specified
 *
 * @param context - you can ignore this
 * @param request A message containing the key to be removed
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status ShardkvManager::Delete(::grpc::ServerContext* context,
                                           const ::DeleteRequest* request,
                                           Empty* response) {
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Not implemented yet");
}

/**
 * In part 2, this function get address of the server sending the Ping request, who became the primary server to which the
 * shardmanager will forward Get, Put, Append and Delete requests. It answer with the name of the shardmaster containeing
 * the information about the distribution.
 *
 * @param context - you can ignore this
 * @param request A message containing the name of the server sending the request, the number of the view acknowledged
 * @param response The current view and the name of the shardmaster
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status ShardkvManager::Ping(::grpc::ServerContext* context, const PingRequest* request,
                                       ::PingResponse* response){
    return ::grpc::Status(::grpc::StatusCode::OK, "Success");
}

