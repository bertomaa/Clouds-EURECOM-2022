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
    //TODO A key is supplied in the
    // * request and if its value can be found, we should either set the appropriate
    // * field in the response Otherwise, we should return an error. An error should
    // * also be returned if the server is not responsible for the specified key

    cout << "in the manager get, key: " << request->key() << endl;
    ClientContext cc;
    GetRequest req;
    req.set_key(request->key());
    GetResponse res;

    auto channel = grpc::CreateChannel(shardkv_address, grpc::InsecureChannelCredentials());
    auto kvStub = Shardkv::NewStub(channel);
    auto status = kvStub->Get(&cc, req, &res);
    if(status.ok()) {
        cout << "IN THE GET IN MANAGER" << endl;
        std::cout << "Get returned: " << res.data() << "\n";
        response->set_data(res.data());
        return ::grpc::Status(::grpc::Status::OK);
    } else {
        //cout << "IN THE GET error IN MANAGER" << endl;
        logError("Get", status);
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, status.error_message());
    }
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
    cout << "in the manager put, key: " << request->key() << " data: " << request->data() << " user: " << request->user() << endl;
    ClientContext cc;
    PutRequest req;
    req.set_key(request->key());
    req.set_data(request->data());
    req.set_user(request->user());
    Empty res;

    auto channel = grpc::CreateChannel(shardkv_address, grpc::InsecureChannelCredentials());
    auto kvStub = Shardkv::NewStub(channel);
    auto status = kvStub->Put(&cc, req, &res);
    if(status.ok()) {
        cout << "manager: put: ok status received" << endl;
    } else {
        cout << "manager: put: ERROR received" << endl;
        logError("Put", status);
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, status.error_message());
    }
    return ::grpc::Status(::grpc::Status::OK);
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
    cout << "in the manager append, key: " << request->key() << ", data: " << request->data() << endl;
    ClientContext cc;
    AppendRequest req;
    req.set_key(request->key());
    req.set_data(request->data());
    Empty res;

    auto channel = grpc::CreateChannel(shardkv_address, grpc::InsecureChannelCredentials());
    auto kvStub = Shardkv::NewStub(channel);
    auto status = kvStub->Append(&cc, req, &res);
    if(status.ok()) {
        cout << "manager: append ok status received" << endl;
    } else {
        cout << "manager: append: ERROR received" << endl;
        logError("Append", status);
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, status.error_message());
    }
    return ::grpc::Status(::grpc::Status::OK);
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
    cout << "in the manager delete, key: " << request->key() << endl;
    ClientContext cc;
    DeleteRequest req;
    req.set_key(request->key());
    Empty res;

    auto channel = grpc::CreateChannel(shardkv_address, grpc::InsecureChannelCredentials());
    auto kvStub = Shardkv::NewStub(channel);
    auto status = kvStub->Delete(&cc, req, &res);
    if(status.ok()) {
        cout << "manager: delete ok status received" << endl;
    } else {
        cout << "manager: delete: ERROR received" << endl;
        logError("Delete", status);
    }
    return ::grpc::Status(::grpc::Status::OK);
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
    //a shardkv was already registered
        shardkv_address = request->server();
        response->set_shardmaster(sm_address);
        return ::grpc::Status(::grpc::StatusCode::OK, "Success");
}

