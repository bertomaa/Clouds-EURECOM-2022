#include <grpcpp/grpcpp.h>

#include "shardkv.h"
#include "../build/shardkv.grpc.pb.h"
#include <grpcpp/grpcpp.h>

using grpc::Channel;
using grpc::Status;
using grpc::ClientContext;

vector<shard_t> shards_assigned;


bool isKeyAssigned(string key){
    string key_str = key.substr(5);
    if (key.find("posts") != std::string::npos) {
        key_str = key_str.substr(0, key_str.length()-6);
    }
    unsigned int key_int = stoul(key_str);
    for(shard s: shards_assigned){
        if(s.lower <= key_int && s.upper >= key_int){
            return true;
        }
    }
    return false;
}
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
::grpc::Status ShardkvServer::Get(::grpc::ServerContext* context,
                                  const ::GetRequest* request,
                                  ::GetResponse* response) {
    string key = request->key();
    bool NO_REQ = false;
    if (key.find("_no_req") != std::string::npos) {
        key = key.substr(0, key.length()-7);
        NO_REQ = true;
    }


    cout << "in shardkv, get, key: " << request->key() << endl;
    bool is_all_users = key.compare("all_users") == 0;
    if(!NO_REQ && (!is_all_users && !::isKeyAssigned(key))) {
        cerr << "key not assigned" << endl;
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "key is not assigned to this shardkv");
    }
    try {
        if (key.find("all_users") != std::string::npos) {
            //list all users
            cout << "listing all users" << endl;
            string res = "";
            for (map<string, string>::iterator it = users.begin(); it != users.end(); ++it)
            {
                //cout << "listing user" << it->first << endl;
                res += it->first;
                res += ",";
            }
            response->set_data(res);
            return ::grpc::Status(::grpc::Status::OK);
        } else if (key.find("post", 0) == 0) {
            //get on a post
            if(posts.count(key) == 0) {
                return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "post does not exist");
            }
            response->set_data(posts[key].content);
        } else {
            //get on a user
            if (key.find("posts") != std::string::npos) {
                //list all posts of user
                string user_key = key.substr(0, key.size()-6);
                //if(users.count(user_key) == 0){
                //    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "user does not exist");
                //}
                string res = "";
                for (map<string, post_t>::iterator it = posts.begin(); it != posts.end(); ++it)
                {
                    if(it->second.user_id.compare(user_key) == 0){
                        res += it->first;
                        res += ",";
                    }
                }
                /*
                 *
                 * ASK TO OTHER MANAGERS ABOUT USER POSTS
                 *
                 * */
                if(NO_REQ == false) {
                    for (server_t serv: other_managers) {
                        cout << "asking to other managers" << endl;
                        ClientContext cc;
                        GetRequest req;
                        string new_key = key + "_no_req";
                        req.set_key(new_key);
                        GetResponse get_resp;

                        auto channel = grpc::CreateChannel(serv.name, grpc::InsecureChannelCredentials());
                        auto kvStub = Shardkv::NewStub(channel);
                        auto status = kvStub->Get(&cc, req, &get_resp);
                        if (status.ok()) {
                            cout << serv.name << " answered with " << get_resp.data() << endl;
                            res += get_resp.data();
                        } else {
                            cout << serv.name << " DID NOT ANSWER " << status.error_message() << endl;
                        }
                    }
                }



                /*
                 *
                 * ---END---
                 *
                 * */
                if(res.compare("") == 0){
                    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "user does not have posts");
                }else{
                    response->set_data(res);
                    return ::grpc::Status(::grpc::Status::OK);
                }
            }
            else if(users.count(key) == 0) {
                return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "user does not exist");
            }else
                response->set_data(users[key]);
        }
    }catch (const exception& e){
        //cout << "shardkv, in get, try exception: " << e.what() << endl;
        //return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, e.what());
        response->set_data(e.what());
        return ::grpc::Status(::grpc::Status::OK);
    }
    return ::grpc::Status(::grpc::Status::OK);
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
::grpc::Status ShardkvServer::Put(::grpc::ServerContext* context,
                                  const ::PutRequest* request,
                                  Empty* response) {
    cout << "in the shardkv put, key: " << request->key() << " data: " << request->data() << " user: " << request->user() << endl;

    string key = request->key();
    if(!::isKeyAssigned(key))
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "key is not assigned to this shardkv");

    if (key.rfind("post", 0) == 0) {
        //put on a post
        post_t p = post_t();
        p.content = request->data();
        p.user_id = request->user();
        posts[key] = p;
    } else {
        //put on a user
        users[request->key()] = request->data();
    }
    cout << "printing users" << endl;
    for_each(users.begin(),
             users.end(),
             [](const std::pair<string, string> &p) {
                 std::cout << "{" << p.first << ": " << p.second << "}\n";
             });

    /*cout << "printing posts" << endl;
    for_each(users.begin(),
             users.end(),
             [](const std::pair<string, post_t> &p) {
                 std::cout << "{" << p.first << ": {" << p.second.user_id << ", " << p.second.content << "}\n";
             });*/

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
::grpc::Status ShardkvServer::Append(::grpc::ServerContext* context,
                                     const ::AppendRequest* request,
                                     Empty* response) {
    cout << "in the shardkv append, key: " << request->key() << ", data: " << request->data() << endl;
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
::grpc::Status ShardkvServer::Delete(::grpc::ServerContext* context,
                                           const ::DeleteRequest* request,
                                           Empty* response) {
    cout << "in the shardkv delete, key: " << request->key() << endl;

    string key = request->key();

    if (key.rfind("post", 0) == 0) {
        //delete on a post
        if(posts.count(key) == 0)
            return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "post does not exist");
        posts.erase(key);
    } else {
        //delete on a user
        if(users.count(key) == 0)
            return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "user does not exist");
        users.erase(key);
    }
    cout << "printing users" << endl;
    for_each(users.begin(),
             users.end(),
             [](const std::pair<string, string> &p) {
                 std::cout << "{" << p.first << ": " << p.second << "}\n";
             });

    return ::grpc::Status(::grpc::Status::OK);
    return ::grpc::Status(::grpc::Status::OK);
}

/**
 * This method is called in a separate thread on periodic intervals (see the
 * constructor in shardkv.h for how this is done). It should query the shardmaster
 * for an updated configuration of how shards are distributed. You should then
 * find this server in that configuration and look at the shards associated with
 * it. These are the shards that the shardmaster deems this server responsible
 * for. Check that every key you have stored on this server is one that the
 * server is actually responsible for according to the shardmaster. If this
 * server is no longer responsible for a key, you should find the server that
 * is, and call the Put RPC in order to transfer the key/value pair to that
 * server. You should not let the Put RPC fail. That is, the RPC should be
 * continually retried until success. After the put RPC succeeds, delete the
 * key/value pair from this server's storage. Think about concurrency issues like
 * potential deadlock as you write this function!
 *
 * @param stub a grpc stub for the shardmaster, which we use to invoke the Query
 * method!
 */
void ShardkvServer::QueryShardmaster(Shardmaster::Stub* stub) {
    ClientContext cc;
    Empty req;
    QueryResponse res;
    //TODO mettere mutex su vector other_managers e su chiunque ci accceda

    //cout <<  "my address: " << address << endl;

    other_managers.clear();
    auto status = stub->Query(&cc, req, &res);
    if(status.ok()) {
        for(const ConfigEntry config : res.config()) {
            if(config.server().compare(shardmanager_address) != 0){
                server_t s = server_t();
                s.name = config.server();
                other_managers.push_back(s);
            }else {
                for (const Shard s: config.shards()) {
                    shard_t new_shard = shard_t();
                    new_shard.lower = s.lower();
                    new_shard.upper = s.upper();
                    shards_assigned.push_back(new_shard);
                }
            }
        }
    } else {
        logError("Query", status);
    }
    //for (shard_t i: shards_assigned)
    //    std::cout << i.lower << " - " << i.upper << endl;
    //for (server_t s: other_managers)
    //    std::cout << s.name << endl;
}


/**
 * This method is called in a separate thread on periodic intervals (see the
 * constructor in shardkv.h for how this is done).
 * BASIC LOGIC - PART 2
 * It pings the shardmanager to signal the it is alive and available to receive Get, Put, Append and Delete RPCs.
 * The first time it pings the sharmanager, it will  receive the name of the shardmaster to contact (by means of a QuerySharmaster).
 *
 * PART 3
 *
 *
 * @param stub a grpc stub for the shardmaster, which we use to invoke the Query
 * method!
 * */
void ShardkvServer::PingShardmanager(Shardkv::Stub* stub) {

    PingRequest pingReq;
    pingReq.set_server(address);

    PingResponse pingResponse;
    ClientContext cc;

    Status status = stub->Ping(&cc, pingReq, &pingResponse);
    if(status.ok()) {
        shardmaster_address = pingResponse.shardmaster();
    } else {
        logError("Ping request", status);
    }
}




/**
 * PART 3 ONLY
 *
 * This method is called by a backup server when it joins the system for the firt time or after it crashed and restarted.
 * It allows the server to receive a snapshot of all key-value pairs stored by the primary server.
 *
 * @param context - you can ignore this
 * @param request An empty message
 * @param response the whole database
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status ShardkvServer::Dump(::grpc::ServerContext* context, const Empty* request, ::DumpResponse* response) {
    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Not implemented yet");
}
