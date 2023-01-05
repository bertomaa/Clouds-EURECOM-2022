#include "shardmaster.h"
#include "iostream"
#include "../common/common.h"
#include "../build/shardkv.pb.h"

using namespace std;

vector<server_t> servers;

/**
 * Based on the server specified in JoinRequest, you should update the
 * shardmaster's internal representation that this server has joined. Remember,
 * you get to choose how to represent everything the shardmaster tracks in
 * shardmaster.h! Be sure to rebalance the shards in equal proportions to all
 * the servers. This function should fail if the server already exists in the
 * configuration.
 *
 * @param context - you can ignore this
 * @param request A message containing the address of a key-value server that's
 * joining
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status StaticShardmaster::Join(::grpc::ServerContext *context,
                                       const ::JoinRequest *request,
                                       Empty *response)
{
    /*
    join a:1
    join b:2
    join c:3
    join d:4
    join e:5
     */
    //server name is request->server()

    //check if server already exists
    if(findServerByName(servers, request->server()) != servers.end()){
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "The server already exists!");
    }
    int new_lower = resizeShards(servers,servers.size()+1);
    server new_server = server_t();
    new_server.name = request->server();

    shard new_shard = shard_t();
    new_shard.lower = new_lower;
    new_shard.upper = MAX_KEY;
    new_server.shards.push_back(new_shard);
    servers.push_back(new_server);
    return ::grpc::Status(::grpc::Status::OK);
}

/**
 * LeaveRequest will specify a list of servers leaving. This will be very
 * similar to join, wherein you should update the shardmaster's internal
 * representation to reflect the fact the server(s) are leaving. Once that's
 * completed, be sure to rebalance the shards in equal proportions to the
 * remaining servers. If any of the specified servers do not exist in the
 * current configuration, this function should fail.
 *
 * @param context - you can ignore this
 * @param request A message containing a list of server addresses that are
 * leaving
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status StaticShardmaster::Leave(::grpc::ServerContext *context,
                                        const ::LeaveRequest *request,
                                        Empty *response)
{
    //check all the servers to remove exist

    for(string i: request->servers()){
        if(findServerByName(servers, i) == servers.end()){
            string errorString = "The server " + i + " does not exist!";
            return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, errorString);
        }
    }
    //remove shards
    for(string i: request->servers()){
        vector<server_t>::iterator toRemove = findServerByName(servers, i);
        servers.erase(toRemove);
    }
    resizeShards(servers,servers.size());

    return ::grpc::Status(::grpc::Status::OK);
}

/**
 * Move the specified shard to the target server (passed in MoveRequest) in the
 * shardmaster's internal representation of which server has which shard. Note
 * this does not transfer any actual data in terms of kv-pairs. This function is
 * responsible for just updating the internal representation, meaning whatever
 * you chose as your data structure(s).
 *
 * @param context - you can ignore this
 * @param request A message containing a destination server address and the
 * lower/upper bounds of a shard we're putting on the destination server.
 * @param response An empty message, as we don't need to return any data
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status StaticShardmaster::Move(::grpc::ServerContext *context,
                                       const ::MoveRequest *request,
                                       Empty *response)
{
    // Hint: Take a look at get_overlap in common.{h, cc}
    // Using the function will save you lots of time and effort!

    //request->shard().lower_
    //request->server()

    //check the server exists
    if(findServerByName(servers, request->server()) == servers.end()){
        string errorString = "The server " + request->server() + " does not exist!";
        return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, errorString);
    }
    
    shard new_shard = shard_t();
    new_shard.upper = request->shard().upper();
    new_shard.lower = request->shard().lower();
    for(server &server: servers){
        if(server.name.compare(request->server()) == 0){
            continue;
        }
        for(shard &current_shard: server.shards) {
            switch (get_overlap(new_shard, current_shard)) {
                case OverlapStatus::OVERLAP_START:
                    current_shard.upper = new_shard.lower-1;
                    break;
                case OverlapStatus::OVERLAP_END:
                    current_shard.lower = new_shard.upper+1;
                    break;
                case OverlapStatus::COMPLETELY_CONTAINS:
                    current_shard.to_be_deleted = true;
                    break;
                case OverlapStatus::COMPLETELY_CONTAINED:
                    if(current_shard.lower == new_shard.lower && current_shard.upper != new_shard.upper){
                        current_shard.lower = new_shard.upper + 1;
                    }else if(current_shard.upper == new_shard.upper && current_shard.lower != new_shard.lower){
                        current_shard.upper = new_shard.lower - 1;
                    }else if(current_shard.upper == new_shard.upper && current_shard.lower == new_shard.lower) {
                        current_shard.to_be_deleted = true;
                    }else{
                        int tmp = current_shard.upper;
                        current_shard.upper = new_shard.lower - 1;
                        shard second_part_shard = shard_t();
                        second_part_shard.lower = new_shard.upper + 1;
                        second_part_shard.upper = tmp;
                        server.shards.push_back(second_part_shard);
                    }
                    break;
            }
        }
    }
    cleanEmptyShards(servers);

    bool overlapped_once = false;

    vector<server_t>::iterator it = findServerByName(servers, request->server());
    for(shard &current_shard: it->shards){
        switch (get_overlap(new_shard, current_shard)) {
            case OverlapStatus::OVERLAP_START:
                current_shard.upper = new_shard.upper;
                overlapped_once = true;
                break;
            case OverlapStatus::OVERLAP_END:
                current_shard.lower = new_shard.lower;
                overlapped_once = true;
                break;
            case OverlapStatus::COMPLETELY_CONTAINS:
                current_shard.lower = new_shard.lower;
                current_shard.upper = new_shard.upper;
                overlapped_once = true;
                break;
            case OverlapStatus::COMPLETELY_CONTAINED:
                overlapped_once = true;
                break;
            case OverlapStatus::NO_OVERLAP:
                if(current_shard.upper + 1 == new_shard.lower){
                    current_shard.upper = new_shard.upper;
                }else if(current_shard.lower - 1 == new_shard.upper){
                    current_shard.lower = new_shard.lower;
                }
                break;
        }
    }

    if(!overlapped_once) it->shards.push_back(new_shard);

    return ::grpc::Status(::grpc::Status::OK);
}

/**
 * When this function is called, you should store the current servers and their
 * corresponding shards in QueryResponse. Take a look at
 * 'protos/shardmaster.proto' to see how to set QueryResponse correctly. Note
 * that its a list of ConfigEntry, which is a struct that has a server's address
 * and a list of the shards its currently responsible for.
 *
 * @param context - you can ignore this
 * @param request An empty message, as we don't need to send any data
 * @param response A message that specifies which shards are on which servers
 * @return ::grpc::Status::OK on success, or
 * ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "<your error message
 * here>")
 */
::grpc::Status StaticShardmaster::Query(::grpc::ServerContext *context,
                                        const StaticShardmaster::Empty *request,
                                        ::QueryResponse *response)
{
    for(server i: servers){
        ConfigEntry * c = response->add_config();
        c->set_server(i.name);
        for(shard j: i.shards) {
            Shard *s = c->add_shards();
            s->set_lower(j.lower);
            s->set_upper(j.upper);
        }
    }
    //return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "Not implemented yet");
    return ::grpc::Status(::grpc::Status::OK);
}


