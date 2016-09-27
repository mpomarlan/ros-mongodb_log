#ifndef __ROS_MONGO_INTERFACE_H__

#define __ROS_MONGO_INTERFACE_H__

#include "mongoversion.h"

#ifdef USE_DEFAULT_MONGODB

#include <mongo/client/dbclient.h>

typedef unsigned int MongoInt;
typedef mongo::DBClientConnection* MongoConnectionT;
#define DECLARE_MONGO_CONNECTION(Var) MongoConnectionT Var;

typedef mongo::BSONObj DataObject;
typedef mongo::BSONObjBuilder DataObjectBuilder;
typedef mongo::BSONArrayBuilder ArrayObjectBuilder;
#define MAKE_BSON_DATA_OBJECT(Val) (BSON(Val))

template<typename T> void builderAppend(DataObjectBuilder &builder, std::string const& key, T const& val)
{
    builder.append(key, val);
}
void builderAppendDocumentVector(DataObjectBuilder &builder, std::string const& key, std::vector<DataObject> const& val)
{
    builder << key << val;
}
void builderAppendBinaryData(DataObjectBuilder &builder, std::string const& key, int len, const void* data)
{
    builder.appendBinData(key, len, mongo::BinDataGeneral, data);
}
void builderAppendElements(DataObjectBuilder &builder, DataObject data)
{
    builder.appendElements(data);
}
#define START_BUILDER_ARRAY(builder, parent, key) ArrayObjectBuilder builder((parent).subarrayStart(key));
#define FINISH_BUILDER_ARRAY(builder, parent, key) (builder).doneFast();
template<typename T> void builderArrayAppend(ArrayObjectBuilder & builder, T const& data)
{
    builder.append(data);
}
DataObject builderGetObject(DataObjectBuilder &builder)
{
    builder.obj();
}

bool ConnectToDatabase(MongoConnectionT &clientConnection, std::string const&mongodb_host, std::string const& dbcollname, std::string &errmsg)
{
    clientConnection = new mongo::DBClientConnection(/* auto reconnect*/ true);
    return(clientConnection->connect(mongodb_host, errmsg));
}

void insertOne(MongoConnectionT &clientConnection, std::string const& collection, DataObject const& dataObj)
{
    clientConnection->insert(collection, dataObj);
}

typedef mongo::Date_t BSONDate;
#define MAKE_BSON_DATE(Val) (BSONDate(Val))

#else

#include <bsoncxx/builder/stream/array.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/types/value.hpp>
#include <bsoncxx/types.hpp>

#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/logger.hpp>
#include <mongocxx/options/client.hpp>
#include <mongocxx/uri.hpp>

typedef signed long int MongoInt;
typedef struct
{
    mongocxx::client clientConn;
    mongocxx::database databaseConn;
}MongoConnection;
typedef MongoConnection* MongoConnectionT;
#define DECLARE_MONGO_CONNECTION(Var) MongoConnectionT Var;

typedef bsoncxx::document::view_or_value DataObject;
typedef bsoncxx::builder::stream::document DataObjectBuilder;
typedef bsoncxx::builder::stream::array ArrayObjectBuilder;
#define MAKE_BSON_DATA_OBJECT(Val) ((DataObjectBuilder() << Val << bsoncxx::builder::stream::finalize).view())

template<typename T> void builderAppend(DataObjectBuilder &builder, std::string const& key, T const& val)
{
    builder << key << val;
}
template<> void builderAppend(DataObjectBuilder &builder, std::string const& key, bsoncxx::document::view_or_value const& val)
{
    builder << key << bsoncxx::types::b_document{val.view()};
}
template<> void builderAppend(DataObjectBuilder &builder, std::string const& key, bsoncxx::document::view const& val)
{
    builder << key << bsoncxx::types::b_document{val};
}
template<> void builderAppend(DataObjectBuilder &builder, std::string const& key, bsoncxx::document::value const& val)
{
    builder << key << bsoncxx::types::b_document{val};
}

void builderAppendDocumentVector(DataObjectBuilder &builder, std::string const& key, std::vector<DataObject> const& val)
{
    int maxK = val.size();
    ArrayObjectBuilder arr;
    for(int k = 0 ; k < maxK; k++)
    {
        arr << bsoncxx::types::b_document{val[k]};
    }
    builder << key << bsoncxx::types::b_array{arr};
}

void builderAppendBinaryData(DataObjectBuilder &builder, std::string const& key, int len, const void* data)
{
    bsoncxx::types::b_binary binDataObj;
    binDataObj.size = len;
    binDataObj.bytes = (const unsigned char*)data;
    binDataObj.sub_type = bsoncxx::binary_sub_type::k_binary;
    builder << key << binDataObj;
}
void builderAppendElements(DataObjectBuilder &builder, DataObject data)
{
    builder << bsoncxx::builder::stream::concatenate(data.view());
}
#define START_BUILDER_ARRAY(builder, parent, key) ArrayObjectBuilder builder;
#define FINISH_BUILDER_ARRAY(builder, parent, key) parent << key << bsoncxx::types::b_array{builder};
template<typename T> void builderArrayAppend(ArrayObjectBuilder & builder, T const& data)
{
    builder << data;
}
template<> void builderArrayAppend(ArrayObjectBuilder &builder, bsoncxx::document::view_or_value const& data)
{
    builder << bsoncxx::types::b_document{data.view()};
}
template<> void builderArrayAppend(ArrayObjectBuilder &builder, bsoncxx::document::view const& data)
{
    builder << bsoncxx::types::b_document{data};
}
template<> void builderArrayAppend(ArrayObjectBuilder &builder, bsoncxx::document::value const& data)
{
    builder << bsoncxx::types::b_document{data};
}
DataObject builderGetObject(DataObjectBuilder &builder)
{
    //builder << bsoncxx::builder::stream::finalize;
    builder.view();
}

bool ConnectToDatabase(MongoConnectionT &clientConnection, std::string const&mongodb_host, std::string const& dbcollname, std::string &errmsg)
{
    std::string db_name = dbcollname.substr(0, dbcollname.find('.'));

    clientConnection = new MongoConnection();

    clientConnection->clientConn = mongocxx::client(mongocxx::uri(mongocxx::uri::k_default_uri));
    if(!((bool)clientConnection->clientConn))
    {
        errmsg = "Couldn't connect to Mongodb server.";
        return false;
    }

    clientConnection->databaseConn = clientConnection->clientConn[db_name];

    return (true);
}

void insertOne(MongoConnectionT &clientConnection, std::string const& dbcollname, DataObject const& dataObj)
{
    //std::string collection_name = dbcollname;
    //size_t separator = dbcollname.find('.');
    //if(string::npos != separator)
    //    collection_name = dbcollname.substr(separator + 1);
    //clientConnection->databaseConn[collection_name].insert_one(dataObj);
}

typedef bsoncxx::types::b_date BSONDate;

#define MAKE_BSON_DATE(Val) (BSONDate((long int)(Val)))

#endif

#endif

