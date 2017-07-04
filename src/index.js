'use strict';

var Query      = require('bazalt-query');
var Translator = require('bazalt-query-to-node-mysql');

class QueryNodeMysqlTransformer {

    // Define allowed Mode
    static get Mode() {
        return {
            Connection: 'CONNECTION',
            Pool:       'POOL'
        };
    }

    pool(pool) {
        this.$__mode = QueryNodeMysqlTransformer.Mode.Pool;
        this.$__pool = pool;
    }

    connection(connection) {
        this.$__mode       = QueryNodeMysqlTransformer.Mode.Connection;
        this.$__connection = connection;
    }

    executeOnConnection(connection, query, callback) {
        var translator = new Translator(query);

        // Run the query
        connection.query(translator.sql, translator.parameters, function(err, results) {

            // Release the connection
            connection.release();

            // If needed, execute the callback
            if('function' === typeof callback)
            {
                callback(err, results);
            } 
        });
    }

    executeQuery(query, callback) {
        if(false === query instanceof Query) {
            throw new Error('The given object is not an instance of `Query`.');
        }

        var self = this;

        switch(this.$__mode) {
            case QueryNodeMysqlTransformer.Mode.Connection:
                // Execute on Main Connection
                self.executeOnConnection(self.$__connection, query, callback);

                break;

            case QueryNodeMysqlTransformer.Mode.Pool:
                // Get the connection from the pool
                self.$__pool.getConnection(function(err, connection) {
                    if(err)
                    {
                        // If needed, execute the callback
                        if('function' === typeof callback)
                        {
                            callback(err);
                        }

                        return;
                    }

                    // Execute on Pool Connection
                    self.executeOnConnection(connection, query, callback);
                });

                break;
        }
    }

    transformer() {
        var self = this;

        // Return the transformer for Query
        return function(callback) {
            // Transfer the query
            self.executeQuery(this, callback);
        };
    }

    static transformer(connection, mode = QueryNodeMysqlTransformer.Mode.Pool) {
        var transformer = new QueryNodeMysqlTransformer();
        
        switch(mode) {
            case QueryNodeMysqlTransformer.Mode.Connection:
                transformer.connection(connection);

                break;

            case QueryNodeMysqlTransformer.Mode.Pool:
                transformer.pool(connection);

                break;

            default:
                throw new Error('The given mode is not supported.');
                
                break;
        }


        return transformer.transformer();
    }
}

// Export QueryNodeMysqlTransformer
module.exports = QueryNodeMysqlTransformer;
