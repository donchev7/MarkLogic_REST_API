using ServiceStack;
using ServiceStack.Text;
using System;
using System.Collections;
using System.IO;
using System.Net;


namespace REST_Client_API
{
    public class Connection
    {
        public Configuration conf { get; set; }

        private IRestClient restClient = null; // see how this supports Digest, Basic, and HTTPS -> Automatically once user/pass configured (see configure)

        private string txid = null;


        // static searches for convenience

        public Connection()
        {
            // set up ServiceStack as required
            JsConfig.PropertyConvention = PropertyConvention.Lenient; // supports deserialising hyphenated properties in ServiceStack.Text V3.9.43+

            // setup this class now
            conf = new Configuration();
            configure(conf);
        }

        private string completePath(string path, Hashtable parameters)
        {
            string nextChar = "?";
            if (path.Contains("?"))
            {
                nextChar = "&";
            }
            string newPath = (string)path.Clone();
            foreach (string key in parameters.Keys)
            {
                newPath += nextChar + key + "=" + (string)parameters[key]; // TODO param encode for uri
                nextChar = "&";
            }
            return newPath;
        }


        public Response doGet(string path, Hashtable parameters)
        {
            try
            {
                HttpWebResponse hres = restClient.Get<HttpWebResponse>(completePath(path, parameters));

                string body = "";
                using (var stream = hres.GetResponseStream())
                using (var sr = new StreamReader(stream))
                {
                    body = sr.ReadToEnd();
                }

                Response resp = new Response();
                resp.statusCode = hres.StatusCode.ToString();

                // TODO check if response is error XML or valid JSON content
                if (hres.StatusCode.ToString().Substring(0, 1).Equals("4"))
                {
                    resp.inError = true;
                    resp.error = body;
                }
                else {
                    resp.doc = new Doc();
                    resp.doc.setJsonContent(body);
                    resp.inError = false;
                }

                return resp;
            }
            catch (Exception e)
            {
                Response resp = new Response();
                resp.inError = true;
                resp.error = e.ToString();
                resp.exception = e;
                return resp;
            }
        }

        public Response doPut(string path, Hashtable parameters, Doc doc)
        {
            try
            {
                Response resp = restClient.Put<Response>(completePath(path, parameters), doc.getTextContent()); // TODO serialise document to restClient URL as string content format json
                resp.inError = false;
                return resp;
            }
            catch (Exception e)
            {
                Response resp = new Response();
                resp.inError = true;
                resp.error = e.ToString();
                resp.exception = e;
                return resp;
            }
        }

        public Response doPost(string path, Hashtable parameters, Doc doc)
        {
            try
            {
                Response resp = restClient.Post<Response>(completePath(path, parameters), doc.getTextContent()); // TODO serialise document to restClient URL as string content format json
                resp.inError = false;
                return resp;
            }
            catch (Exception e)
            {
                Response resp = new Response();
                resp.inError = true;
                resp.error = e.ToString();
                resp.exception = e;
                return resp;
            }
        }

        public Response doDelete(string path, Hashtable parameters)
        {
            try
            {
                Response resp = restClient.Delete<Response>(completePath(path, parameters));
                resp.inError = false;
                return resp;
            }
            catch (Exception e)
            {
                Response resp = new Response();
                resp.inError = true;
                resp.error = e.ToString();
                resp.exception = e;
                return resp;
            }
        }

        /**
         * Function allowing MLDB's underlying REST invocation mechanism to be used for an arbitrary request. 
         * Useful for future proofing should some new functionality come out, or bug discovered that prevents
         * your use of a JavaScript Driver API call.
         * options = {method: "GET|POST|PUT|DELETE", path: "/v1/somepath?key=value&format=json"}
         * content = undefined for GET, DELETE, json for PUT, whatever as required for POST
         */
        public Response doRequest(string path, Hashtable parameters, Doc doc, string method)
        {
            switch (method)
            {
                case "GET":
                    return doGet(path, parameters);
                case "POST":
                    return doPost(path, parameters, doc);
                case "PUT":
                    return doPut(path, parameters, doc);
                case "DELETE":
                    return doDelete(path, parameters);
                default:
                    return null;
            }
        }



        // TODO ENSURE ALL METHODS FROM MLDB ARE SUPPORTED

        // DRIVER CONFIGURATION
        /**
         * Provide configuration information to this database. This is merged with the defaults.
         */
        public void configure(Configuration conf)
        {
            this.conf = conf;

            // NB This automatically uses Basic and Digest authentication where required
            this.restClient = new JsonServiceClient(this.conf.getConnectionString())
            {
                UserName = this.conf.username,
                Password = this.conf.password,
                AlwaysSendBasicAuthHeader = true // TODO check this is always OK -> NB Must set REST server to Basic, not Digest (ServiceStack doesn't work with this for some reason)
            };
        }

        // DATABASE MANAGEMENT
        /**
         * Does this database exist? Returns an object, not boolean, to the callback
         */
        public bool exists()
        {
            // TODO perform check in exists()
            return false;
        }

        /**
         * Creates the database and rest server if it does not already exist
         */
        public void create()
        {

        }

        /**
         * Destroys the database and rest api instance
         */
        public void destroy()
        {

        }

        // DOCUMENT MANAGEMENT
        // NEEDED FOR FILE SYNC PROJECT
        /**
         * Fetches a document with the given URI.
         * 
         * https://docs.marklogic.com/REST/GET/v1/documents
         */
        public Doc get(string uri)
        {
            Hashtable qp = new Hashtable();
            qp.Add("uri", uri);
            Doc doc = new Doc();

            byte[] responseBytes = restClient.Get<byte[]>(completePath("/v1/documents", qp));


            doc.setFileContent(responseBytes);

            return doc;

        }

        /**
         * Fetches the metadata for a document with the given URI. Metadata document returned in result.doc
         * 
         * https://docs.marklogic.com/REST/GET/v1/documents
         */
        public Doc metadata(string uri)
        {
            string path = @"/v1/documents";
            Hashtable qp = new Hashtable();
            qp.Add("category", "metadata");
            qp.Add("uri", uri);

            string json = restClient.Get<string>(completePath(path, qp));
            Doc doc = new Doc();
            doc.setJsonContent(json);

            Prop props = JsonObject.Parse(json).GetUnescaped("properties").FromJson<Prop>();
            doc.properties = props;

            // TODO handle collections, permissions, quality within metadata too
            // example:  metadata: {"collections":[],"permissions":[],"properties":{"last-modified":"2013-03-23T14:27:37Z"},"quality":0}

            return doc;
        }


        public Response save(Doc doc, string docuri)
        {
            return save(doc, docuri, null);
        }

        // NEEDED FOR FILE SYNC PROJECT
        /**
         * Saves new docs with GUID-timestamp, new docs with specified id, or updates doc with specified id
         * NB handle json being an array of multiple docs rather than a single json doc
         * If no docuri is specified, one is generated by using a combination of the time and a large random number.
         *
         * https://docs.marklogic.com/REST/PUT/v1/documents
         */
        public Response save(Doc doc, string docuri, Doc properties)
        {
            string path = @"/v1/documents";
            Hashtable qp = new Hashtable();
            qp.Add("uri", docuri);
            return doPut(path, qp, doc);
        }

        /**
         * Updates the document with the specified uri by only modifying the passed in properties.
         * NB May not be possible in V6 REST API elegantly - may need to do a full fetch, update, save
         */
        public void merge()
        {
        }

        // NEEDED FOR FILE SYNC PROJECT
        /**
         * Deletes the specified document
         * 
         * https://docs.marklogic.com/REST/DELETE/v1/documents
         */
        public Response delete(string docuri)
        {
            string path = @"/v1/documents";
            Hashtable qp = new Hashtable();
            qp.Add("uri", docuri);
            return doDelete(path, qp);
        }

        // SEARCH FUNCTIONS
        /**
         * Returns all documents in a collection, optionally matching against the specified fields
         * http://docs.marklogic.com/REST/GET/v1/search
         */
        public SearchResponse collect()
        {
            return null; // TODO change from null
        }

        /**
         * Lists all documents in a directory, to the specified depth (default: 1), optionally matching the specified fields
         * http://docs.marklogic.com/REST/GET/v1/search
         */
        public SearchResponse list()
        {
            return null; // TODO change from null
        }

        /**
         * Performs a simple key-value search. Of most use to JSON programmers.
         * 
         * https://docs.marklogic.com/REST/GET/v1/keyvalue
         */
        public void keyvalue()
        {
            return; // TODO change from void
        }

        /**
         * Performs a search:search via REST
         * http://docs.marklogic.com/REST/GET/v1/search
         *
         * See supported search grammar http://docs.marklogic.com/guide/search-dev/search-api#id_41745 
         */
        public SearchResponse search()
        {
            return null; // TODO change from void
        }

        /**
         * Performs a search:search via REST
         * http://docs.marklogic.com/REST/GET/v1/search
         *
         * See supported search grammar http://docs.marklogic.com/guide/search-dev/search-api#id_41745 
         */
        public SearchResponse searchCollection()
        {
            return null; // TODO change from void
        }

        /**
         * Performs a structured search.
         * http://docs.marklogic.com/REST/GET/v1/search
         * 
         * Uses structured search instead of cts:query style searches. See http://docs.marklogic.com/guide/search-dev/search-api#id_53458
         */
        public SearchResponse structuredSearch(string options_opt, string query_opt)
        {
            string path = @"/v1/search";
            Hashtable qp = new Hashtable();
            qp.Add("format", "json");
            if (null != query_opt)
            {
                qp.Add("structuredQuery", query_opt);
            }
            //qp.Add ("directory", "/myproject/");

            if (options_opt != null)
            {
                qp.Add("options", options_opt);
            }

            // make transaction aware
            if (null != this.txid)
            {
                qp.Add("txid", this.txid);
            }

            //Response response = doGet (path, qp);
            Search search = new Search();
            search.Options = options_opt;
            search.StructuredQuery = query_opt;


            SearchResponse result = null;
            /*
            using (Stream responseStream = restClient.Get<Stream> (search)) {
              var str = responseStream.ReadFully().FromUtf8Bytes();
              result = str.FromJson<SearchResponse>();
            }
            */

            HttpWebResponse webResponse = restClient.Get<HttpWebResponse>(completePath("/v1/search", qp));

            using (var stream = webResponse.GetResponseStream())
            using (var sr = new StreamReader(stream))
            {
                var text = sr.ReadToEnd();
                result = text.FromJson<SearchResponse>();
            }

            /*
                  Response response = new Response ();
                  return response; */
            return result;
        }

        // NEEDED FOR FILE SYNC PROJECT
        /**
         * Saves search options with the given name. These are referred to by mldb.structuredSearch.
         * http://docs.marklogic.com/REST/PUT/v1/config/query/*
         *
         * For structured serch options see http://docs.marklogic.com/guide/rest-dev/search#id_48838
         */
        public bool saveSearchOptions(string name, string options)
        {
            string path = @"/v1/config/query/" + name;
            Hashtable qp = new Hashtable();
            qp.Add("format", "json");

            Doc doc = new Doc();
            doc.setJsonContent(options);

            Response response = doPut(path, qp, doc);
            return !response.inError; // TODO log any errors and pass to caller as exception
        }

        // NEEDED FOR FILE SYNC PROJECT
        /**
         * Ensures a search has been saved to the server. If it hasn't, it saves the search options to the server.
         * Returns true if it exists on the server, or has been successfully created on the server by this call.
         */
        public bool ensureSearchSaved(string name, string options)
        {
            // get search options from DB

            string path = @"/v1/config/query/" + name;
            Hashtable qp = new Hashtable();
            qp.Add("format", "json");

            Response getResult = doGet(path, qp); // TODO verify the URI name of the persisted search options
            if (getResult.inError)
            {
                // if error (does not exist) then persist search options
                return saveSearchOptions(name, options);
            }
            else {
                return true;
            }
        }

        public DocRefs listURIs(string uri)
        {
            // TODO listURIs
            return null; // TODO change from null
        }

        public DocRefs listURIsSinceVersion(string uribase, string mvccVersion)
        {
            // TODO listURIsSinceVersion
            return null; // TODO change from null
        }

        public Response beginTransaction()
        {
            return beginTransaction("client-txn");
        }

        // TRANSACTIONS
        // NEEDED FOR FILE SYNC PROJECT
        /**
         * Opens a new transaction. Optionally, specify your own name.
         * http://docs.marklogic.com/REST/POST/v1/transactions
         */
        public Response beginTransaction(string txname)
        {
            string path = @"/v1/transactions";
            Hashtable qp = new Hashtable();
            qp.Add("category", "metadata");
            if (null != txname)
            {
                qp["name"] = txname;
            }
            else {
                qp["name"] = "client-txn";
            }
            this.txid = (string)qp["name"];
            return doPost(path, qp, null);
            // TODO add error check and throw
        }

        // NEEDED FOR FILE SYNC PROJECT
        /**
         * Commits the open transaction
         * http://docs.marklogic.com/REST/POST/v1/transactions/*
         */
        public Response commitTransaction()
        {
            string path = @"/v1/transactions/" + this.txid;
            Hashtable qp = new Hashtable();
            qp.Add("result", "commit");
            return doPost(path, qp, null);
        }

        // NEEDED FOR FILE SYNC PROJECT
        /**
         * Rolls back the open transaction.
         * http://docs.marklogic.com/REST/POST/v1/transactions/*
         */
        public Response rollbackTransaction()
        {
            string path = @"/v1/transactions/" + this.txid;
            Hashtable qp = new Hashtable();
            qp.Add("result", "rollback");
            return doPost(path, qp, null);
        }

        // REST API EXTENSIONS

        // UTILITY FUNCTIONS

        // POTENTIALLY NEEDED FOR FILE SYNC PROJECT (for saving all docs in a folder (uri) )
        /**
         * Inserts many JSON documents. FAST aware, TRANSACTION aware.
         */
        public void saveAll()
        {
            return; // TODO change from void
        }

    }
}
