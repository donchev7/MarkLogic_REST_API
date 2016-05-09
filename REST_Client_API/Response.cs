using System;

namespace REST_Client_API
{
    public class Response
    {
        public Response()
        {

        }

        public Doc doc { get; set; }
        public bool inError { get; set; }
        public string error { get; set; }
        public Exception exception { get; set; }
        public string statusCode { get; set; }
    }
}
