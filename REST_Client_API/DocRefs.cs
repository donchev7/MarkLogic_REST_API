using System.Collections;
using System.Collections.Generic;


namespace REST_Client_API
{
    public class DocRefs : IEnumerable
    {
        List<string> docuris = new List<string>();

        public DocRefs()
        {
        }

        public void Add(string uri)
        {
            docuris.Add(uri);
        }

        public int Count()
        {
            return docuris.Count;
        }

        public IEnumerator GetEnumerator()
        {
            return docuris.GetEnumerator();
        }
    }
}
