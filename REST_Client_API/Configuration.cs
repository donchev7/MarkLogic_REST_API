using ServiceStack;

namespace REST_Client_API
{
    public class Configuration : IReturn<Configuration>
    {
        public string host { get; set; }
        public string port { get; set; }
        public string adminport { get; set; }
        public bool ssl { get; set; }
        public string auth { get; set; }
        public string username { get; set; }
        public string password { get; set; }
        public string database { get; set; }
        public string searchoptions { get; set; }
        public string baseuri { get; set; }

        public Configuration()
        {
            host = Properties.Settings.Default.host;
            port = Properties.Settings.Default.APIport;
            adminport = Properties.Settings.Default.adminport;
            ssl = Properties.Settings.Default.ssl;
            auth = Properties.Settings.Default.auth;
            username = Properties.Settings.Default.username;
            password = Properties.Settings.Default.password;
            database = Properties.Settings.Default.database;
            searchoptions = Properties.Settings.Default.searchoptions;
            baseuri = Properties.Settings.Default.baseuri;
        }

        public override string ToString()
        {
            return string.Format("[Options: host={0}, port={1}, adminport={2}, ssl={3}, auth={4}, username={5}, password={6}, database={7}, searchoptions={8}, baseuri={9}]", host, port, adminport, ssl, auth, username, password, database, searchoptions, baseuri);
        }

        public string getConnectionString()
        {
            string cs = "http";
            if (ssl)
            {
                cs += "s";
            }
            cs += "://" + host + ":" + port;
            return cs;
        }

        public void setConnectionString(string cs)
        {
            // parse cs for protocol
            if (cs.StartsWith("https://"))
            {
                ssl = true;
                cs = cs.Substring(8);
            }
            else {
                ssl = false;
                cs = cs.Substring(7); // http://
            }

            // check for username/password
            int atpos = cs.IndexOf("@");
            if (-1 != atpos)
            {
                string userpass = cs.Substring(0, atpos);
                cs = cs.Substring(atpos + 1);

                // parse
                int colonpos = userpass.IndexOf(":");
                username = userpass.Substring(0, colonpos);
                password = userpass.Substring(colonpos + 1);
            }

            // check for host/port
            int pos = cs.IndexOf(":");
            if (-1 == pos)
            {
                int pos2 = cs.IndexOf("/");
                if (-1 == pos)
                {
                    host = cs;
                    cs = "/";
                }
                else {
                    host = cs.Substring(0, pos2);
                    cs = cs.Substring(pos2);
                }
            }
            else {
                host = cs.Substring(0, pos);
                cs = cs.Substring(pos + 1);
                int pos2 = cs.IndexOf("/");
                if (-1 == pos2)
                {
                    port = cs;
                    cs = "/";
                }
                else {
                    port = cs.Substring(0, pos2);
                    cs = cs.Substring(pos2);
                }
            }

            // get baseuri
            baseuri = cs;

        }
    }
}
