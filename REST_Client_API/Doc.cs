﻿using System;
using ServiceStack.Text;
using System.IO;
using ServiceStack;

namespace REST_Client_API
{
    public class Doc
    {
        public const string JSON = "json";
        public const string XML = "xml";
        public const string BINARY = "binary";
        public const string TEXT = "text";

        string textContent = null; // for text, json
                                   // TODO binary content
        byte[] fileContent = null;
        // TODO XML content

        public string type { get; set; }

        public bool exists { get; set; }

        public Prop properties { get; set; }

        public Doc()
        {
            exists = false;
            type = JSON;
        }

        public Doc(string type)
        {
            this.type = type;
        }

        public void setJsonContent(string content)
        {
            this.textContent = content;
        }

        public void fromJsonObject(Object obj)
        {
            this.textContent = obj.ToJson();
        }

        public Object toJsonObject()
        {
            //return this.textContent.FromJson (); // TODO research templating and json serialisation in ServiceStack
            return null;
        }

        public JsonObject getJsonContent()
        {
            return JsonObject.Parse(getTextContent());
        }

        public void setFileContent(byte[] content)
        {
            this.fileContent = content;
        }

        public byte[] getFileContent()
        {
            return fileContent;
        }

        public void detectType(string extension)
        {
            // TODO link this to remote server
            // NB those below are default mime - file extension mappings for ML 6
            switch (extension.ToLower())
            {
                case ".xml":
                    type = XML;
                    break;
                case ".xsd":
                    type = XML;
                    break;
                case ".sgml":
                    type = XML;
                    break;
                case ".sgm":
                    type = XML;
                    break;
                case ".svg":
                    type = XML;
                    break;
                case ".xhtml":
                    type = XML;
                    break;
                case ".xslt":
                    type = XML;
                    break;
                case ".xsl":
                    type = XML;
                    break;
                case ".txt":
                    type = TEXT;
                    break;
                case ".csv":
                    type = TEXT;
                    break;
                case ".asc":
                    type = TEXT;
                    break;
                case ".css":
                    type = TEXT;
                    break;
                case ".etx":
                    type = TEXT;
                    break;
                case ".htc":
                    type = TEXT;
                    break;
                case ".html":
                    type = TEXT;
                    break;
                case ".htm":
                    type = TEXT;
                    break;
                case ".js":
                    type = TEXT;
                    break;
                case ".m3u8":
                    type = TEXT;
                    break;
                case ".manifest":
                    type = TEXT;
                    break;
                case ".rtf":
                    type = TEXT;
                    break;
                case ".rtx":
                    type = TEXT;
                    break;
                case ".si":
                    type = TEXT;
                    break;
                case ".sl":
                    type = TEXT;
                    break;
                case ".tsv":
                    type = TEXT;
                    break;
                case ".wml":
                    type = TEXT;
                    break;
                case ".wmls":
                    type = TEXT;
                    break;
                case ".xqy":
                    type = TEXT;
                    break;
                case ".xqe":
                    type = TEXT;
                    break;
                case ".xq":
                    type = TEXT;
                    break;
                case ".xquery":
                    type = TEXT;
                    break;
                case ".json":
                    type = JSON;
                    break;
                default:
                    type = BINARY;
                    break;
            }
        }

        /*
        public void setContent(Object obj) {
          switch (this.type) {
          case JSON:
            textContent = JsonSerializer.SerializeTostring<Response>(obj);
            break;
          case TEXT:
            textContent = obj.Tostring();
            break;
          case XML:
            // TODO
            break;
          case BINARY:
            break;
          }
        }*/

        public string getTextContent()
        {
            return textContent;
        }

        public void toFile(string path)
        {
            File.WriteAllBytes(path, fileContent);
        }

        public void fromFile(string path)
        {
            this.setFileContent(File.ReadAllBytes(path));
            // determine type from file extension
            this.detectType(path.Substring(path.LastIndexOf(".")));
        }
    }
}
