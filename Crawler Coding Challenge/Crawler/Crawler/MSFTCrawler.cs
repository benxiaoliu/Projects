using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;

namespace Crawler
{
    class MSFTCrawler
    {
        private static string Crawler(string URL)
        {
            WebClient client = new WebClient();

            string downloadedString = client.DownloadString(URL); // get the whole html as a string
            string pattern = "id=\"History\">.*?<h2>"; // get the history section using regular expression
            string historyDoc = Regex.Matches(downloadedString, pattern, RegexOptions.Singleline).First().Value;
            string doc = "";

            doc = Regex.Replace("<" + historyDoc, @"<[^>]*>", "", RegexOptions.Singleline); // remove HTML tags from the crawler string
            doc = Regex.Replace(doc, @"[^a-zA-Z]+", " ", RegexOptions.Singleline); // get only words from the crawler string
         
            return doc;
        }


        // format the crawler string as a dictory with key as word and value as word's frequency
        private static Dictionary<string, int> FormatData(string doc)
        {
            string[] words = doc.Split(" ");
            Dictionary<string, int> wordFrequency = new Dictionary<string, int>();
            foreach (string word in words)
            {
                if (wordFrequency.ContainsKey(word)) wordFrequency[word] += 1;
                else wordFrequency[word] = 1;
            }

            // convert dictory as ordered dictory
            wordFrequency = wordFrequency.OrderByDescending(u => u.Value).ToDictionary(z => z.Key, y => y.Value);
            return wordFrequency;
        }
        private static string[] GetTopKWords(Dictionary<string,int> wordFrequency, System.Collections.Generic.HashSet<string> excepts, int k)  
        {
            string[] result = new String[k];
            int idx = 0;
            foreach (KeyValuePair<string, int> kvp in wordFrequency)
            {
                if (idx == k) break;

                if (excepts.Contains(kvp.Key)) continue;
                 
                result[idx] = kvp.Key;
                idx += 1;
            }
            return result;
        }


        static void Main(string[] args)
        {
            String URL = "https://en.wikipedia.org/wiki/Microsoft";
            string doc = Crawler(URL);

            Dictionary<string, int> wordFrequency = FormatData(doc);

            Console.WriteLine("Input any words you want to exclude from the count. Use spacebar to distinguish different words. If no any, press Enter to continue:");
            string input_excepts = Console.ReadLine();

            Console.WriteLine("Input the number of top words ou want to see. If no any, press Enter to continue, the default is 10:");
            string input_top = Console.ReadLine();

            int top = 10;
            if (input_top != "") top = Convert.ToInt32(input_top);
          
            string[] excepts = input_excepts.Split(" ");

            HashSet<string> excepts_set = new HashSet<string>(excepts);
            string[] results = GetTopKWords(wordFrequency, excepts_set, top);
            foreach (string res in results)
            {
                
                Console.WriteLine(String.Format("{0, -10} {1, 6}", res, wordFrequency[res]));
            }
        }
    }
}
