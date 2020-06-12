using System;
using System.Collections.Generic;
using System.ComponentModel.Composition;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using log4net;
using Newtonsoft.Json;
using QueryProbe.Backend.APEnvironment;
using QueryProbe.Backend.DataParsers;
using QueryProbe.Common.Attributes;
using QueryProbe.Common.ClassContracts;
using QueryProbe.Common.ClassContracts.AzureEntities;
using QueryProbe.Common.Constants;
using QueryProbe.Common.DataSources;
using QueryProbe.Common.DataTypes;
using QueryProbe.Common.DataTypes.Factories;
using QueryProbe.Common.DistributedCache;
using QueryProbe.Common.Exceptions;
using QueryProbe.Common.Logging;
using QueryProbe.Common.Parameters;
using QueryProbe.Common.Utilites;
using QueryProbe.Common.Utilites.TlaStatus;
using QueryProbeMonitor.Events;
using System.Net;
using System.Configuration;
using System.Net.Http;

namespace QueryProbe.Backend.DataSources
{
    /// <summary>
    /// Implementation of the Bing data source
    /// </summary>
    [DataSourceExport(typeof(IBingDataSource))]
    public class BingDataSource : DataSource, IBingDataSource
    {
        private static readonly ILog Logger = LogManager.GetLogger(typeof(BingDataSource));

        private readonly BingQuerySettings _setting;
        private readonly IDistributedCache _capturedDataCache;
        private readonly ClassifierParser _classifierParser;
        private readonly IndexUtilities _indexUtility;
        private readonly IDistributedCache _distributedCache;
        private readonly ResultSetFactory _resultSetFactory;
        private readonly Lazy<string[]> _guardSuperfreshTreeMarkets;
        private readonly FlightsDataFetcher _flightsDataFetcher;
        private readonly CapturedDetailsHelper _capturedDetailsHelper;
        private readonly ITlaStatusFetcher _tlaStatusFetcher;
        private readonly ExpClient _expMan;

        [ImportingConstructor]
        public BingDataSource(
            BingQuerySettings setting,
            ResultSetFactory resultSetFactory,
            ActionLogger actionLogger,
            ClassifierParser classifierParser,
            [Import(ConfigurationManagerConfigReader.ConfigurationManagerConfigReaderExport)]
            ConfigReader globalConfiguration,
            IndexUtilities indexUtility,
            FlightsDataFetcher flightsDataFetcher,
            [Import(DistributedCacheFactory.DistributedCacheExport)]
            IDistributedCache distributedCache,
            [Import(DistributedCacheFactory.CapturedDataCacheExport)]
            IDistributedCache capturedDataCache,
            CapturedDetailsHelper capturedDetailsHelper,
            ITlaStatusFetcher tlaStatusFetcher)
            : base(actionLogger)
        {
            _setting = setting;
            _resultSetFactory = resultSetFactory;
            _classifierParser = classifierParser;
            _indexUtility = indexUtility;
            _flightsDataFetcher = flightsDataFetcher;
            _guardSuperfreshTreeMarkets = new Lazy<string[]>(() => globalConfiguration.ReadConfig<string>("GRMarketsWithGuardSFTree").ToLowerInvariant().Split(new[] { ',' }));

            _distributedCache = distributedCache;
            _capturedDataCache = capturedDataCache;
            _capturedDetailsHelper = capturedDetailsHelper;
            _tlaStatusFetcher = tlaStatusFetcher;

            _expMan = new ExpClient();
        }

        [Resource("BingUrlExtendedData", new[] { "WebResults", "Captions" })]
        public UrlExtendedData ExtendedDataForUrl(QueryParameter query, UrlParameter url, TierParameter tier, StringStringDictionaryParameter pluginInfo = null)
        {
            TestIndex();
            return ExtendedDataForUrlInternal(query, url, tier, null, pluginInfo);
        }

        [Resource("PassageExtendedData", new[] { "WebResults" })]
        public PassageFeatures ExtendedDataForPassage(QueryParameter query, UrlParameter url)
        {
            return ExtendedDataForPassageInternal(query, url);
        }

        public PbxmlData GeneratePbxmlLink(QueryParameter query, ResultCountParameter count)
        {
            return GeneratePbxmlLinkInternal(query, count);
        }

        [Resource("BingRankedResults", new[] { "WebResults", "Social", "Captions" })]
        public ResultSet RankedResults(QueryParameter query, ResultCountParameter count, StringStringDictionaryParameter pluginInfo, StringParameter pbxml)
        {
            return RankedResultsInternal(query, count, null, pluginInfo, pbxml);
        }

        public ResultSet RankedResults(QueryParameter query, ResultCountParameter count, StringStringDictionaryParameter pluginInfo = null)
        {
            return RankedResultsInternal(query, count, null, pluginInfo, null);
        }

        /// <summary>
        /// Normalize the provided query in DataSource-specific manner
        /// </summary>
        [Resource("BingNormalizedQuery", new[] { "WebResults", "Entity", "Hero", "Images", "Videos", "Satori", "SBI", "News", "NewsAnswer" })] // TODO: this should not be required.
        public ParsedQueryData NormalizeQuery(QueryParameter queryParameter)
        {
            return QueryNormalization.NormalizeQuery(queryParameter);
        }

        [Resource("BingRankedResults", new[] { "Hero" })]
        public ResultSet SearchCharmRankedResults(QueryParameter query, ResultCountParameter count)
        {
            return RankedResultsInternal(query, count, UpdateBingQueryBuilderForSearchCharm);
        }

        [Resource("BingUrlExtendedData", new[] { "Hero" })]
        public UrlExtendedData SearchCharmExtendedDataForUrl(QueryParameter query, UrlParameter url, TierParameter tier)
        {
            return ExtendedDataForUrlInternal(query, url, tier, UpdateBingQueryBuilderForSearchCharm);
        }

        [Resource("CaptionOverrides", new[] { "Captions" })]
        public CaptionOverridesData CaptionOverrides(QueryParameter query, UrlParameter url)
        {
            int[] incrementalResultCounts = new[] { 10, 20, 50, 100 };
            ResultCountParameter count = new ResultCountParameter();
            CaptionOverridesData captionOverridesData = new CaptionOverridesData();

            foreach (int resultCount in incrementalResultCounts)
            {
                count.Count = resultCount;
                ResultSet resultSet = RankedResultsInternal(query, count, null);
                ExtendedResult result = resultSet.GetMatchingResults(url.Hash).FirstOrDefault() as ExtendedResult;
                if (result != null)
                {
                    captionOverridesData.CaptionUpdateDetails = result.CaptionDetails.CaptionUpdateDetails;
                    captionOverridesData.UpdatedUrl = result.CaptionDetails.UpdatedUrl;
                    captionOverridesData.UpdatedTitle = result.CaptionDetails.UpdatedTitle;
                    captionOverridesData.UpdatedSnippet = result.CaptionDetails.UpdatedSnippet;
                    captionOverridesData.DocId = result.DocId;
                    captionOverridesData.Url = result.Url;
                    captionOverridesData.UrlHash = result.PrimaryHash;
                    break;
                }
            }

            return captionOverridesData;
        }

        [Resource("NotRegisteredFlights", new[] { "WebResults", "Images", "Videos", "Entity", "Hero", "Satori", "SBI", "News", "NewsAnswer" })]
        public StringListData NotRegisteredFlights(FlightsParameter flights)
        {
            return new StringListData()
            {
                Values = _expMan.GetNotRegisteredFlights(flights.Flights.Select(f => f.FlightId).ToList()) ////_flightsDataFetcher.GetNotRegisteredFlights(flights.Flights)
            };
        }

        [Resource("CaptureBingRankedResults", new[] { "WebResults" })]
        public CaptureDetailsData CaptureRankedResults(CaptureRankedResultsParameter captureParameter)
        {
            if (!String.IsNullOrWhiteSpace(captureParameter.ParentCaptureId) &&
                !String.IsNullOrWhiteSpace(captureParameter.ParentSessionId))
            {
                return CloneCapturedRankedResults(new StringParameter() { Value = captureParameter.ParentCaptureId }, new StringParameter() { Value = captureParameter.ParentSessionId }, new StringParameter() { Value = captureParameter.SessionId }, new StringParameter() { Value = captureParameter.UserName }, new StringParameter() { Value = captureParameter.Description });
            }

            return CaptureRankedResultsInternal(new QueryParameter() { QueryText = captureParameter.Query }, new ResultCountParameter() { Count = captureParameter.ResultCount }, new StringParameter() { Value = captureParameter.SessionId }, new StringParameter() { Value = captureParameter.UserName }, new StringParameter() { Value = captureParameter.Description });
        }

        [Resource("RestoreBingRankedResults", new[] { "WebResults" })]
        public ResultSet RestoreRankedResults(RestoreRankedResultsParameter restoreParameter)
        {
            return RestoreRankedResultsInternal(new StringParameter() { Value = restoreParameter.CaptureId }, new StringParameter() { Value = restoreParameter.CapturedSessionId });
        }

        [Resource("CaptureBingUrlExtendedData", new[] { "WebResults", "Images" })]
        public CaptureDetailsData CaptureExtendedDataForUrl(CaptureExtendedDataParameter captureParameter)
        {
            if (!String.IsNullOrWhiteSpace(captureParameter.ParentBingResultsCaptureId) &&
                !String.IsNullOrWhiteSpace(captureParameter.ParentSessionId) &&
                !String.IsNullOrWhiteSpace(captureParameter.ParentPbxmlId))
            {
                return CloneCapturedExtendedDataForUrl(new StringParameter() { Value = captureParameter.ParentBingResultsCaptureId }, new StringParameter() { Value = captureParameter.ParentSessionId }, new StringParameter() { Value = captureParameter.ParentPbxmlId }, new StringParameter() { Value = captureParameter.SessionId }, new StringParameter() { Value = captureParameter.BingResultsCaptureId }, new UrlParameter() { Url = captureParameter.Url, PrimaryHash = captureParameter.PrimaryHash }, new StringParameter() { Value = captureParameter.UserName });
            }

            CaptureDetailsData captureDetailsData = CaptureExtendedDataForUrlInternal(new QueryParameter() { QueryText = captureParameter.Query }, new UrlParameter() { Url = captureParameter.Url, PrimaryHash = captureParameter.PrimaryHash }, new TierParameter() { Tier = captureParameter.Tier }, new StringParameter() { Value = captureParameter.BingResultsCaptureId }, new StringParameter() { Value = captureParameter.SessionId }, new StringParameter() { Value = captureParameter.UserName });

            // Update capture details in the context of the captured Bing result set
            CaptureDetailsEntity entity = _capturedDetailsHelper.GetDetails(captureParameter.SessionId, captureParameter.BingResultsCaptureId);
            if (entity != null)
            {
                // Add or update list of capture details per PBXML ID
                Dictionary<string, Tuple<string, CaptureDetails>> perPbxmlIdCaptureDetails;
                if (entity.AdditionalUrlsCaptureDetails == null)
                {
                    perPbxmlIdCaptureDetails = new Dictionary<string, Tuple<string, CaptureDetails>>();
                }
                else
                {
                    perPbxmlIdCaptureDetails = JsonConvert.DeserializeObject<Dictionary<string, Tuple<string, CaptureDetails>>>(entity.AdditionalUrlsCaptureDetails);
                }

                perPbxmlIdCaptureDetails.Add(captureDetailsData.CaptureDetails.PbxmlId, new Tuple<string, CaptureDetails>(captureParameter.PrimaryHash, captureDetailsData.CaptureDetails));
                entity.AdditionalUrlsCaptureDetails = JsonConvert.SerializeObject(perPbxmlIdCaptureDetails, Formatting.None);

                DetailedCallResult tableCallResult = _capturedDetailsHelper.UpdateDetails(entity);
                if (!String.IsNullOrWhiteSpace(tableCallResult.Error))
                {
                    captureDetailsData.Errors.Add(String.Format("Error occurred while adding capture details data for URL '{0}' (query '{1}') associated with session '{2}' (capture ID '{3}').  Details:", captureParameter.Url, captureParameter.Query, captureParameter.SessionId, captureParameter.BingResultsCaptureId));
                    captureDetailsData.Errors.Add(tableCallResult.Error);
                }
            }
            else
            {
                captureDetailsData.Errors.Add(String.Format("Could not get a record of capture details previously stored for Bing results in session '{0}' with capture ID '{1}'.", captureParameter.SessionId, captureParameter.BingResultsCaptureId));
            }

            return captureDetailsData;
        }
        [Resource("DLISData", new[] { "WebResults" })]
        public StringData CallDLISAPI(StringParameter requestInput, StringParameter endpoint)
        {
            HttpClient client = new HttpClient();
            var request_input = requestInput.Value;
            var endpointInput = endpoint.Value; 
            var request = new StringContent(request_input); 
            var response = client.PostAsync(endpointInput, request).Result;
            var responseString = response.Content.ReadAsStringAsync().Result;
            return new StringData()
            {
                Value = responseString
            };
        }
        [Resource("RestoreBingUrlExtendedData", new[] { "WebResults", "Images" })]  
        public UrlExtendedData RestoreExtendedDataForUrl(RestoreExtendedDataParameter restoreParameter)
        {
            return RestoreExtendedDataForUrlInternal(new StringParameter() { Value = restoreParameter.PbxmlId }, new UrlParameter() { Url = restoreParameter.Url, PrimaryHash = restoreParameter.PrimaryHash }, new StringParameter() { Value = restoreParameter.CaptureId }, new StringParameter() { Value = restoreParameter.CapturedSessionId });
        }

        private static void UpdateBingQueryBuilderWithOptions(
            BingQueryBuilder builder,
            bool getRerankingDetails,
            bool getTokenMatchingData,
            bool setFilter)
        {
            if (getRerankingDetails)
            {
                builder.AddUserAugmentation("[dbg:TLARerankInfo]");
            }

            if (getTokenMatchingData)
            {
                builder.AddUserAugmentation("[dbg=matchinfo]");
            }

            if (setFilter)
            {
                // Set service to avoid exceeding the 10 MB max size for PBXML set by the SNR team
                // See https://msasg.visualstudio.com/Engineering%20Fundamentals/_git/bingwiki_markdown#path=%2Fmarkdown%2FPbxml.md&version=GBmaster&_a=contents
                builder.Filter = "WebAnswer";
            }
        }

        private void UpdateBingQueryBuilderForSearchCharm(BingQueryBuilder builder)
        {
            if (string.IsNullOrWhiteSpace(_setting.QueryRouting.XapWorkflow))
            {
                builder.XapWorkflow = "WinSearchCharm.BingFirstPageResults";
            }

            builder.SetVariantConstraint("browser", "charm");
        }

        private string KeyCallback(Stream response, string traceIdFromHeaders)
        {
            string traceId = traceIdFromHeaders;
            if (string.IsNullOrEmpty(traceId))
            {
                var parser = new PbXmlParser(
                    response,
                    _setting.QueryRouting.Index,
                    _classifierParser,
                    _indexUtility,
                    _setting);
                traceId = parser.TraceId;
            }

            ActionLogger.StartAction("BingDataSource", "Cache pbxml", "Pbxml trace Id: " + traceId);

            return traceId;
        }

        private UrlExtendedData ExtendedDataForUrlInternal(QueryParameter query, UrlParameter url, TierParameter tier, Action<BingQueryBuilder> queryUpdater, StringStringDictionaryParameter pluginInfo = null)
        {
            var updater = GetExtendedDataQueryBuilderUpdater(url, tier, queryUpdater);

            var data = new UrlExtendedData();
            Action<PbXmlParser, ResultSet> resultsCustomizer = GetExtendedDataResultsCustomizer(url, data, false);

            var requery = pluginInfo != null ? !bool.Parse(pluginInfo["isPermalink"]) : true;
            if (requery == false)
            {
                var pbxmlPath = pluginInfo["cachedResponseUri"] + "/pbxml.xml";
                var cachedResponseUri = pluginInfo["cachedResponseUri"];
                var cachedSazUri = pluginInfo["cachedSazUri"];
                _setting.QueryRouting.SetIsIndexSwitch(bool.Parse(pluginInfo["isIndexSwitch"]));
                Stream contentStream = null;
                try
                {
                    System.Net.WebClient c = new System.Net.WebClient();
                    Byte[] pbxmlContent = c.DownloadData(pbxmlPath);
                    contentStream = new MemoryStream(pbxmlContent);
                }
                catch (Exception ex)
                {
                    requery = true;
                    Logger.Warn(LoggerParser.GetLoggerContent("BingDataSource pbxml file download faild. except message:" + ex.Message + ";the pbxml path:" + pluginInfo["cachedResponseUri"], "", ex));
                }
                if (requery == false)
                {
                    FetchAndProcessResultsByPbxml(query, updater, resultsCustomizer, contentStream, cachedResponseUri, cachedSazUri);
                }
            }
            if (requery == true)
            {
                FetchAndProcessResults(query, updater, resultsCustomizer, null, false);
            }

            return data;
        }

        private PassageFeatures ExtendedDataForPassageInternal(QueryParameter query, UrlParameter url)
        {
            var updater = GetPassageExtendedDataQueryBuilderUpdater(url);

            var data = new PassageFeatures();
            Action<PbXmlParser, ResultSet> resultsCustomizer = GetPassageExtendedDataResultsCustomizer(url, data);

            FetchAndProcessResults(query, updater, resultsCustomizer, null, false);
            return data;
        }

        [Resource("SinglePathFeatureResults", new[] { "WebResults" })]
        public UrlExtendedData GetSinglePathFeatureResults(QueryParameter query, UrlParameter url, StringParameter path)
        {
            var updater = GetSinglePathDataQueryBuilderUpdater(url, path.Value);
            var data = new UrlExtendedData();
            Action<PbXmlParser, ResultSet> resultsCustomizer = GetExtendedDataResultsCustomizer(url, data, false);

            FetchAndProcessResults(query, updater, resultsCustomizer, null, false);

            return data;
        }

        [Resource("PbxmlResults", new[] { "WebResults" })]
        public UrlExtendedData GetPbxmlResultsFromAddress(QueryParameter query, StringParameter path)
        {
            var updater = GetPbxmlDataQueryBuilderUpdater();
            var data = new UrlExtendedData();
            Action<PbXmlParser, ResultSet> resultsCustomizer = GetPbxmlResultsCustomizer(data);

            ProcessPbxmlResults(path, updater, resultsCustomizer, query);

            return data;
        }

        private Action<BingQueryBuilder> GetExtendedDataQueryBuilderUpdater(UrlParameter url, TierParameter tier, Action<BingQueryBuilder> queryUpdater)
        {
            return new Action<BingQueryBuilder>(builder =>
            {
                url.UpdateBingQueryBuilderWithFeaturesRequest(builder);
                tier.UpdateBingQueryBuilderWithForcedTier(builder);
                UpdateBingQueryBuilderWithOptions(builder, false, true, true);
                if (queryUpdater != null) { queryUpdater(builder); }
            });
        }

        private Action<BingQueryBuilder> GetPassageExtendedDataQueryBuilderUpdater(UrlParameter url)
        {
            return new Action<BingQueryBuilder>(builder =>
            {
                url.UpdateBingQueryBuilderWithPassageFeaturesRequest(builder);
            });
        }

        private Action<BingQueryBuilder> GetPbxmlDataQueryBuilderUpdater()
        {
            return new Action<BingQueryBuilder>(builder => { });
        }

        private Action<BingQueryBuilder> GetSinglePathDataQueryBuilderUpdater(UrlParameter url, string path)
        {
            return new Action<BingQueryBuilder>(builder =>
            {
                url.UpdateBingQueryBuilderWithSingleQueryPath(builder, path);
                UpdateBingQueryBuilderWithOptions(builder, false, false, true);
            });
        }

        private Action<PbXmlParser, ResultSet> GetExtendedDataResultsCustomizer(UrlParameter url, UrlExtendedData data, bool restored)
        {
            return new Action<PbXmlParser, ResultSet>((parser, innerResults) =>
            {
                data.ResultSet = innerResults;
                if (restored)
                {
                    data.ResultSet.QueryFlags[QueryFlagNames.RestoredResults] = true;
                }
                string urlHash = UrlHashing.GetUrlHash(url.Url);
                data.FeaturesData = parser.ParseToFeatureSet(url).UrlVectors.Where(p => p.Value.Hashes.Contains(urlHash)).Select(p => p.Value).SingleOrDefault();
                data.ResultData = data.ResultSet.GetMatchingResults(urlHash).FirstOrDefault() as ExtendedResult;
                data.ResultSet.Results.Clear(); // remove redundant result data
            });
        }

        private Action<PbXmlParser, ResultSet> GetPbxmlResultsCustomizer(UrlExtendedData data)
        {
            return new Action<PbXmlParser, ResultSet>((parser, innerResults) =>
            {
                data.ResultSet = innerResults;
                data.ResultSet.Results.Clear(); // remove redundant result data
            });
        }

        private Action<PbXmlParser, ResultSet> GetPassageExtendedDataResultsCustomizer(UrlParameter url, PassageFeatures data)
        {
            return new Action<PbXmlParser, ResultSet>((parser, innerResults) =>
            {
                data.FeaturesByRanker = parser.ParseToPassageFeatures(innerResults, url);
            });
        }

        private PbxmlData GeneratePbxmlLinkInternal(QueryParameter query, ResultCountParameter count)
        {
            var updater = GetRankedResultsQueryBuilderUpdater(count, null);

            BingQueryBuilder builder = GetConfiguredQueryBuilder(query, null);

            IDistributedCache cache = _distributedCache;

            PbxmlData pbxmlData = new PbxmlData();
            using (BingRequestHelperWithCaching requestHelper = _setting.QueryRouting.GetBingRequestHelperWithCaching(builder, cache, KeyCallback))
            {
                BingHttpResponseMessage responseMessage = null;
                try
                {
                    responseMessage = requestHelper.GetResponseMessage();
                    WebRequestVariants webRequestVariants = requestHelper.GetRequestVariantsWithCaching(responseMessage);
                    pbxmlData.PbxmlLink = webRequestVariants.PrimaryRecord.CachedResponseUri;
                }
                catch (Exception ex)
                {
                    RemoteDataException remoteDataException = new RemoteDataException(
                        "Exception getting Bing data, see error details",
                        string.Format("{0}: {1}", ex.GetType(), ex.Message),
                        ex);
                    throw remoteDataException;
                }
                finally
                {
                    if (responseMessage != null)
                    {
                        responseMessage.Dispose();
                    }
                }
            }
            return pbxmlData;
        }

        private ResultSet RankedResultsInternal(QueryParameter query, ResultCountParameter count, Action<BingQueryBuilder> queryUpdater, StringStringDictionaryParameter pluginInfo = null, StringParameter pbxml = null)
        {
            var updater = GetRankedResultsQueryBuilderUpdater(count, queryUpdater);

            ResultSet results = null;
            var resultsCustomizer = new Action<PbXmlParser, ResultSet>((parser, innerResults) =>
            {
                results = innerResults;

                // include live index switch status information for webprecisionexp
                if (_setting.QueryRouting.IsExperimentalConfiguration)
                {
                    TierInfo webPrecisionExpInfo = results.TierInfoList.FirstOrDefault(t => string.Compare(TlaStatusClient.WebPrecisionExp, t.TierName, StringComparison.OrdinalIgnoreCase) == 0);
                    if (webPrecisionExpInfo != null)
                    {
                        int availableRowCount;
                        if (_tlaStatusFetcher.TryGetWebPrecisionExpAvailableRowCount(out availableRowCount))
                        {
                            webPrecisionExpInfo.AvailableRowCount = availableRowCount;
                        }
                    }
                }
            });
            if (pbxml != null && !string.IsNullOrEmpty(pbxml.Value))
            {
                string pbxmlPath = pbxml.Value;
                var cachedResponseUri = pbxml.Value;
                var cachedSazUri = pbxml.Value;
                Stream contentStream = null;
                try
                {
                    System.Net.WebClient c = new System.Net.WebClient();
                    Byte[] pbxmlContent = c.DownloadData(pbxmlPath);
                    contentStream = new MemoryStream(pbxmlContent);
                }
                catch (Exception)
                {
                    QueryProbeBackEndException remoteDataException = new QueryProbeBackEndException("cached pbxml does not exist", ErrorCodeEnum.PbxmlUrlError);
                    throw remoteDataException;
                }
                FetchAndProcessResultsByPbxml(query, updater, resultsCustomizer, contentStream, cachedResponseUri, cachedSazUri);
                return results;
            }
            var requery = pluginInfo != null ? !bool.Parse(pluginInfo["isPermalink"]) : true;
            if (requery == false)
            {
                var pbxmlPath = pluginInfo["cachedResponseUri"] + "/pbxml.xml";
                var cachedResponseUri = pluginInfo["cachedResponseUri"];
                var cachedSazUri = pluginInfo["cachedSazUri"];
                _setting.QueryRouting.SetIsIndexSwitch(bool.Parse(pluginInfo["isIndexSwitch"]));
                Stream contentStream = null;
                try
                {
                    System.Net.WebClient c = new System.Net.WebClient();
                    Byte[] pbxmlContent = c.DownloadData(pbxmlPath);
                    contentStream = new MemoryStream(pbxmlContent);
                }
                catch (Exception ex)
                {
                    requery = true;
                    Logger.Warn(LoggerParser.GetLoggerContent("BingDataSource pbxml file download faild. except message:" + ex.Message + ";the pbxml path:" + pluginInfo["cachedResponseUri"], "", ex));
                }
                if (requery == false)
                {
                    FetchAndProcessResultsByPbxml(query, updater, resultsCustomizer, contentStream, cachedResponseUri, cachedSazUri);
                }
            }
            if (requery == true)
            {
                FetchAndProcessResults(query, updater, resultsCustomizer, null, false);
            }
            return results;
        }

        private Action<BingQueryBuilder> GetRankedResultsQueryBuilderUpdater(ResultCountParameter count, Action<BingQueryBuilder> queryUpdater)
        {
            return new Action<BingQueryBuilder>(builder =>
            {
                count.UpdateBingQueryBuilder(builder);
                UpdateBingQueryBuilderWithOptions(builder, true, false, false);
                if (queryUpdater != null) { queryUpdater(builder); }
            });
        }

        private CaptureDetailsData CaptureRankedResultsInternal(QueryParameter query, ResultCountParameter resultCount, StringParameter sessionId, StringParameter userName, StringParameter description)
        {
            string startActionMessage = String.Format("Capturing Bing top {0} results for query '{1}' issued by user '{2}' and extended data for each result for session ID {3}.", resultCount.Count, query.QueryText, userName.Value, sessionId.Value);

            CaptureDetailsData detailsData = new CaptureDetailsData()
            {
                CaptureId = CreateNewCaptureId(),
                SessionId = sessionId.Value,
                UserName = userName.Value,
                Description = description.Value
            };

            ActionMessage startAction = ActionLogger.StartAction("BingDataSource", "Capture Bing results", startActionMessage);

            Action<BingQueryBuilder> queryUpdater = GetRankedResultsQueryBuilderUpdater(resultCount, null);
            detailsData.CaptureDetails = new CaptureDetails();
            Action<BingHttpResponseMessage> responseCustomizer = GetResponseCustomizer(detailsData.CaptureDetails);
            ResultSet results = null;
            var resultsCustomizer = new Action<PbXmlParser, ResultSet>((parser, innerResults) =>
            {
                results = innerResults;
            });

            int resultsWithExtendedDataCount = 0;
            try
            {
                // Bing top N result set (currently, without feature data)
                FetchAndProcessResults(query, queryUpdater, resultsCustomizer, responseCustomizer, true);

                if (results.Results.Count < resultCount.Count)
                {
                    detailsData.Errors.Add(String.Format("Requested {0} results be captured but Bing returned {1}.", resultCount.Count, results.Results.Count));
                }

                // Add capture details about Bing top N result set in Azure
                CaptureDetailsEntity bingTopNDetailsEntity = new CaptureDetailsEntity(detailsData.CaptureId, userName.Value, query.QueryText, sessionId.Value, detailsData.CaptureDetails.PbxmlId, detailsData.CaptureDetails.CaptureTimestamp, resultCount.Count, null, null, detailsData.CaptureDetails.CachedResponseUri, detailsData.CaptureDetails.CachedSazUri, description.Value);
                DetailedCallResult tableCallResult = _capturedDetailsHelper.AddDetails(bingTopNDetailsEntity);
                if (!String.IsNullOrWhiteSpace(tableCallResult.Error))
                {
                    detailsData.Errors.Add(String.Format("Error occurred while adding capture details data of Bing top {0} result set from session '{1}' in Azure.  Details:", resultCount.Count, sessionId.Value));
                    detailsData.Errors.Add(tableCallResult.Error);
                }

                // Extended results plus feature data for each result in Bing top N result set
                Dictionary<string, CaptureDetails> resultsCaptureDetails = new Dictionary<string, CaptureDetails>();
                foreach (Result result in results.Results)
                {
                    UrlParameter url = new UrlParameter()
                    {
                        Url = result.Url,
                        PrimaryHash = result.PrimaryHash
                    };

                    TierParameter tier = new TierParameter();
                    if (result is ExtendedResult)
                    {
                        tier.Tier = ((ExtendedResult)result).Tier;
                    }

                    CaptureDetailsData resultCaptureDetailsData = CaptureExtendedDataForUrlInternal(query, url, tier, new StringParameter() { Value = detailsData.CaptureId }, sessionId, userName);
                    if (resultCaptureDetailsData.Errors.Count != 0)
                    {
                        resultCaptureDetailsData.Errors.ToList().ForEach(error => detailsData.Errors.Add(error));
                    }
                    else
                    {
                        resultsCaptureDetails.Add(result.PrimaryHash, resultCaptureDetailsData.CaptureDetails);
                        resultsWithExtendedDataCount++;
                    }

                    // Throttle the number of extended-data requests sent to the backend
                    Thread.Sleep(TimeSpan.FromSeconds(1));
                }

                // Add capture details about each Bing top N result in Azure
                if (resultsWithExtendedDataCount > 0)
                {
                    bingTopNDetailsEntity.BingTopNCaptureDetails = JsonConvert.SerializeObject(resultsCaptureDetails, Formatting.None);
                    tableCallResult = _capturedDetailsHelper.UpdateDetails(bingTopNDetailsEntity);
                    if (!String.IsNullOrWhiteSpace(tableCallResult.Error))
                    {
                        detailsData.Errors.Add("Error occurred while adding capture details data of each of Bing top N results in Azure.  Details:");
                        detailsData.Errors.Add(tableCallResult.Error);
                    }
                }
            }
            catch (Exception exception)
            {
                string message = String.Format("Exception thrown when capturing Bing top {0} results for query '{1}' issued by user '{2}' and extended data for each result for session ID {3}.  Details: {4}", resultCount.Count, query.QueryText, userName.Value, sessionId.Value, exception);
                detailsData.Errors.Add(message);
                Logger.Warn(LoggerParser.GetLoggerContent(message, "", exception));

                WebRequestRecord request = null;
                if (exception is RemoteDataException)
                {
                    request = ((RemoteDataException)exception).RequestVariants.PrimaryRecord;
                }
                else if (exception is RemoteSystemException)
                {
                    request = ((RemoteSystemException)exception).RequestVariants.PrimaryRecord;
                }

                if (request != null)
                {
                    message = String.Format("{0} request URL: {1}", request.Method, request.RequestUri);
                    detailsData.Errors.Add(message);
                    Logger.Warn(LoggerParser.GetLoggerContent(message));
                }
            }

            if (detailsData.Errors.Count > 0)
            {
                ActionLogger.EndAction(startAction, String.Format("Finished attempt to capture Bing top {0} results for query '{1}' issued by user '{2}' and extended data for each result for session ID {3}.  Attempt failed.  Details: {4}", resultCount.Count, query.QueryText, userName.Value, sessionId.Value, String.Join(Environment.NewLine, detailsData.Errors)));
            }
            else
            {
                ActionLogger.EndAction(startAction, String.Format("Finished capturing Bing top {0} results for query '{1}' issued by user '{2}' and extended data for each result for session ID {3}.", resultCount.Count, query.QueryText, userName.Value, sessionId.Value));
            }

            return detailsData;
        }

        private CaptureDetailsData CloneCapturedRankedResults(StringParameter parentCaptureId, StringParameter parentSessionId, StringParameter sessionId, StringParameter userName, StringParameter description)
        {
            CaptureDetailsEntity entity = _capturedDetailsHelper.GetDetails(parentSessionId.Value, parentCaptureId.Value);
            if (entity == null)
            {
                throw new QueryProbeBackEndException(String.Format("When cloning a previously captured Bing result set could not get a record of capture details previously stored for Bing results in session '{0}' with capture ID '{1}'.", parentSessionId.Value, parentCaptureId.Value));
            }

            // Captured sessions are immutable and as such, capturing a previously-restore (i.e., "parent") session
            // results in a new captured session and the corresponding entry in the capture details Azure table.

            List<string> errors = new List<string>();

            // Clone capture details about Bing top N result set in Azure but exclude capture details of
            // the cart items in that session
            CaptureDetailsEntity extendedEntity = new CaptureDetailsEntity(CreateNewCaptureId(), userName.Value, entity.Query, sessionId.Value, entity.BingTopNPbxmlId, entity.BingTopNCaptureTimestamp, entity.ResultCount, entity.BingTopNCaptureDetails, null, entity.CachedBingTopNResponseUri, entity.CachedBingTopNSazUri, description.Value);

            DetailedCallResult tableCallResult = _capturedDetailsHelper.AddDetails(extendedEntity);
            if (!String.IsNullOrWhiteSpace(tableCallResult.Error))
            {
                errors.Add(String.Format("Error occurred while adding cloned capture details data of parent Bing top {0} result set in Azure previously captured in session ID '{1}'.  Details:", entity.ResultCount, entity.SessionId));
                errors.Add(tableCallResult.Error);
            }

            CaptureDetails captureDetails = new CaptureDetails()
            {
                CachedResponseUri = extendedEntity.CachedBingTopNResponseUri,
                CachedSazUri = extendedEntity.CachedBingTopNSazUri,
                CaptureTimestamp = extendedEntity.BingTopNCaptureTimestamp,
                PbxmlId = extendedEntity.BingTopNPbxmlId
            };

            return new CaptureDetailsData()
            {
                CaptureDetails = captureDetails,
                CaptureId = extendedEntity.CaptureId,
                SessionId = sessionId.Value,
                Errors = errors,
                UserName = userName.Value,
                Description = extendedEntity.Description
            };
        }

        private ResultSet RestoreRankedResultsInternal(StringParameter captureId, StringParameter capturedSessionId)
        {
            CaptureDetailsEntity entity = _capturedDetailsHelper.GetDetails(capturedSessionId.Value, captureId.Value);
            if (entity == null)
            {
                throw new QueryProbeBackEndException(String.Format("Could not get a record of capture details previously stored for Bing results in session '{0}' with capture ID '{1}'.", capturedSessionId.Value, captureId.Value));
            }

            ResultSet results = null;

            using (DistributedCacheGetResult cacheResult = _capturedDataCache.GetAsync(entity.BingTopNPbxmlId).Result)
            using (Stream resultsContent = cacheResult.Data)
            {
                var resultsCustomizer = new Action<PbXmlParser, ResultSet>((parser, innerResults) =>
                {
                    results = innerResults;
                    results.QueryFlags[QueryFlagNames.RestoredResults] = true;
                });

                BingQueryBuilder builder = GetConfiguredQueryBuilder(new QueryParameter() { QueryText = entity.Query }, null);
                BingRequestHelperWithCaching requestHelper =
                    _setting.QueryRouting.GetBingRequestHelperWithCaching(builder, _capturedDataCache, KeyCallback);
                WebRequestVariants requestVariants =
                    requestHelper.GetRequestVariantsWithCaching(entity.CachedBingTopNResponseUri, entity.CachedBingTopNSazUri);

                ProcessResults(resultsContent, resultsCustomizer, requestVariants, builder);
            }

            return results;
        }

        private CaptureDetailsData CaptureExtendedDataForUrlInternal(QueryParameter query, UrlParameter url, TierParameter tier, StringParameter bingResultsCaptureId, StringParameter sessionId, StringParameter userName)
        {
            const string UnspecifiedTier = "[Unspecified/Unknown]";

            CaptureDetailsData detailsData = new CaptureDetailsData()
            {
                CaptureId = bingResultsCaptureId.Value,
                SessionId = sessionId.Value,
                UserName = userName.Value
            };

            ActionMessage startAction = ActionLogger.StartAction("BingDataSource", "Capture extended data for a URL", String.Format("Capturing extended data for URL '{0}' from tier '{1}' for query '{2}' from session ID {3}.", url.Url, tier.Tier ?? UnspecifiedTier, query.QueryText, sessionId));

            Action<BingQueryBuilder> queryUpdater = GetExtendedDataQueryBuilderUpdater(url, tier, null);
            CaptureDetails captureDetails = new CaptureDetails();
            Action<BingHttpResponseMessage> responseCustomizer = GetResponseCustomizer(captureDetails);

            try
            {
                FetchAndProcessResults(query, queryUpdater, null, responseCustomizer, true);
                detailsData.CaptureDetails = captureDetails;
            }
            catch (Exception exception)
            {
                string message = String.Format("Exception thrown when capturing feature data for URL '{0}' from tier '{1}' for query '{2}'.  Details: {3}", url.Url, tier.Tier ?? UnspecifiedTier, query.QueryText, exception);
                detailsData.Errors.Add(message);
                Logger.Warn(LoggerParser.GetLoggerContent(message, "", exception));

                WebRequestRecord request = null;
                if (exception is RemoteDataException)
                {
                    request = ((RemoteDataException)exception).RequestVariants.PrimaryRecord;
                }
                else if (exception is RemoteSystemException)
                {
                    request = ((RemoteSystemException)exception).RequestVariants.PrimaryRecord;
                }

                if (request != null)
                {
                    message = String.Format("{0} request URL: {1}", request.Method, request.RequestUri);
                    detailsData.Errors.Add(message);
                    Logger.Warn(LoggerParser.GetLoggerContent(message));
                }
            }

            if (detailsData.Errors.Count > 0)
            {
                ActionLogger.EndAction(startAction, String.Format("Finished attempt to capture extended data for URL '{0}' from tier '{1}' for query '{2}' from session ID {3}.  Attempt failed.  Details: {4}", url.Url, tier.Tier ?? UnspecifiedTier, query.QueryText, sessionId, String.Join(Environment.NewLine, detailsData.Errors)));
            }
            else
            {
                ActionLogger.EndAction(startAction, String.Format("Finished capturing extended data for URL '{0}' from tier '{1}' for query '{2}' from session ID {3}.", url.Url, tier.Tier ?? UnspecifiedTier, query.QueryText, sessionId));
            }

            return detailsData;
        }

        private CaptureDetailsData CloneCapturedExtendedDataForUrl(StringParameter parentCaptureId, StringParameter parentSessionId, StringParameter parentPbxmlId, StringParameter sessionId, StringParameter bingResultsCaptureId, UrlParameter url, StringParameter userName)
        {
            CaptureDetailsEntity parentEntity = _capturedDetailsHelper.GetDetails(parentSessionId.Value, parentCaptureId.Value);
            if (parentEntity == null)
            {
                throw new QueryProbeBackEndException(String.Format("When seeking a previously captured Bing result set could not get a record of capture details previously stored for Bing results in session '{0}' with capture ID '{1}'.", parentSessionId.Value, parentCaptureId.Value));
            }

            Dictionary<string, Tuple<string, CaptureDetails>> perPbxmlIdCaptureDetails = JsonConvert.DeserializeObject<Dictionary<string, Tuple<string, CaptureDetails>>>(parentEntity.AdditionalUrlsCaptureDetails);

            if (!perPbxmlIdCaptureDetails.ContainsKey(parentPbxmlId.Value))
            {
                throw new QueryProbeBackEndException(String.Format("Could not find a record of saved captured PBXML ID '{0}' for previously stored for Bing results in session '{1}' with capture ID '{2}'.", parentPbxmlId.Value, parentSessionId.Value, parentCaptureId.Value));
            }

            string primaryHash = perPbxmlIdCaptureDetails[parentPbxmlId.Value].Item1;
            CaptureDetails parentCaptureDetails = perPbxmlIdCaptureDetails[parentPbxmlId.Value].Item2;

            CaptureDetailsEntity entity = _capturedDetailsHelper.GetDetails(sessionId.Value, bingResultsCaptureId.Value);
            if (entity == null)
            {
                throw new QueryProbeBackEndException(String.Format("Could not get a record of capture details previously stored for Bing results in session '{0}' with capture ID '{1}'.", sessionId.Value, bingResultsCaptureId.Value));
            }

            // Add or update list of capture details per PBXML ID
            if (entity.AdditionalUrlsCaptureDetails == null)
            {
                perPbxmlIdCaptureDetails = new Dictionary<string, Tuple<string, CaptureDetails>>();
            }
            else
            {
                perPbxmlIdCaptureDetails = JsonConvert.DeserializeObject<Dictionary<string, Tuple<string, CaptureDetails>>>(entity.AdditionalUrlsCaptureDetails);
            }

            perPbxmlIdCaptureDetails.Add(parentCaptureDetails.PbxmlId, new Tuple<string, CaptureDetails>(primaryHash, parentCaptureDetails));
            entity.AdditionalUrlsCaptureDetails = JsonConvert.SerializeObject(perPbxmlIdCaptureDetails, Formatting.None);

            List<string> errors = new List<string>();

            DetailedCallResult tableCallResult = _capturedDetailsHelper.UpdateDetails(entity);
            if (!String.IsNullOrWhiteSpace(tableCallResult.Error))
            {
                errors.Add(String.Format("Error occurred while adding cloned capture details data for URL '{0}' (query '{1}') associated with session '{2}' (capture ID '{3}').  Details:", url.Url, parentEntity.Query, sessionId.Value, bingResultsCaptureId.Value));
                errors.Add(tableCallResult.Error);
            }

            return new CaptureDetailsData()
            {
                CaptureDetails = parentCaptureDetails,
                CaptureId = bingResultsCaptureId.Value,
                Errors = errors,
                SessionId = sessionId.Value,
                UserName = userName.Value
            };
        }

        private Action<BingHttpResponseMessage> GetResponseCustomizer(CaptureDetails captureDetails)
        {
            return new Action<BingHttpResponseMessage>(
                responseMessage =>
                {
                    captureDetails.CaptureTimestamp = responseMessage.CreationTime;
                    captureDetails.PbxmlId = responseMessage.CachedResponseKey;
                    captureDetails.CachedResponseUri = responseMessage.CachedResponseUri.AbsoluteUri;
                    captureDetails.CachedSazUri = responseMessage.CachedSazUri.AbsoluteUri;
                });
        }

        // For restoring results outside of the Bing top N result set, provide a PBXML ID.  Otherwise, the PBXML ID
        // can be null or empty.
        private UrlExtendedData RestoreExtendedDataForUrlInternal(StringParameter pbxmlId, UrlParameter url, StringParameter captureId, StringParameter capturedSessionId)
        {
            if (String.IsNullOrWhiteSpace(pbxmlId.Value) && String.IsNullOrWhiteSpace(url.PrimaryHash))
            {
                throw new QueryProbeBackEndException("To restore extended data for a URL, either a PBXML or the URL's hash must be provided.");
            }

            CaptureDetailsEntity entity = _capturedDetailsHelper.GetDetails(capturedSessionId.Value, captureId.Value);
            if (entity == null)
            {
                throw new QueryProbeBackEndException(String.Format("Could not get a record of capture details previously stored for Bing results in session '{0}' with capture ID '{1}'.", capturedSessionId.Value, captureId.Value));
            }

            string cacheKey = null;

            if (!String.IsNullOrWhiteSpace(pbxmlId.Value))
            {
                Dictionary<string, Tuple<string, CaptureDetails>> perPbxmlIdCaptureDetails;
                try
                {
                    perPbxmlIdCaptureDetails = JsonConvert.DeserializeObject<Dictionary<string, Tuple<string, CaptureDetails>>>(entity.AdditionalUrlsCaptureDetails);
                }
                catch (Exception ex)
                {
                    throw new QueryProbeBackEndException(String.Format("Could not deserialize the list of additional URLs captured in session '{0}' with capture ID '{1}'.  Additional details: {2}", capturedSessionId.Value, captureId.Value, ex), ex);
                }

                if (!perPbxmlIdCaptureDetails.ContainsKey(pbxmlId.Value))
                {
                    throw new QueryProbeBackEndException(String.Format("Could not find a record of captured data for URL hash '{0}' (URL: {1}) in session '{2}' with capture ID '{3}'.", url.PrimaryHash, url.Url, capturedSessionId.Value, captureId.Value));
                }

                if (perPbxmlIdCaptureDetails[pbxmlId.Value].Item1.ToLowerInvariant() != url.PrimaryHash.ToLowerInvariant())
                {
                    throw new QueryProbeBackEndException(String.Format("URL hash '{0}' found in the record of captured data for URL: {1} in session '{2}' with capture ID '{3}' does not match the hash of the URL ('{4}').", perPbxmlIdCaptureDetails[pbxmlId.Value].Item1, url.Url, capturedSessionId.Value, captureId.Value, url.PrimaryHash));
                }

                if (perPbxmlIdCaptureDetails[pbxmlId.Value].Item2.PbxmlId.ToLowerInvariant() != pbxmlId.Value.ToLowerInvariant())
                {
                    throw new QueryProbeBackEndException(String.Format("PBXML ID '{0}' found in the record of captured data for URL hash '{1}' (URL: {2}) in session '{3}' with capture ID '{4}' does not match the PBXML ID '{5}'.", pbxmlId.Value, url.PrimaryHash, url.Url, capturedSessionId.Value, captureId.Value, perPbxmlIdCaptureDetails[pbxmlId.Value].Item2.PbxmlId));
                }

                cacheKey = pbxmlId.Value;
            }
            else
            {
                Dictionary<string, CaptureDetails> perUrlHashCaptureDetails;
                try
                {
                    perUrlHashCaptureDetails = JsonConvert.DeserializeObject<Dictionary<string, CaptureDetails>>(entity.BingTopNCaptureDetails);
                }
                catch (Exception ex)
                {
                    throw new QueryProbeBackEndException(String.Format("Could not deserialize the list of Bing top N results captured in session '{0}' with capture ID '{1}'.  Additional details: {2}", capturedSessionId.Value, captureId.Value, ex), ex);
                }

                if (!perUrlHashCaptureDetails.ContainsKey(url.PrimaryHash))
                {
                    throw new QueryProbeBackEndException(String.Format("Could not find a record of Bing top N result with URL hash '{0}' (URL: {1}) in session '{2}' with capture ID '{3}'.", url.PrimaryHash, url.Url, capturedSessionId.Value, captureId.Value));
                }

                cacheKey = perUrlHashCaptureDetails[url.PrimaryHash].PbxmlId;
            }

            UrlExtendedData extendedData = new UrlExtendedData();

            using (DistributedCacheGetResult cacheResult = _capturedDataCache.GetAsync(cacheKey).Result)
            using (Stream resultContent = cacheResult.Data)
            {
                if (cacheResult.HitStatus != DistributedCacheHitStatus.Hit)
                {
                    throw new QueryProbeBackEndException(string.Format("Could not retrieve data with key {0} from cache. Please contact qpbel if this persists.", cacheKey));
                }

                Action<PbXmlParser, ResultSet> resultsCustomizer = GetExtendedDataResultsCustomizer(url, extendedData, true);
                BingQueryBuilder builder = GetConfiguredQueryBuilder(new QueryParameter() { QueryText = entity.Query }, null);
                ProcessResults(resultContent, resultsCustomizer, null, builder);
            }

            return extendedData;
        }

        private void FetchAndProcessResultsByPbxml(QueryParameter query, Action<BingQueryBuilder> queryUpdater, Action<PbXmlParser, ResultSet> resultsCustomizer, Stream contentStream = null, String cachedResponseUri = null, String cachedSazUri = null)
        {
            string index = this._setting.QueryRouting.Index;
            string answersBedConstraint = this._setting.QueryRouting.AnswersBedConstraint;
            BingQueryBuilder builder = GetConfiguredQueryBuilder(query, queryUpdater);
            IDistributedCache cache = _distributedCache;

            using (var metricEvent = new BingDataSourceEvent(this.CurrentRequestResource))
            {
                metricEvent.Query = query.QueryText;
                metricEvent.Market = this._setting.Market.MarketLower;
                metricEvent.AnswersBedConstraint = answersBedConstraint;
                metricEvent.Index = index;
                metricEvent.SearchBedName = this._setting.QueryRouting.SearchBedName;
                metricEvent.UserAugmentation = this._setting.Augmentations.UserAugmentation;
                metricEvent.VariantConstraint = this._setting.Augmentations.VariantConstraint;
                metricEvent.IsPermalink = true;
                metricEvent.IsIndexSwitch = this._setting.QueryRouting.IsIndexSwitch;
                metricEvent.ResetStopwatch();

                using (BingRequestHelperWithCaching requestHelper = _setting.QueryRouting.GetBingRequestHelperWithCaching(builder, cache, KeyCallback))
                {
                    var exceptionMessages = new StringBuilder();
                    WebRequestVariants webRequestVariants = requestHelper.GetRequestVariantsWithCaching(cachedResponseUri, cachedSazUri);
                    try
                    {
                        metricEvent.CalculateXapRequestLatency();
                        if (resultsCustomizer != null)
                        {
                            ProcessResults(
                                contentStream,
                                resultsCustomizer,
                                webRequestVariants,
                                builder);
                        }

                        metricEvent.CalculateResponseParseLatency(false);
                        metricEvent.ResponseLengthInBytes = contentStream?.Length ?? 0;
                        metricEvent.Finish();
                    }
                    catch (RemoteDataException ex)
                    {
                        exceptionMessages.AppendLine(ex.Message);
                        ex.RequestVariants = webRequestVariants;
                        throw;
                    }
                    catch (RemoteSystemException ex)
                    {
                        exceptionMessages.AppendLine(ex.Message);
                        ex.RequestVariants = webRequestVariants;
                        throw;
                    }
                    catch (Exception ex)
                    {
                        exceptionMessages.AppendLine(ex.Message);
                        RemoteDataException remoteDataException = new RemoteDataException(
                            "Exception getting Bing data, see error details",
                            string.Format("{0}: {1}", ex.GetType(), ex.Message),
                            ex);
                        remoteDataException.RequestVariants = webRequestVariants;
                        throw remoteDataException;
                    }
                    finally
                    {
                        metricEvent.RequestUri = webRequestVariants.PrimaryRecord.RequestUri;
                        metricEvent.CachedResponseUri = webRequestVariants.PrimaryRecord.CachedResponseUri;
                        metricEvent.ExceptionInfo = exceptionMessages.ToString();
                        metricEvent.Finish(true);
                    }
                }
            }
        }

        private void FetchAndProcessResults(QueryParameter query, Action<BingQueryBuilder> queryUpdater, Action<PbXmlParser, ResultSet> resultsCustomizer, Action<BingHttpResponseMessage> responseCustomizer, bool captureData)
        {
            string index = this._setting.QueryRouting.Index;
            string answersBedConstraint = this._setting.QueryRouting.AnswersBedConstraint;
            if (captureData && responseCustomizer == null)
            {
                string exceptionMessage = "For capturing the results of a query request a response customization callback must be provided.";
                throw new QueryProbeBackEndException(exceptionMessage, new ArgumentNullException("responseCustomizer", exceptionMessage));
            }

            BingQueryBuilder builder = GetConfiguredQueryBuilder(query, queryUpdater);

            IDistributedCache cache = captureData ? _capturedDataCache : _distributedCache;

            using (var metricEvent = new BingDataSourceEvent(this.CurrentRequestResource))
            {
                metricEvent.Query = query.QueryText;
                metricEvent.Market = this._setting.Market.MarketLower;
                metricEvent.AnswersBedConstraint = answersBedConstraint;
                metricEvent.Index = index;
                metricEvent.SearchBedName = this._setting.QueryRouting.SearchBedName;
                metricEvent.UserAugmentation = this._setting.Augmentations.UserAugmentation;
                metricEvent.VariantConstraint = this._setting.Augmentations.VariantConstraint;
                metricEvent.IsIndexSwitch = this._setting.QueryRouting.IsIndexSwitch;
                metricEvent.ResetStopwatch();

                using (BingRequestHelperWithCaching requestHelper = _setting.QueryRouting.GetBingRequestHelperWithCaching(builder, cache, KeyCallback))
                {
                    var exceptionMessages = new StringBuilder();
                    BingHttpResponseMessage responseMessage = null;
                    try
                    {
                        responseMessage = requestHelper.GetResponseMessage();
                        metricEvent.CalculateXapRequestLatency();

                        if (captureData)
                        {
                            responseCustomizer(responseMessage);
                        }

                        var contentStream = responseMessage?.GetContentXmlAsStream();
                        if (resultsCustomizer != null)
                        {
                            ProcessResults(
                                contentStream,
                                resultsCustomizer,
                                requestHelper.GetRequestVariantsWithCaching(responseMessage),
                                builder);
                        }

                        metricEvent.CalculateResponseParseLatency(false);
                        metricEvent.ResponseLengthInBytes = contentStream?.Length ?? 0;
                        metricEvent.Finish();
                    }
                    catch (RemoteDataException ex)
                    {
                        exceptionMessages.AppendLine(ex.Message);
                        ex.RequestVariants = requestHelper.GetRequestVariantsWithCaching(responseMessage);
                        throw;
                    }
                    catch (RemoteSystemException ex)
                    {
                        exceptionMessages.AppendLine(ex.Message);
                        ex.RequestVariants = requestHelper.GetRequestVariantsWithCaching(responseMessage);
                        throw;
                    }
                    catch (Exception ex)
                    {
                        exceptionMessages.AppendLine(ex.Message);
                        RemoteDataException remoteDataException = new RemoteDataException(
                            "Exception getting Bing data, see error details",
                            string.Format("{0}: {1}", ex.GetType(), ex.Message),
                            ex);

                        remoteDataException.RequestVariants = requestHelper.GetRequestVariantsWithCaching(responseMessage);
                        throw remoteDataException;
                    }
                    finally
                    {
                        if (responseMessage != null)
                        {
                            responseMessage.Dispose();
                        }
                        var requestVariants = requestHelper.GetRequestVariantsWithCaching(responseMessage);
                        if (requestVariants != null)
                        {
                            metricEvent.RequestUri = requestVariants.PrimaryRecord.RequestUri;
                            metricEvent.CachedResponseUri = requestVariants.PrimaryRecord.CachedResponseUri;
                            metricEvent.ExceptionInfo = exceptionMessages.ToString();
                        }
                        // try to regard this as a failure event, which will be ignored if metricEvent.Finish() is executed before
                        metricEvent.Finish(true);
                    }
                }
            }
        }

        private void ProcessResults(Stream resultsContent, Action<PbXmlParser, ResultSet> resultsCustomizer, WebRequestVariants requestVariants, BingQueryBuilder builder = null)
        {
            try
            {
                PbXmlParser dataParser = new PbXmlParser(resultsContent, _setting.QueryRouting.Index, _classifierParser, _indexUtility, _setting);

                ResultSet results = _resultSetFactory.CreateFromKif(dataParser);
                resultsCustomizer(dataParser, results);

                if (requestVariants != null)
                {
                    results.SetRequestVariants(requestVariants);
                    SetResultsBingLinkDetails(results);
                }

                if (builder != null && !string.IsNullOrWhiteSpace(builder.UserAugmentation) && (builder.UserAugmentation.Contains("gini$granker:te") || builder.UserAugmentation.Contains("xini$xranker:te")))
                {
                    results.IsNewRanker = true;
                    results.AugmentationWarningMessage = "Noticed that you used a customized ranker, and it might not be ready on the server machines if it is the first time used. If your ranker is not loaded properly, please try several times to wait for the machines finishing loading your ranker.";
                }
                else
                {
                    results.IsNewRanker = false;
                    results.AugmentationWarningMessage = string.Empty;
                }
                results.IsIndexSwitch = _setting.QueryRouting.IsIndexSwitch;
                results.IsProductionQuery = _setting.QueryRouting.IsProductionConfiguration;
                results.QueryFlags[QueryFlagNames.SuperFreshQuery] = results.IsSuperFresh(_setting.Market.MarketLower, _guardSuperfreshTreeMarkets.Value);
            }
            catch (RemoteDataException rde)
            {
                rde.IsIndexSwitch = _setting.QueryRouting.IsIndexSwitch;
                throw rde;
            }
            catch (Exception e)
            {
                RemoteDataException remoteDataException = new RemoteDataException(
                            "Exception getting Bing data, see error details",
                            string.Format("{0}: {1}", e.GetType(), e.Message),
                            e);
                remoteDataException.RequestVariants = requestVariants;
                remoteDataException.IsIndexSwitch = _setting.QueryRouting.IsIndexSwitch;
                throw remoteDataException;
            }
        }


        private void ProcessPbxmlResults(StringParameter path, Action<BingQueryBuilder> queryUpdater, Action<PbXmlParser, ResultSet> resultsCustomizer, QueryParameter query = null)
        {
            var exceptionMessages = new StringBuilder();
            HttpWebResponse twitpicResponse = null;
            try
            {
                HttpWebRequest request = (HttpWebRequest)WebRequest.Create(path.Value);
                twitpicResponse = (HttpWebResponse)request.GetResponse();

                var contentStream = twitpicResponse?.GetResponseStream();
                if (resultsCustomizer != null)
                {
                    BingQueryBuilder builder = GetConfiguredQueryBuilder(query, queryUpdater);
                    ProcessResults(
                        contentStream,
                        resultsCustomizer,
                        null,
                        builder);
                }

            }
            catch (RemoteDataException ex)
            {
                exceptionMessages.AppendLine(ex.Message);
                throw;
            }
            catch (RemoteSystemException ex)
            {
                exceptionMessages.AppendLine(ex.Message);
                throw;
            }
            catch (Exception ex)
            {
                exceptionMessages.AppendLine(ex.Message);
                RemoteDataException remoteDataException = new RemoteDataException(
                    "Exception getting Bing data, see error details",
                    string.Format("{0}: {1}", ex.GetType(), ex.Message),
                    ex);

                throw remoteDataException;
            }
            finally
            {
                if (twitpicResponse != null)
                {
                    twitpicResponse.Dispose();
                }
            }
        }

        private void SetResultsBingLinkDetails(ResultSet results)
        {
            if (results.RequestVariants != null)
            {

                WebRequestRecord bingEdgeRecord = results.RequestVariants.VariantRecords[BingRequestHelperQueryMode.BingEdge.ToString()];
                if (bingEdgeRecord != null)
                {
                    results.BingLink = bingEdgeRecord.RequestUri.Replace("format=pbxml&", "");
                    results.BingLinkWarnings = bingEdgeRecord.CompatibilityMessages;
                }
            }
        }

        private BingQueryBuilder GetConfiguredQueryBuilder(QueryParameter query, Action<BingQueryBuilder> queryUpdater)
        {
            BingQueryBuilder builder = BingQueryBuilder.GetBaseWebBuilder();
            AugmentationsMonitor();
            _setting.UpdateBingQueryBuilder(builder);
            UpdateBingQueryBuilderWithOptions(builder, true, false, false);
            if (queryUpdater != null)
            {
                queryUpdater(builder);
            }
            query.UpdateBingQueryBuilder(builder);
            return builder;
        }

        private void AugmentationsMonitor()
        {
            if (_setting.Augmentations != null)
            {
                var augs = _setting.Augmentations.UserAugmentation;
                if (augs != null && augs != string.Empty)
                {
                    using (var e = new UserAugmentationMonitoerEvent(augs))
                    {
                        e.Finish();
                    }
                }
            }
        }

        private string CreateNewCaptureId()
        {
            return Guid.NewGuid().ToString();
        }

        private void TestIndex()
        {
            try
            {
                InjectionMonitor injectionMonitor = new InjectionMonitor("Web-Prod", "B02", new DateTime(2015, 04, 14, 19, 51, 15));
                string expectedSplitIndexStr = "";
                string l1ExpectedIndexStr = "";
                string l2ExpectedIndexStr = "";
                MonitorStatus monitorStatus = injectionMonitor.MonitorInjectionStatus(out expectedSplitIndexStr, out l1ExpectedIndexStr, out l2ExpectedIndexStr);
                if (monitorStatus.ToString() == "success")
                {
                    Logger.Info("success index switch");
                }
                else
                {
                    Logger.Info("failed index switch");
                }
            }
            catch (Exception ex)
            {
                Logger.Info("injectionMonitor failed." + ex.Message);
            }
        }
    }
}