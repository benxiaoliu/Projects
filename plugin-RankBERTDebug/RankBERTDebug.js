/// <reference path="..\..\..\Classes\ViewInstance\Base\BaseViewInstance.ts" />
var __extends = (this && this.__extends) || (function () {
    var extendStatics = function (d, b) {
        extendStatics = Object.setPrototypeOf ||
            ({ __proto__: [] } instanceof Array && function (d, b) { d.__proto__ = b; }) ||
            function (d, b) { for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p]; };
        return extendStatics(d, b);
    };
    return function (d, b) {
        extendStatics(d, b);
        function __() { this.constructor = d; }
        d.prototype = b === null ? Object.create(b) : (__.prototype = b.prototype, new __());
    };
})();
var QueryProbe;
(function (QueryProbe) {
    // This class is designed to quickly get Query Probe partners started with developing
    // a plug-in.
    // It works with SampleDataSource to implement a complete end-to-end scenario.
    var RankBERTDebug = /** @class */ (function (_super) {
        __extends(RankBERTDebug, _super);
        function RankBERTDebug(viewEntry) {
            var _this = _super.call(this, viewEntry) || this;
            _this._$displayDatasourceDataDiv = _this.$containingElement.find(".RankBERTDebug-display-datasource-data-div");
            _this._$displayCartDataDiv = _this.$containingElement.find(".RankBERTDebug-display-cart-data-div");
            _this._dlisResource = _this.createLogicalResource("DLISData");
            // this._$testDLIS = this.$containingElement.find(".submit_dlis_request");
            _this._$DLISresponse = _this.$containingElement.find(".testdlis-response-div");
            _this._debugTemplate = new QueryProbe.HandlebarsTemplate("DebugTemplate", _this.$containingElement);
            _this._$JsonRequestTextArea = _this.$containingElement.find(".Json-of-request");
            var ce = _this;
            $(document).ready(function () {
                $("#generate-Json").click(function () {
                    var queries = [];
                    var snippets = [];
                    var urls = [];
                    var titles = [];
                    var markets = [];
                    $("input[name='Query']").each(function () {
                        queries.push($(this).val());
                    });
                    $("input[name='Snippet']").each(function () {
                        snippets.push($(this).val());
                    });
                    $("input[name='URL']").each(function () {
                        urls.push($(this).val());
                    });
                    $("input[name='Title']").each(function () {
                        titles.push($(this).val());
                    });
                    $("input[name='Market']").each(function () {
                        markets.push($(this).val());
                    });
                    var batch = [];
                    queries.forEach(function (v, i) {
                        batch.push([queries[i], snippets[i], urls[i], titles[i], markets[i]]);
                    });
                    var jsonObject = {
                        Task: "Web:Score_mt_v1",
                        Batch: batch
                    };
                    var jsonString = JSON.stringify(jsonObject);
                    $('#Json-of-request').val(jsonString);
                    // this._$JsonRequestTextArea.val(jsonString);
                    console.log(jsonString);
                    $("#submit_dlis_request").click(function () {
                        var result = ce.getDLIS($("#Json-of-request").val(), $("#endpointInput").val());
                        console.log(result);
                    });
                });
            });
            _this.$containingElement.find(".accordion").accordion();
            var debugHtml = "";
            var CartItems = _this.cart.getItems(_this.verticalName, true);
            for (var i = 0; i < CartItems.length; i++) {
                var item = CartItems[i];
                var url = item.url;
                var title = item.data.ResultData.CaptionDetails.TitleCandidates[0].Buffer;
                var snippet = item.data.ResultData.CaptionDetails.SnippetCandidates[0].Buffer;
                var query = item.data.ResultSet.RawQuery;
                var market = item.data.ResultData.Language.concat("-", item.data.ResultData.Country);
                debugHtml += _this._debugTemplate.render({
                    Query: query,
                    Snippet: snippet,
                    URL: url,
                    Title: title,
                    Market: market
                });
            }
            var $generateJsonButton = _this.$containingElement.find("#generate-Json");
            $generateJsonButton.before(debugHtml);
            return _this;
        }
        RankBERTDebug.prototype.getDLIS = function (input, endpointInput) {
            var _this = this;
            var dlisInput = this.parameterFactory.createDefaultFromName("StringParameter");
            dlisInput.value = input;
            var endpoint = this.parameterFactory.createDefaultFromName("StringParameter");
            endpoint.value = endpointInput;
            this._dlisResource.fetchData(function (data) { return _this.showDLISData(data); }, function (error) { return _this.renderError(error); }, dlisInput, endpoint);
        };
        RankBERTDebug.prototype.showDLISData = function (d) {
            this._$DLISresponse.html(d.Value);
        };
        RankBERTDebug.prototype.onCartChange = function () {
            this._$displayCartDataDiv.html("<p>Number of items in the Analysis Set: " + this.cart.getItemsCount() + "</p>");
        };
        RankBERTDebug.prototype.onShow = function () {
        };
        RankBERTDebug.prototype.getResults = function (context) {
        };
        RankBERTDebug.prototype.renderResults = function (data) {
            this._$displayDatasourceDataDiv.html("<p>RankBERTDebug data source successfully replied with:" + data.Value + "</p>");
            this.notificationArea.empty();
        };
        RankBERTDebug.prototype.renderError = function (error) {
            this.notificationArea.showError("Contacting the RankBERTDebug data source failed.", error);
        };
        return RankBERTDebug;
    }(QueryProbe.BaseViewInstance));
    QueryProbe.RankBERTDebug = RankBERTDebug;
})(QueryProbe || (QueryProbe = {}));
//# sourceMappingURL=RankBERTDebug.js.map