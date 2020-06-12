/// <reference path="..\..\..\Classes\ViewInstance\Base\BaseViewInstance.ts" />

module QueryProbe {
    // This class is designed to quickly get Query Probe partners started with developing
	// a plug-in.
	// It works with SampleDataSource to implement a complete end-to-end scenario.
    export class RankBERTDebug extends BaseViewInstance {
		private _$displayDatasourceDataDiv: JQuery;
		private _$displayCartDataDiv: JQuery;
		private _echo: LogicalResource;
        private _queryBox: LogicalResourceBox;
        // private _$testDLIS: JQuery;
        private _$DLISresponse: JQuery;
        private _$JsonRequestTextArea: JQuery;
        private _dlisResource: LogicalResource;
        private _debugTemplate: HandlebarsTemplate<any>;

        constructor(viewEntry: ViewEntry) {
			super(viewEntry); 
            
            this._$displayDatasourceDataDiv = this.$containingElement.find(".RankBERTDebug-display-datasource-data-div");
            this._$displayCartDataDiv = this.$containingElement.find(".RankBERTDebug-display-cart-data-div");
            this._dlisResource = this.createLogicalResource("DLISData");
            // this._$testDLIS = this.$containingElement.find(".submit_dlis_request");
            this._$DLISresponse = this.$containingElement.find(".testdlis-response-div");
            this._debugTemplate = new HandlebarsTemplate<any>("DebugTemplate", this.$containingElement);
            this._$JsonRequestTextArea = this.$containingElement.find(".Json-of-request");
          

            var ce = this;
            $(document).ready(function () {
             

                $("#generate-Json").click(function () {
                    var queries = [];
                    var snippets = [];
                    var urls = [];
                    var titles = [];
                    var markets = [];

                    $("input[name='Query']").each(function () {
                        queries.push($(this).val());
                    })
                    $("input[name='Snippet']").each(function () {
                        snippets.push($(this).val());
                    })
                    $("input[name='URL']").each(function () {
                        urls.push($(this).val());
                    })
                    $("input[name='Title']").each(function () {
                        titles.push($(this).val());
                    })
                    $("input[name='Market']").each(function () {
                        markets.push($(this).val());
                    })

                   

                    var batch = [];

                    queries.forEach(function (v, i) {
                        batch.push([queries[i], snippets[i], urls[i], titles[i], markets[i]])
                    })
                   
                    var jsonObject = {
                        Task: "Web:Score_mt_v1",
                        Batch: batch
                    }

                    var jsonString = JSON.stringify(jsonObject);
                    $('#Json-of-request').val(jsonString);
                    // this._$JsonRequestTextArea.val(jsonString);
                    console.log(jsonString);

                    $("#submit_dlis_request").click(function () {
                        var result = ce.getDLIS($("#Json-of-request").val(), $("#endpointInput").val());
                        console.log(result)
                    })
                   
                });


            });

            this.$containingElement.find(".accordion").accordion();


            var debugHtml: string = "";
            var CartItems: CartItem[] = this.cart.getItems(this.verticalName, true);          
            for (var i: number = 0; i < CartItems.length; i++) {
                var item = CartItems[i];               
                var url = item.url; 
                var title = item.data.ResultData.CaptionDetails.TitleCandidates[0].Buffer;
                var snippet = item.data.ResultData.CaptionDetails.SnippetCandidates[0].Buffer;
                var query = item.data.ResultSet.RawQuery; 
                var market = item.data.ResultData.Language.concat("-", item.data.ResultData.Country);
      
                debugHtml += this._debugTemplate.render({
                    Query: query,
                    Snippet: snippet,
                    URL: url,
                    Title: title,
                    Market: market
                });
            }

            var $generateJsonButton: JQuery = this.$containingElement.find("#generate-Json");
            $generateJsonButton.before(debugHtml);
      
        }

        public getDLIS(input: string, endpointInput: string) {

            var dlisInput: BaseParameter = this.parameterFactory.createDefaultFromName("StringParameter");
            dlisInput.value = input;

            var endpoint: BaseParameter = this.parameterFactory.createDefaultFromName("StringParameter");
            endpoint.value = endpointInput;
          
            this._dlisResource.fetchData(
                data => this.showDLISData(data),
                error => this.renderError(error),
                dlisInput,
                endpoint
            );
        }
        private showDLISData(d: any) {
            this._$DLISresponse.html(d.Value);
        }

        public onCartChange() {
            this._$displayCartDataDiv.html("<p>Number of items in the Analysis Set: " + this.cart.getItemsCount()  + "</p>");
        }

        public onShow() {
            
        }

        private getResults(context: QueryContext) {
            
        }    

        private renderResults(data: any) {
            this._$displayDatasourceDataDiv.html("<p>RankBERTDebug data source successfully replied with:" + data.Value + "</p>");
            this.notificationArea.empty();
        }

        private renderError(error: IAjaxError) {
            this.notificationArea.showError("Contacting the RankBERTDebug data source failed.", error);
        }	
	}
}
