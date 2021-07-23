var db = require("../config.js");
var async = require('async');
var mongoose = require('mongoose');
var Schema = mongoose.Schema;

var cors = require('cors');
const Axios = require("axios");

var operationsCompleted = 0;var dataarr = [];var user=[];

exports.port_folio_data = (req, res) => {

    try {
       
        var resdata = ""; 
        pipeline1 = [  //trans_cams
            { $match: { PAN: req.body.pan, INV_NAME: { $regex: `^${req.body.name}.*`, $options: 'i' } } },
            { $group: { _id: { INV_NAME: "$INV_NAME", PAN: "$PAN", SCHEME: "$SCHEME", FOLIO_NO: "$FOLIO_NO" } } },
            { $project: { _id: 0, NAME: "$_id.INV_NAME", PAN: "$_id.PAN", SCHEME: "$_id.SCHEME", FOLIO: "$_id.FOLIO_NO", RTA: "CAMS" } }
        ]
        pipeline2 = [  //trans_karvy
            { $match: { PAN1: req.body.pan, INVNAME: { $regex: `^${req.body.name}.*`, $options: 'i' } } },
            { $group: { _id: { INVNAME: "$INVNAME", PAN1: "$PAN1", FUNDDESC: "$FUNDDESC", TD_ACNO: "$TD_ACNO" } } },
            { $project: { _id: 0, NAME: "$_id.INVNAME", PAN: "$_id.PAN1", SCHEME: "$_id.FUNDDESC", FOLIO: "$_id.TD_ACNO", RTA: "KARVY" } }
        ]
        pipeline3 = [   //trans_franklin
            { $match: { IT_PAN_NO1: req.body.pan, INVESTOR_2: { $regex: `^${req.body.name}.*`, $options: 'i' } } },
            { $group: { _id: { INVESTOR_2: "$INVESTOR_2", IT_PAN_NO1: "$IT_PAN_NO1", SCHEME_NA1: "$SCHEME_NA1", FOLIO_NO: "$FOLIO_NO" } } },
            { $project: { _id: 0, NAME: "$_id.INVESTOR_2", PAN: "$_id.IT_PAN_NO1", SCHEME: "$_id.SCHEME_NA1", FOLIO: "$_id.FOLIO_NO", RTA: "FRANKLIN" } }
        ]

            db.collection('trans_karvy').aggregate(pipeline2, (err, results) => {
               // console.log(results.length)
                    if ( results.length != 0 ) {
                        if (err) {
                            res.send(err);
                        }
                        else {
                         //   for (i = 0; i < results.length; i++) {
                            async.eachSeries(results,function(data,callback){ // It will be executed one by one
                          //     console.log(data.SCHEME)
                                //Here it will be wait query execute. It will work like synchronous
                            pipeline5 = [  //trans_karvy    "TD_TRTYPE":{$not: /^SINR.*/
                                        { $match: { FUNDDESC: data.SCHEME, PAN1: data.PAN, TD_ACNO: data.FOLIO, INVNAME: { $regex: `^${data.NAME}.*`, $options: 'i' } } },
                                        { $group: { _id: { TD_ACNO: "$TD_ACNO", FUNDDESC: "$FUNDDESC", TD_NAV: "$TD_NAV", TD_TRTYPE: "$TD_TRTYPE", NAVDATE: "$NAVDATE", SCHEMEISIN: "$SCHEMEISIN" }, TD_UNITS: { $sum: "$TD_UNITS" }, TD_AMT: { $sum: "$TD_AMT" } } },
                                        { $lookup: { from: 'cams_nav', localField: '_id.SCHEMEISIN', foreignField: 'ISINDivPayoutISINGrowth', as: 'nav' } },
                                        { $unwind: "$nav" },
                                        { $project: { _id: 0, FOLIO: "$_id.TD_ACNO", SCHEME: "$_id.FUNDDESC", TD_NAV: "$_id.TD_NAV", NATURE: "$_id.TD_TRTYPE", TD_TRDT: { $dateToString: { format: "%d-%m-%Y", date: "$_id.NAVDATE" } }, ISIN: "$_id.SCHEMEISIN", cnav: "$nav.NetAssetValue", navdate: "$nav.Date", UNITS: { $sum: "$TD_UNITS" }, AMOUNT: { $sum: "$TD_AMT" } } },
                                        { $sort: { TD_TRDT: -1 } }
                                    ]
                                    db.collection('trans_karvy').aggregate(pipeline5, (err, data5) => {
                                     //   console.log(data5)
                                    
                                        dataarr.push(data5);
                                        callback();
              
        
                                    });
                                });
                         
                          
                         //   console.log(datacon.length,"--=--",i)
                       
                        //    }
                            console.log("hello=",dataarr);
                        }
                    }
        });
    } catch (err) {
        console.log(err)
    }
}

 