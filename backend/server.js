import express from 'express';
import dotenv from 'dotenv';
 import config from './config.js';
 import mongoose from 'mongoose';
 import path from 'path';
import bodyParser from 'body-parser';
var Schema = mongoose.Schema;
import Axios from 'axios'
 import moment from 'moment';
dotenv.config();

const mongodbUrl= config.MONGODB_URL;


//moment.suppressDeprecationWarnings = true;

console.log("I am mongodbUrflfffffffffffff", mongodbUrl);


mongoose.connect(mongodbUrl, {
	useNewUrlParser:true,
	useUnifiedTopology: true,
	//useCreateIndex:true,
	autoIndex: false, // Don't build indexes
	//useMongoClient: true,
	reconnectTries: Number.MAX_VALUE, // Never stop trying to reconnect
	reconnectInterval: 500, // Reconnect every 500ms
	poolSize: 10, // Maintain up to 10 socket connections
        // If not connected, return errors immediately rather than waiting for reconnect
	bufferMaxEntries: 0
}).catch(error => console.log(error.reason));

const app=express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

const __dirname = path.resolve();
app.use('/uploads', express.static(path.join(__dirname, '/uploads')));



const navcams = new Schema({
    SchemeCode: { type: String },
    ISINDivPayoutISINGrowth: { type: String },
    ISINDivReinvestment: { type: String },
    SchemeName: { type: String ,required: true},
    NetAssetValue: { type: Number },
    Date: { type: Date },
}, { versionKey: false });


const productdata = new Schema({
    id: { type: String },
    product_master_diffgr_id: { type: String },
    msdata_rowOrder: { type: String },
    AMC_CODE: { type: String },
    PRODUCT_CODE: { type: String },
    PRODUCT_LONG_NAME: { type: String },
    SYSTEMATIC_FREQUENCIES:{ type: String },
    SIP_DATES: { type: String },
    STP_DATES: { type: String },
    SWP_DATES: { type: String },
    PURCHASE_ALLOWED: { type: String },
    SWITCH_ALLOWED: { type: String },
    REDEMPTION_ALLOWED: { type: String },
    SIP_ALLOWED: { type: String },
    STP_ALLOWED: { type: String },
    SWP_ALLOWED: { type: String },
    REINVEST_TAG: { type: String },
    PRODUCT_CATEGORY: { type: String },
    ISIN: { type: String },
    LAST_MODIFIED_DATE: { type: String },
    ACTIVE_FLAG: { type: String },
    ASSET_CLASS: { type: String },
    SUB_FUND_CODE: { type: String },
    PLAN_TYPE: { type: String },
    INSURANCE_ENABLED: { type: String },
    RTACODE: { type: String },
    NFO_ENABLED: { type: String },
    NFO_CLOSE_DATE: { type: String },
    NFO_SIP_EFFECTIVE_DATE: { type: String },
    ALLOW_FREEDOM_SIP: { type: String },
    ALLOW_FREEDOM_SWP: { type: String },
    ALLOW_DONOR: { type: String },
    ALLOW_PAUSE_SIP: { type: String },
    ALLOW_PAUSE_SIP_FREQ: { type: String },
    PAUSE_SIP_MIN_MONTH: { type: String },
    PAUSE_SIP_MAX_MONTH: { type: String },
    PAUSE_SIP_GAP_PERIOD: { type: String },
}, { versionKey: false });

const foliocams = new Schema({
    AMC_CODE: { type: String },
    FOLIOCHK: { type: String },
    INV_NAME: { type: String },
    SCH_NAME: { type: String },
    JNT_NAME1: { type: String },
    JNT_NAME2: { type: String },
    HOLDING_NATURE: { type: String },
    PAN_NO: { type: String },
    JOINT1_PAN: { type: String },
    BANK_NAME: { type: String },
    AC_NO: { type: String },
    NOM_NAME: { type: String },
    NOM2_NAME: { type: String },
    NOM3_NAME: { type: String },
    IFSC_CODE: { type: String },
    PRODUCT: {type: String},
}, { versionKey: false });


const foliokarvy = new Schema({
    FUNDDESC: { type: String },
    ACNO: { type: String },
    INVNAME: { type: String },
    JTNAME1: { type: String },
    JTNAME2: { type: String },
    BNKACNO: { type: String },
    BNAME: { type: String },
    PANGNO: { type: String },
    NOMINEE: { type: String },
    PRCODE: { type: String},
    FUND: { type : String},
    BNKACTYPE : { type: String},
}, { versionKey: false });

const foliofranklin = new Schema({
    BANK_CODE: { type: String },
    IFSC_CODE: { type: String },
    NEFT_CODE: { type: String },
    NOMINEE1: { type: String },
    FOLIO_NO: { type: String },
    INV_NAME: { type: String },
    JOINT_NAM1: { type: String },
    ADDRESS1: { type: String },
    BANK_NAME: { type: String },
    ACCNT_NO: { type: String },
    D_BIRTH: { type: String },
    F_NAME: { type: String },
    PHONE_RES: { type: String },
    PANNO1: { type: String },
    COMP_CODE : { type: String },
    AC_TYPE : { type: String },
    KYC_ID :{ type: String },
    HOLDING_T6 : { type: String },
    PBANK_NAME : { type: String },
    PERSONAL_9 : { type: String },
}, { versionKey: false });

const transcams = new Schema({
    AMC_CODE: { type: String },
    FOLIO_NO: { type: String },
    PRODCODE: { type: String },
    SCHEME: { type: String },
    INV_NAME: { type: String }, 
    TRXNNO: {type: String },
    TRADDATE: { type: Date },   
    UNITS: { type: Number },
    AMOUNT: { type: Number },
    TRXN_NATUR: { type: String },
    SCHEME_TYP: { type: String },
    PAN: { type: String },
    TRXN_TYPE_: { type: String },   
    AC_NO: { type: String } ,
    BANK_NAME: { type: String } ,
}, { versionKey: false });

const transkarvy = new Schema({
    FMCODE: { type: String },
    TD_ACNO: { type: String },
    FUNDDESC: { type: String },
    TD_TRNO: { type: String },
    SMCODE: { type: String },
    INVNAME: { type: String },
    TD_TRDT: { type: Date },
    TD_POP: { type: String },
    TD_AMT: { type: Number },
    TD_APPNO: { type: String },
    UNQNO: { type: String },
    TD_NAV: { type: String },
    IHNO: { type: String },
    BRANCHCODE: { type: String },
    TRDESC: { type: String },
    PAN1: { type: String },
    ASSETTYPE:{ type: String},
    TD_UNITS: { type: Number},
    SCHEMEISIN:{ type: String},
    TD_FUND:{ type: String},
    TD_TRTYPE: { type : String},
    NEWUNQNO: {type : String},
}, { versionKey: false });


const transfranklin = new Schema({
    COMP_CODE: { type: String },
    SCHEME_CO0: { type: String },
    SCHEME_NA1: { type: String },
    FOLIO_NO: { type: String },
    TRXN_TYPE: { type: String },
    TRXN_NO: { type: String },
    INVESTOR_2: { type: String },
    TRXN_DATE: { type: Date},
    NAV: { type: Number },
    POP: { type: String },
    UNITS: { type: Number },
    AMOUNT: { type: Number },
    ADDRESS1: { type: String },
    IT_PAN_NO1: { type: String },
    ISIN: { type: String },
    JOINT_NAM1: { type: String },
    JOINT_NAM2: { type: String },
    PLAN_TYPE: { type: String },
    NOMINEE1: { type: String },
    ACCOUNT_16: { type: String },
    PBANK_NAME: { type: String },
    PERSONAL23: { type: String},
}, { versionKey: false });


  var transc = mongoose.model('trans_cams', transcams, 'trans_cams');   
  var transk = mongoose.model('trans_karvy', transkarvy, 'trans_karvy'); 
  var transf = mongoose.model('trans_franklin', transfranklin, 'trans_franklin');  
  var folioc = mongoose.model('folio_cams', foliocams, 'folio_cams'); 
  var foliok = mongoose.model('folio_karvy', foliokarvy, 'folio_karvy');  
  var foliof = mongoose.model('folio_franklin', foliofranklin, 'folio_franklin');
  var camsn = mongoose.model('cams_nav', navcams, 'cams_nav');  
  var data="";var karvydata="";var camsdata="";var frankdata="";var datacon="";
var i=0;var resdata="";

app.post("/api/getfoliodetail", function (req, res) {     
	try{
		 var prodcode = req.body.amc_code+req.body.prodcode;
                    const pipeline3 = [  //trans_cams
                        {$match : {"FOLIO_NO":req.body.folio,"AMC_CODE":req.body.amc_code,"PRODCODE":prodcode}}, 
                        {$group : {_id : {INV_NAME:"$INV_NAME",BANK_NAME:"$BANK_NAME",AC_NO:"$AC_NO", AMC_CODE:"$AMC_CODE", PRODCODE:"$PRODCODE", code :{$reduce:{input:{$split:["$PRODCODE","$AMC_CODE"]},initialValue: "",in: {$concat: ["$$value","$$this"]}} } ,UNITS:{$sum:"$UNITS"}, AMOUNT:{$sum:"$AMOUNT"}  }}},
                        {$lookup:
                        {
                        from: "products",
                        let: { ccc: "$_id.code", amc:"$_id.AMC_CODE"},
                        pipeline: [
                            { $match:
                                { $expr:
                                    { $and:
                                    [
                                        { $eq: [ "$PRODUCT_CODE",  "$$ccc" ] },
                                        { $eq: [ "$AMC_CODE", "$$amc" ] }
                                    ]
                                    }
                                }
                            },
                            { $project: {  _id: 0 } }
                        ],
                        as: "products"
                        }},
                        { $unwind: "$products"},
                        {$group :{ _id: {INV_NAME:"$_id.INV_NAME",BANK_NAME:"$_id.BANK_NAME",AC_NO:"$_id.AC_NO", products:"$products.ISIN" } , UNITS:{$sum:"$_id.UNITS"}, AMOUNT:{$sum:"$_id.AMOUNT"} } },
                        {$lookup: { from: 'cams_nav',localField: '_id.products',foreignField: 'ISINDivPayoutISINGrowth',as: 'nav' } },
                        { $unwind: "$nav"},                                                                                                                                                        
                        {$project:  {_id:0 , INVNAME:"$_id.INV_NAME",BANK_NAME:"$_id.BANK_NAME",AC_NO:"$_id.AC_NO",products:"$products.ISIN", cnav:"$nav.NetAssetValue"  ,navdate:{ $dateToString: { format: "%d-%m-%Y", date: "$nav.Date" } } , UNITS:{$sum:"$UNITS"},AMOUNT:{$sum:"$AMOUNT"} }   },
                    ]
                    const pipeline11 = [  //folio_karvy
                        {$match : {"ACNO":req.body.folio}}, 
                        {$group :{_id :  {INVNAME:"$INVNAME",BNAME:"$BNAME",BNKACNO:"$BNKACNO",NOMINEE:"$NOMINEE",JTNAME2:"$JTNAME2",JTNAME1:"$JTNAME1"} }}, 
                        {$project:{_id:0,INVNAME:"$_id.INVNAME", BANK_NAME:"$_id.BNAME",AC_NO:"$_id.BNKACNO",NOMINEE:"$_id.NOMINEE",JTNAME2:"$_id.JTNAME2",JTNAME1:"$_id.JTNAME1"}}
                   ]  
                   const pipeline33 = [  //folio_cams
                    {$match : {"FOLIOCHK":req.body.folio}}, 
                    {$group :{_id :  {INV_NAME:"$INV_NAME",BANK_NAME:"$BANK_NAME",AC_NO:"$AC_NO",NOM_NAME:"$NOM_NAME",JNT_NAME1:"$JNT_NAME1",JNT_NAME2:"$JNT_NAME2"} }}, 
                    {$project:{_id:0,INVNAME:"$_id.INV_NAME", BANK_NAME:"$_id.BANK_NAME",AC_NO:"$_id.AC_NO",NOMINEE:"$_id.NOM_NAME",JTNAME1:"$_id.JNT_NAME1",JTNAME2:"$_id.JNT_NAME2"}}
               ]    
                const pipeline1=[  //trans_karvy
                        {$match : {"TD_ACNO":req.body.folio,"SCHEMEISIN":req.body.isin}}, 
                        {$group :{_id : {INVNAME:"$INVNAME",SCHEMEISIN:"$SCHEMEISIN",cnav:"$nav.NetAssetValue"},TD_UNITS:{$sum:"$TD_UNITS"}, TD_AMT:{$sum:"$TD_AMT"} }},
                        {$group :{_id:{ INVNAME:"$_id.INVNAME",SCHEMEISIN:"$_id.SCHEMEISIN",cnav:"$nav.NetAssetValue"}, TD_UNITS:{$sum:"$TD_UNITS"},TD_AMT:{$sum:"$TD_AMT"} }},
                        {$lookup: { from: 'cams_nav',localField: '_id.SCHEMEISIN',foreignField: 'ISINDivPayoutISINGrowth',as: 'nav' } },
                            { $unwind: "$nav"},
                        {$project:  {_id:0, INVNAME:"$_id.INVNAME",SCHEMEISIN:"$_id.SCHEMEISIN", cnav:"$nav.NetAssetValue" ,navdate:{ $dateToString: { format: "%d-%m-%Y", date: "$nav.Date" } }  , UNITS:{$sum:"$TD_UNITS"},AMOUNT:{$sum:"$TD_AMT"} }   },
                    ] 
                    const pipeline2=[  //trans_franklin
                        {$match : {"FOLIO_NO":req.body.folio,"ISIN":req.body.isin}}, 
                        {$group :{_id : {INVESTOR_2:"$INVESTOR_2",ISIN:"$ISIN",NOMINEE1:"$NOMINEE1",PBANK_NAME:"$PBANK_NAME",PERSONAL23:"$PERSONAL23",JOINT_NAM2:"$JOINT_NAM2",JOINT_NAM1:"$JOINT_NAM1",cnav:"$nav.NetAssetValue"},UNITS:{$sum:"$UNITS"}, AMOUNT:{$sum:"$AMOUNT"} }},
                        {$group :{_id:{ INVESTOR_2:"$_id.INVESTOR_2",ISIN:"$_id.ISIN",NOMINEE1:"$_id.NOMINEE1",PBANK_NAME:"$_id.PBANK_NAME",PERSONAL23:"$_id.PERSONAL23",JOINT_NAM2:"$_id.JOINT_NAM2",JOINT_NAM1:"$_id.JOINT_NAM1",cnav:"$nav.NetAssetValue"}, UNITS:{$sum:"$UNITS"},AMOUNT:{$sum:"$AMOUNT"} }},
                        {$lookup: { from: 'cams_nav',localField: '_id.ISIN',foreignField: 'ISINDivPayoutISINGrowth',as: 'nav' } },
                        { $unwind: "$nav"},
                        {$project:  {_id:0, INVNAME:"$_id.INVESTOR_2",SCHEMEISIN:"$_id.ISIN",NOMINEE:"$_id.NOMINEE1",BANK_NAME:"$_id.PBANK_NAME",AC_NO:"$_id.PERSONAL23",JTNAME2:"$_id.JOINT_NAM2",JTNAME1:"$_id.JOINT_NAM1", cnav:"$nav.NetAssetValue",navdate:{ $dateToString: { format: "%d-%m-%Y", date: "$nav.Date" } }   , UNITS:{$sum:"$UNITS"},AMOUNT:{$sum:"$AMOUNT"} }   },
                ] 
                var transc = mongoose.model('trans_cams', transcams, 'trans_cams');   
                    var transk = mongoose.model('trans_karvy', transkarvy, 'trans_karvy'); 
                     var foliok = mongoose.model('folio_karvy', foliokarvy, 'folio_karvy');  
                     var folioc = mongoose.model('folio_cams', foliocams, 'folio_cams');   
                    var transf = mongoose.model('trans_franklin', transfranklin, 'trans_franklin');          
                    transc.aggregate(pipeline3, (err, newdata3) => {
                        folioc.aggregate(pipeline33, (err, newdata33) => {
                            transk.aggregate(pipeline1, (err, newdata1) => {
                                foliok.aggregate(pipeline11, (err, newdata11) => {
                                transf.aggregate(pipeline2, (err, newdata2) => {
                                    if (
                                        newdata2 != 0 ||
                                        newdata1 != 0 ||
                                        newdata3 != 0 ||
                                        newdata33 != 0 ||
                                        newdata11 != 0
                                    ) {
                                        resdata = {
                                        status: 200,
                                        message: "Successfull",
                                        data: newdata2
                                        };
                                    } else {
                                        resdata = {
                                        status: 400,
                                        message: "Data not found"
                                        };
                                    }
                                  let merged3 =  newdata3.map((items, j) => Object.assign({}, items, newdata33[j]));
                                  let merged1 =  newdata1.map((items, j) => Object.assign({}, items, newdata11[j]));
                                    var datacon = merged3.concat(merged1.concat(newdata2));
                                    datacon = datacon
                                        .map(JSON.stringify)
                                        .reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
                                        .filter(function(item, index, arr) {
                                        return arr.indexOf(item, index + 1) === -1;
                                        }) // check if there is any occurence of the item in whole array
                                        .reverse()
                                        .map(JSON.parse);
                                    resdata.data = datacon;
                                    //console.log("res="+JSON.stringify(resdata))
                                    res.json(resdata);
                                    return resdata;
                                    });  
                              });
                            });
                        });
                    })
		 } catch (err) {
                    console.log(err)
                }
}) 


app.post("/api/getamclist", function(req, res) { 
	try{
    if(req.body.pan === "" || req.body.pan === undefined || req.body === "" || req.body.pan === null){
                    resdata= {
                        status:400,
                        message:'Data not found',            
                   }
                   res.json(resdata) 
                   return resdata;
                  }else{
                    var pan = req.body.pan;

     const pipeline = [
      //trans_franklin
      { $match: { IT_PAN_NO1: pan } },
      { $group: { _id: { FOLIO_NO: "$FOLIO_NO", COMP_CODE: "$COMP_CODE"  } } },
      {
        $project: {
          _id: 0,
          folio: "$_id.FOLIO_NO",
          amc_code: "$_id.COMP_CODE",
        }       
      },
      {$sort: {amc_code: 1}},
    
    ];

    const pipeline1 = [
      //trans_cams
      { $match: { PAN: pan } },
      { $group: { _id: { FOLIO_NO: "$FOLIO_NO", AMC_CODE: "$AMC_CODE" } } },
      {
        $project: {
          _id: 0,
          folio: "$_id.FOLIO_NO",
          amc_code: "$_id.AMC_CODE",
        }
      },
       {$sort: {amc_code: 1}}
    ];
    const pipeline2 = [
      //trans_karvy
      { $match: { PAN1: pan } },
      { $group: { _id: { TD_ACNO: "$TD_ACNO", TD_FUND: "$TD_FUND" } } },
      {
        $project: {
          _id: 0,
          folio: "$_id.TD_ACNO",
          amc_code: "$_id.TD_FUND",
        }
      },
       {$sort: {amc_code: 1}}
    ];
    transf.aggregate(pipeline, (err, newdata) => {
      transc.aggregate(pipeline1, (err, newdata1) => {
        transk.aggregate(pipeline2, (err, newdata2) => {
          if (
            newdata2.length != 0 ||
            newdata1.length != 0 ||
            newdata.length != 0
          ) {
            resdata = {
              status: 200,
              message: "Successfull",
              data: newdata2
            };
          } else {
            resdata = {
              status: 400,
              message: "Data not found"
            };
          }
          var datacon = newdata.concat(newdata1.concat(newdata2));
          datacon = datacon
            .map(JSON.stringify)
            .reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
            .filter(function(item, index, arr) {
              return arr.indexOf(item, index + 1) === -1;
            }) // check if there is any occurence of the item in whole array
            .reverse()
            .map(JSON.parse);
           
            for(var i=0; i<datacon.length; i++){
                if(datacon[i]['amc_code'] != "" &&  datacon[i]['folio'] != "" &&  datacon[i]['scheme'] != "" ){                 
                    resdata.data = datacon[i];             
                }
            }
		resdata.data = datacon.sort((a, b) => (a.amc_code > b.amc_code) ? 1 : -1);
          res.json(resdata);
          return resdata;
        });
      });
    });
                  }
                } catch (err) {
                    console.log(err)
                }
});


app.post("/api/gettaxsavinguserwise", function (req, res) {
    try{
    var yer = req.body.fromyear;
    var secyer = req.body.toyear;
    yer = yer + "-04-01";
    secyer = secyer + "-03-31"
    var pan = req.body.pan;

    var transk = mongoose.model('trans_karvy', transkarvy, 'trans_karvy');
    var transc = mongoose.model('trans_cams', transcams, 'trans_cams');
    var transf = mongoose.model('trans_franklin', transfranklin, 'trans_franklin');
     if(req.body.pan != "" && req.body.name != ""){
        const pipeline = [  ///trans_cams
            { $match: { $and: [{ SCHEME: /Tax/ }, { PAN: pan },{ INV_NAME: {$regex : `^${req.body.name}.*` , $options: 'i' } }, { TRXN_NATUR: { $not: /^Redemption.*/ } }, { TRXN_NATUR: { $not: /^Dividend Paid.*/ } }, { TRXN_NATUR: { $not: /^Switchout.*/ } }, { TRXN_NATUR: { $not: /^Transfer-Out.*/ } }, { TRXN_NATUR: { $not: /^Lateral Shift Out.*/ } },{ TRADDATE: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }] } },
            { $group: { _id: { INV_NAME: "$INV_NAME", PAN: "$PAN", SCHEME: "$SCHEME", TRXN_NATUR: "$TRXN_NATUR", FOLIO_NO: "$FOLIO_NO", AMOUNT: "$AMOUNT", TRADDATE: "$TRADDATE" } } },
            { $project: { _id: 0, INVNAME: "$_id.INV_NAME", PAN: "$_id.PAN", SCHEME: "$_id.SCHEME", TRXN_NATURE: "$_id.TRXN_NATUR", FOLIO_NO: "$_id.FOLIO_NO", AMOUNT: "$_id.AMOUNT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRADDATE" } }} },
            { $sort: { TRADDATE: -1 } }
        ]
        const pipeline1 = [  ///trans_karvy
            { $match: { $and: [{ FUNDDESC: /Tax/ }, { PAN1: pan },{ INVNAME: {$regex : `^${req.body.name}.*` , $options: 'i' } } ,{ TRDESC: { $not: /^Redemption.*/ } }, { TRDESC: { $not: /^Dividend Paid.*/ } }, { TRXN_NATURE: { $not: /^Switchout.*/ } }, { TRXN_NATURE: { $not: /^Transfer-Out.*/ } }, { TRXN_NATURE: { $not: /^Lateral Shift Out.*/ } },{ TD_TRDT: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } } ] } },
            { $group: { _id: { INVNAME: "$INVNAME", PAN1: "$PAN1", FUNDDESC: "$FUNDDESC", TRDESC: "$TRDESC", TD_ACNO: "$TD_ACNO", TD_AMT: "$TD_AMT", TD_TRDT: "$TD_TRDT" } } },
            { $project: { _id: 0, INVNAME: "$_id.INVNAME", PAN: "$_id.PAN1", SCHEME: "$_id.FUNDDESC", TRXN_NATURE: "$_id.TRDESC", FOLIO_NO: "$_id.TD_ACNO", AMOUNT: "$_id.TD_AMT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TD_TRDT" } } } },
            { $sort: { TRADDATE: -1 } }
        ]
        const pipeline2 = [  ///trans_franklin
            { $match: { $and: [{ SCHEME_NA1: /Tax/ }, { IT_PAN_NO1: pan }, { INVESTOR_2: {$regex : `^${req.body.name}.*` , $options: 'i' } },{ TRXN_TYPE: { $not: /^Redemption.*/ } }, { TRXN_TYPE: { $not: /^Dividend Paid.*/ } }, { TRXN_TYPE: { $not: /^Switchout.*/ } }, { TRXN_TYPE: { $not: /^Transfer-Out.*/ } }, { TRXN_TYPE: { $not: /^Lateral Shift Out.*/ } } , { TRXN_DATE: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }  ] } },
            { $group: { _id: { INVESTOR_2: "$INVESTOR_2", IT_PAN_NO1: "$IT_PAN_NO1", SCHEME_NA1: "$SCHEME_NA1", TRXN_TYPE: "$TRXN_TYPE", FOLIO_NO: "$FOLIO_NO", AMOUNT: "$AMOUNT", TRXN_DATE: "$TRXN_DATE"  } } },
            { $project: { _id: 0, INVNAME: "$INVESTOR_2", PAN: "$_id.IT_PAN_NO1", SCHEME: "$_id.SCHEME_NA1", TRXN_NATURE: "$_id.TRXN_TYPE", FOLIO_NO: "$_id.FOLIO_NO", AMOUNT: "$_id.AMOUNT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRXN_DATE" } } } },
            { $sort: { TRADDATE: -1 } }
        ]
        transc.aggregate(pipeline, (err, camsdata) => {
            transk.aggregate(pipeline1, (err, karvydata) => {
                transf.aggregate(pipeline2, (err, frankdata) => {
                    if (frankdata.length != 0 || karvydata.length != 0 || camsdata.length != 0) {
                        resdata = {
                            status: 200,
                            message: 'Successfull',
                            data: frankdata
                        }
                    } else {
                        resdata = {
                            status: 400,
                            message: 'Data not found',
                        }
                    }
                    var datacon = frankdata.concat(karvydata.concat(camsdata))
                    datacon = datacon.map(JSON.stringify).reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
                        .filter(function (item, index, arr) { return arr.indexOf(item, index + 1) === -1; }) // check if there is any occurence of the item in whole array
                        .reverse().map(JSON.parse);
                    for (var i = 0; i < datacon.length; i++) {
                        if (datacon[i]['TRXN_NATURE'] === "Redemption") {
                            datacon[i]['TRXN_NATURE'] = "RED";
                        } if (datacon[i]['TRXN_NATURE'].match(/Systematic Investment.*/) || datacon[i]['TRXN_NATURE'].match(/Systematic Withdrawal.*/) || datacon[i]['TRXN_NATURE'].match(/Systematic - Instalment.*/) || datacon[i]['TRXN_NATURE'].match(/Systematic - To.*/) || datacon[i]['TRXN_NATURE'].match(/Systematic-NSE.*/) || datacon[i]['TRXN_NATURE'].match(/Systematic Physical.*/) || datacon[i]['TRXN_NATURE'].match(/Systematic.*/) || datacon[i]['TRXN_NATURE'].match(/Systematic-Normal.*/) || datacon[i]['TRXN_NATURE'].match(/Systematic (ECS).*/)) {
                            datacon[i]['TRXN_NATURE'] = "SIP";
                        } if (Math.sign(datacon[i]['AMOUNT']) === -1) {
                            datacon[i]['TRXN_NATURE'] = "SIPR";
                        } if (datacon[i]['TRXN_NATURE'].match(/Systematic - From.*/)) {
                            datacon[i]['TRXN_NATURE'] = "STP";
                        }
                    }
                    resdata.data = datacon.sort((a, b) => new Date(b.TRADDATE.split("-").reverse().join("/")).getTime() - new Date(a.TRADDATE.split("-").reverse().join("/")).getTime())
                    res.json(resdata)
                    return resdata
                });
            });
        });
    }else{
        resdata = {
            status: 400,
            message: 'Data not found',
        }
    }
} catch (err) {
    console.log(err)
}
});
app.post("/api/getsipstpuserwise", function (req, res) {
    try{
    var mon = parseInt(req.body.month);
    var yer = parseInt(req.body.year);

    var pan = req.body.pan;
    if(req.body.pan != "" && req.body.name != ""){
        const pipeline = [  ///trans_cams
            { $group: { _id: { INV_NAME: "$INV_NAME", PAN: "$PAN", TRXN_NATUR: "$TRXN_NATUR", FOLIO_NO: "$FOLIO_NO", SCHEME: "$SCHEME", AMOUNT: "$AMOUNT",TAX_STATUS:"$TAX_STATUS" ,TRADDATE: "$TRADDATE" } } },
            { $project: { _id: 0, INVNAME: "$_id.INV_NAME", PAN: "$_id.PAN", TRXN_NATUR: "$_id.TRXN_NATUR",  FOLIO_NO: "$_id.FOLIO_NO", SCHEME: "$_id.SCHEME", AMOUNT: "$_id.AMOUNT" ,TAX_STATUS:"$_id.TAX_STATUS", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRADDATE" } }, month: { $month: ('$_id.TRADDATE') }, year: { $year: ('$_id.TRADDATE') } } },
            { $match: { $and: [{ month: mon }, { year: yer }, { PAN: pan }, { INVNAME: {$regex : `^${req.body.name}.*` , $options: 'si' } } , { TRXN_NATUR: /Systematic/ } ] } },
            { $sort: { TRADDATE: -1 } }
        ]
        const pipeline1 = [  ///trans_karvy
            { $group: { _id: { INVNAME: "$INVNAME", PAN1: "$PAN1", TRDESC: "$TRDESC",  TD_ACNO: "$TD_ACNO", FUNDDESC: "$FUNDDESC", TD_AMT: "$TD_AMT",STATUS:"$STATUS", TD_TRDT: "$TD_TRDT" } } },
            { $project: { _id: 0, INVNAME: "$_id.INVNAME", PAN: "$_id.PAN1", TRXN_NATUR: "$_id.TRDESC", FOLIO_NO: "$_id.TD_ACNO", SCHEME: "$_id.FUNDDESC",STATUS:"$_id.STATUS" ,AMOUNT: "$_id.TD_AMT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TD_TRDT" } }, month: { $month: ('$_id.TD_TRDT') }, year: { $year: ('$_id.TD_TRDT') } } },
            { $match: { $and: [{ month: mon }, { year: yer }, { PAN: pan } ,{ INVNAME: {$regex : `^${req.body.name}.*` , $options: 'si' } } , { TRXN_NATUR: /Systematic/ }  ] } },
            { $sort: { TRADDATE: -1 } }
        ]
        const pipeline2 = [  ///trans_franklin
            { $group: { _id: { INVESTOR_2: "$INVESTOR_2", IT_PAN_NO1: "$IT_PAN_NO1", TRXN_TYPE: "$TRXN_TYPE", FOLIO_NO: "$FOLIO_NO", SCHEME_NA1: "$SCHEME_NA1",SOCIAL_S18:"$SOCIAL_S18", AMOUNT: "$AMOUNT", TRXN_DATE: "$TRXN_DATE" } } },
            { $project: { _id: 0, INVNAME: "$INVESTOR_2", PAN: "$_id.IT_PAN_NO1", TRXN_NATUR: "$_id.TRXN_TYPE", FOLIO_NO: "$_id.FOLIO_NO", SCHEME: "$_id.SCHEME_NA1",SOCIAL_S18:"$_id.SOCIAL_S18", AMOUNT: "$_id.AMOUNT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRXN_DATE" } }, month: { $month: ('$_id.TRXN_DATE') }, year: { $year: ('$_id.TRXN_DATE') } } },
            { $match: { $and: [{ month: mon }, { year: yer }, { PAN: pan } , { INVNAME: {$regex : `^${req.body.name}.*` , $options: 'si' } }    ] } },
            { $sort: { TRADDATE: -1 } }
        ]

        transc.aggregate(pipeline, (err, camsdata) => {
            transk.aggregate(pipeline1, (err, karvydata) => {
                transf.aggregate(pipeline2, (err, frankdata) => {
                    if (frankdata.length != 0 || karvydata.length != 0 || camsdata.length != 0) {
                        resdata = {
                            status: 200,
                            message: 'Successfull',
                            data: frankdata
                        }
                    } else {
                        resdata = {
                            status: 400,
                            message: 'Data not found',
                        }
                    }
                    datacon = frankdata.concat(karvydata.concat(camsdata))
                    datacon = datacon.map(JSON.stringify).reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
                        .filter(function (item, index, arr) { return arr.indexOf(item, index + 1) === -1; }) // check if there is any occurence of the item in whole array
                        .reverse().map(JSON.parse);
                    for (var i = 0; i < datacon.length; i++) {
                        if (datacon[i]['TRXN_NATUR'].match(/Systematic.*/)) {
                            datacon[i]['TRXN_NATUR'] = "SIP";
                        }
                        if ((Math.sign(datacon[i]['AMOUNT']) === -1)) {
                            datacon[i]['TRXN_NATUR'] = "SIPR";
                        }
                        if (datacon[i]['TRXN_NATUR'].match(/Systematic - From.*/)) {
                            datacon[i]['TRXN_NATUR'] = "STP";
                        }
                    }
                    resdata.data = datacon.sort((a, b) => new Date(b.TRADDATE.split("-").reverse().join("/")).getTime() - new Date(a.TRADDATE.split("-").reverse().join("/")).getTime())
                    res.json(resdata)
                    return resdata
                });
            });
        });
    }else{
        resdata = {
            status: 400,
            message: 'Data not found',
        }
        res.json(resdata)
         return resdata
    }
} catch (err) {
    console.log(err)
}
})

app.post("/api/getdividendscheme", function (req, res) {
    try{
    var yer = req.body.fromyear;
    var secyer = req.body.toyear;
    yer = yer + "-04-01";
    secyer = secyer + "-03-31"
    if(req.body.pan != "" && req.body.name != ""){
    const pipeline = [  ///trans_cams                                                     
        { $match: { $and: [{ TRXN_NATUR: /Div/ }, { SCHEME: req.body.scheme },{ INV_NAME: {$regex : `^${req.body.name}.*` , $options: 'i' } }, { PAN: req.body.pan }, { TRADDATE: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }] } },
        { $group: { _id: { INV_NAME: "$INV_NAME", SCHEME: "$SCHEME", TRXN_NATUR: "$TRXN_NATUR", FOLIO_NO: "$FOLIO_NO", AMOUNT: "$AMOUNT", TRADDATE: "$TRADDATE" } } },
        { $project: { _id: 0, INVNAME: "$_id.INV_NAME", SCHEME: "$_id.SCHEME", TRXN_NATUR: "$_id.TRXN_NATUR", FOLIO_NO: "$_id.FOLIO_NO", AMOUNT: "$_id.AMOUNT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRADDATE" } } } },
    ]
    const pipeline1 = [  ///trans_karvy
        { $match: { $and: [{ TRDESC: /Div/ }, { FUNDDESC: req.body.scheme },{ INVNAME: {$regex : `^${req.body.name}.*` , $options: 'i' } } , { PAN1: req.body.pan }, { TD_TRDT: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }] } },
        { $group: { _id: { INVNAME: "$INVNAME", FUNDDESC: "$FUNDDESC", TRDESC: "$TRDESC", TD_ACNO: "$TD_ACNO", TD_AMT: "$TD_AMT", TD_TRDT: "$TD_TRDT" } } },
        { $project: { _id: 0, INVNAME: "$_id.INVNAME", SCHEME: "$_id.FUNDDESC", TRXN_NATUR: "$_id.TRDESC", FOLIO_NO: "$_id.TD_ACNO", AMOUNT: "$_id.TD_AMT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TD_TRDT" } } } },
    ]
    const pipeline2 = [  ///trans_franklin
        { $match: { $and: [{ TRXN_TYPE: /Div/ }, { SCHEME_NA1: req.body.scheme },{ INVESTOR_2: {$regex : `^${req.body.name}.*` , $options: 'i' } }, { IT_PAN_NO1: req.body.pan }, { TRXN_DATE: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }] } },
        { $group: { _id: { INVESTOR_2: "$INVESTOR_2", SCHEME_NA1: "$SCHEME_NA1", TRXN_TYPE: "$TRXN_TYPE", FOLIO_NO: "$FOLIO_NO", AMOUNT: "$AMOUNT", TRXN_DATE: "$TRXN_DATE" } } },
        { $project: { _id: 0, INVNAME: "$_id.INVESTOR_2", SCHEME: "$_id.SCHEME_NA1", TRXN_NATUR: "$_id.TRXN_TYPE", FOLIO_NO: "$_id.FOLIO_NO", AMOUNT: "$_id.AMOUNT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRXN_DATE" } } } },
    ]

    transc.aggregate(pipeline, (err, camsdata) => {
        transk.aggregate(pipeline1, (err, karvydata) => {
            transf.aggregate(pipeline2, (err, frankdata) => {
                if (camsdata != 0 || karvydata != 0 || frankdata != 0) {
                    resdata = {
                        status: 200,
                        message: 'Successfull',
                        data: frankdata
                    }
                } else {
                    resdata = {
                        status: 400,
                        message: 'Data not found',
                    }
                }
                var datacon = frankdata.concat(karvydata.concat(camsdata))
                datacon = datacon.map(JSON.stringify).reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
                    .filter(function (item, index, arr) { return arr.indexOf(item, index + 1) === -1; }) // check if there is any occurence of the item in whole array
                    .reverse().map(JSON.parse);
                for (var i = 0; i < datacon.length; i++) {
                    if (datacon[i]['TRXN_NATUR'] === "Gross Dividend") {
                        datacon[i]['TRXN_NATUR'] = "Dividend Payout";
                    } if (datacon[i]['TRXN_NATUR'].match(/Div. Rei.*/) || datacon[i]['TRXN_NATUR'].match(/Dividend Reinvest*/)) {
                        datacon[i]['TRXN_NATUR'] = "Div. Reinv.";
                    } if (datacon[i]['TRXN_NATUR'].match(/Dividend Paid*/)) {
                        datacon[i]['TRXN_NATUR'] = "Div. Paid";
                    }
                }
                resdata.data = datacon;
                resdata.data = datacon.sort((a, b) => new Date(b.TRADDATE.split("-").reverse().join("/")).getTime() - new Date(a.TRADDATE.split("-").reverse().join("/")).getTime())
                res.json(resdata)
                return resdata
            });
        });
    });
}else{
    resdata = {
        status: 400,
        message: 'Data not found',
    }
	 res.json(resdata)
         return resdata
}
} catch (err) {
    console.log(err)
}
});

app.post("/api/getdividend", function (req, res) {
    try{
    var yer = req.body.fromyear;
    var secyer = req.body.toyear;
    yer = yer + "-04-01";
    secyer = secyer + "-03-31"
   if(req.body.pan != "" && req.body.name != ""){
    const pipeline = [  ///trans_cams                                                     
        { $match: { $and: [{ TRXN_NATUR: /Div/ }, { PAN: req.body.pan }, { INV_NAME: {$regex : `^${req.body.name}.*` , $options: 'i' } },{ TRADDATE: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }] } },
        { $group: { _id: { SCHEME: "$SCHEME", INV_NAME: "$INV_NAME" }, AMOUNT: { $sum: "$AMOUNT" } } },
        { $project: { _id: 0, SCHEME: "$_id.SCHEME", INVNAME: "$_id.INV_NAME", AMOUNT: { $sum: "$AMOUNT" } } },
    ]
    const pipeline1 = [  ///trans_karvy
        { $match: { $and: [{ TRDESC: /Div/ }, { PAN1: req.body.pan },{ INVNAME: {$regex : `^${req.body.name}.*` , $options: 'i' } }, { TD_TRDT: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }] } },
        { $group: { _id: { FUNDDESC: "$FUNDDESC", INVNAME: "$INVNAME" }, AMOUNT: { $sum: "$TD_AMT" } } },
        { $project: { _id: 0, SCHEME: "$_id.FUNDDESC", INVNAME: "$_id.INVNAME", AMOUNT: { $sum: "$TD_AMT" } } },
    ]
    const pipeline2 = [  ///trans_franklin
        { $match: { $and: [{ TRXN_TYPE: /Div/ }, { IT_PAN_NO1: req.body.pan },{ INVESTOR_2: {$regex : `^${req.body.name}.*` , $options: 'i' } }, { TRXN_DATE: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }] } },
        { $group: { _id: { SCHEME_NA1: "$SCHEME_NA1", INVESTOR_2: "$INVESTOR_2" }, AMOUNT: { $sum: "$AMOUNT" } } },
        { $project: { _id: 0, SCHEME: "$_id.SCHEME_NA1", INVNAME: "$_id.INVESTOR_2", AMOUNT: { $sum: "$AMOUNT" } } },

    ]

    transc.aggregate(pipeline, (err, newdata) => {
        transk.aggregate(pipeline1, (err, newdata1) => {
            transf.aggregate(pipeline2, (err, newdata2) => {
                if (newdata != 0 || newdata1 != 0 || newdata2 != 0) {
                    resdata = {
                        status: 200,
                        message: 'Successfull',
                        data: newdata2
                    }
                } else {
                    resdata = {
                        status: 400,
                        message: 'Data not found',
                    }
                }
                var datacon = newdata2.concat(newdata1.concat(newdata))
                datacon = datacon.map(JSON.stringify).reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
                    .filter(function (item, index, arr) { return arr.indexOf(item, index + 1) === -1; }) // check if there is any occurence of the item in whole array
                    .reverse().map(JSON.parse);
                resdata.data = datacon;
                res.json(resdata)
                return resdata
            });
        });
    })
}else{
    resdata = {
        status: 400,
        message: 'Data not found',
    }
	 res.json(resdata)
         return resdata
}
} catch (err) {
    console.log(err)
}
});



app.post("/api/gettransactionuserwise", function (req, res) {
    try{
    var mon = parseInt(req.body.month);
    var yer = parseInt(req.body.year);
    if(req.body.pan != "" && req.body.name != ""){
        const pipeline = [  ///trans_cams
            { $group: { _id: { INV_NAME: "$INV_NAME", PAN: "$PAN", TRXN_NATUR: "$TRXN_NATUR", FOLIO_NO: "$FOLIO_NO", SCHEME: "$SCHEME", AMOUNT: "$AMOUNT", TRADDATE: "$TRADDATE" } } },
            { $project: { _id: 0, INVNAME: "$_id.INV_NAME", PAN: "$_id.PAN", TRXN_NATUR: "$_id.TRXN_NATUR", FOLIO_NO: "$_id.FOLIO_NO", SCHEME: "$_id.SCHEME", AMOUNT: "$_id.AMOUNT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRADDATE" } }, month: { $month: ('$_id.TRADDATE') }, year: { $year: ('$_id.TRADDATE') } } },
            { $match: { $and: [{ month: mon }, { year: yer }, { PAN: req.body.pan } , { INVNAME: {$regex : `^${req.body.name}.*` , $options: 'i' } } ] } },
            { $sort: { TRADDATE: -1 } }
        ]
        const pipeline1 = [  ///trans_karvy
            { $group: { _id: { INVNAME: "$INVNAME", PAN1: "$PAN1", TRDESC: "$TRDESC", TD_ACNO: "$TD_ACNO", FUNDDESC: "$FUNDDESC", TD_AMT: "$TD_AMT", TD_TRDT: "$TD_TRDT" } } },
            { $project: { _id: 0, INVNAME: "$_id.INVNAME", PAN: "$_id.PAN1", TRXN_NATUR: "$_id.TRDESC", FOLIO_NO: "$_id.TD_ACNO", SCHEME: "$_id.FUNDDESC", AMOUNT: "$_id.TD_AMT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TD_TRDT" } }, month: { $month: ('$_id.TD_TRDT') }, year: { $year: ('$_id.TD_TRDT') } } },
            { $match: { $and: [{ month: mon }, { year: yer }, { PAN: req.body.pan }, { INVNAME: {$regex : `^${req.body.name}.*` , $options: 'i' } } ] } },
            { $sort: { TRADDATE: -1 } }
        ]
        const pipeline2 = [  ///trans_franklin
            { $group: { _id: { INVESTOR_2: "$INVESTOR_2", IT_PAN_NO1: "$IT_PAN_NO1", TRXN_TYPE: "$TRXN_TYPE", FOLIO_NO: "$FOLIO_NO", SCHEME_NA1: "$SCHEME_NA1", AMOUNT: "$AMOUNT", TRXN_DATE: "$TRXN_DATE" } } },
            { $project: { _id: 0, INVNAME: "$INVESTOR_2", PAN: "$_id.IT_PAN_NO1", TRXN_NATUR: "$_id.TRXN_TYPE", FOLIO_NO: "$_id.FOLIO_NO", SCHEME: "$_id.SCHEME_NA1", AMOUNT: "$_id.AMOUNT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRXN_DATE" } }, month: { $month: ('$_id.TRXN_DATE') }, year: { $year: ('$_id.TRXN_DATE') } } },
            { $match: { $and: [{ month: mon }, { year: yer }, { PAN: req.body.pan }, { INVNAME: {$regex : `^${req.body.name}.*` , $options: 'i' } } ] } },
            { $sort: { TRADDATE: -1 } }
        ]

        transc.aggregate(pipeline, (err, camsdata) => {
            transk.aggregate(pipeline1, (err, karvydata) => {
                transf.aggregate(pipeline2, (err, frankdata) => {
                    if (frankdata.length != 0 || karvydata.length != 0 || camsdata.length != 0) {
                        //   if( newdata != 0){
                        resdata = {
                            status: 200,
                            message: 'Successfull',
                            data: frankdata
                        }
                    } else {
                        resdata = {
                            status: 400,
                            message: 'Data not found',
                        }
                    }
                    var datacon = frankdata.concat(karvydata.concat(camsdata))
                    datacon = datacon.map(JSON.stringify).reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
                        .filter(function (item, index, arr) { return arr.indexOf(item, index + 1) === -1; }) // check if there is any occurence of the item in whole array
                        .reverse().map(JSON.parse);
                    for (var i = 0; i < datacon.length; i++) {
                        if (datacon[i]['TRXN_NATUR'] === "Redemption") {
                            datacon[i]['TRXN_NATUR'] = "RED";
                        } if (datacon[i]['TRXN_NATUR'].match(/Systematic Investment.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic Withdrawal.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic - Instalment.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic - To.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic-NSE.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic Physical.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic-Normal.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic (ECS).*/)) {
                            datacon[i]['TRXN_NATUR'] = "SIP";
                        } if (Math.sign(datacon[i]['AMOUNT']) === -1) {
                            datacon[i]['TRXN_NATUR'] = "SIPR";
                        } if (datacon[i]['TRXN_NATUR'].match(/Systematic - From.*/)) {
                            datacon[i]['TRXN_NATUR'] = "STP";
                        }
                    }
                    resdata.data = datacon.sort((a, b) => new Date(b.TRADDATE.split("-").reverse().join("/")).getTime() - new Date(a.TRADDATE.split("-").reverse().join("/")).getTime())
                    res.json(resdata)
                    return resdata
                });
            });
        });
    }else{
        resdata = {
            status: 400,
            message: 'Data not found',
        }
	     res.json(resdata)
             return resdata
    }
} catch (err) {
    console.log(err)
}
})

   app.post("/api/getschemelist", function (req, res) {
	   try{
        var pan = req.body.pan;;
        const pipeline = [
                {$match : {PAN:pan}},
                {$group : {_id : { AMC_CODE:"$AMC_CODE", PRODCODE:"$PRODCODE", code :{$reduce:{input:{$split:["$PRODCODE","$AMC_CODE"]},initialValue: "",in: {$concat: ["$$value","$$this"]}} }  }}},
                {$lookup:
                {
                from: "products",
                let: { ccc: "$_id.code", amc:"$_id.AMC_CODE"},
                pipeline: [
                    { $match:
                        { $expr:
                            { $and:
                            [
                                { $eq: [ "$PRODUCT_CODE",  "$$ccc" ] },
                                { $eq: [ "$AMC_CODE", "$$amc" ] }
                            ]
                            }
                        }
                    },
                    { $project: {  _id: 0 } }
                ],
                as: "products"
                }},
                { $unwind: "$products"},
                {$project :{ _id:0 , products:"$products" } },
           ]
        const pipeline1=[  //trans_karvy
            {$match : {PAN1:pan,SCHEMEISIN : {$ne : null}}}, 
            {$group : {_id:{SCHEMEISIN:"$SCHEMEISIN"} }},
            {$lookup: { from: 'products',localField: '_id.SCHEMEISIN',foreignField: 'ISIN',as: 'master' } },
            { $unwind: "$master"},
            {$project:{_id:0,products:"$master" }   },
       ] 
        const pipeline2=[ ///trans_franklin
            {$match : {IT_PAN_NO1:pan,ISIN : {$ne : null}}}, 
            {$group : {_id:{ISIN:"$ISIN"} }},
            {$lookup: { from: 'products',localField: '_id.ISIN',foreignField: 'ISIN',as: 'master' } },
            { $unwind: "$master"},
            {$project:{_id:0,products:"$master" }   },
      ] 
        transc.aggregate(pipeline, (err, newdata2) => {
           transf.aggregate(pipeline2, (err, newdata1) => {
                transk.aggregate(pipeline1, (err, newdata) => {
                       if(newdata2.length != 0  || newdata1.length != 0 || newdata.length != 0){        
                            resdata= {
                               status:200,
                               message:'Successfull',
                               data:  newdata2
                             }
                           }else{
                               resdata= {
                               status:400,
                               message:'Data not found',            
                          }
                           }
                           var datacon = newdata2.concat(newdata1.concat(newdata))
                           datacon = datacon.map(JSON.stringify).reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
                           .filter(function(item, index, arr){ return arr.indexOf(item, index + 1) === -1; }) // check if there is any occurence of the item in whole array
                           .reverse().map(JSON.parse) ;
                            resdata.data = datacon.sort((a, b) => (a.products.PRODUCT_LONG_NAME > b.products.PRODUCT_LONG_NAME) ? 1 : -1);
                           res.json(resdata)  
                           return resdata    
                                   
                  });
               });
  
             });   
             } catch (err) {
                    console.log(err)
                }
        })  

app.get("/api/getfoliolist", function (req, res) {
    Axios.get('https://prodigyfinallive.herokuapp.com/getUserDetails',
    {data:{ email:'sunilguptabfc@gmail.com'}}
      ).then(function(result) {
        //let json = CircularJSON.stringify(result);
        var pan =  result.data.data.User[0].pan_card;
        var datacollection = folio.find({"pan_no":pan}).distinct("foliochk", function (err, newdata) { 
            if(newdata != 0){    
                     resdata= {
                        status:200,
                        message:'Successfull',
                        data:  newdata 
                      }
                    }else{
                     resdata= {
                        status:400,
                        message:'Data not found',            
                   }
                      // res.send(newdata);
                    }
                 //   res.json(resdata)    
                });
        var trans = mongoose.model('trans_cams', transcams, 'trans_cams');
        var datacollection1 = trans.find({"pan":pan}).distinct("folio_no", function (err, newdata) { 
            if(newdata != 0){    
                     resdata1= {
                        status:200,
                        message:'Successfull',
                        data:  newdata 
                      }
                    }else{
                        resdata1= {
                        status:400,
                        message:'Data not found',            
                   }
                      // res.send(newdata);
                    }
                    //res.json(resdata1)
                    var datacon = resdata.data.concat(resdata1.data)
                    var removeduplicates = Array.from(new Set(datacon));
                    resdata.data = removeduplicates
                  //  console.log(datacon)
                  //  console.log(removeduplicates)
                    res.send(resdata)
                
                });
    
            //return resdata
    });
    })



app.use(express.static(path.join(__dirname, '/frontend/build')));
app.get('*', (req, res) =>
  res.sendFile(path.join(__dirname, '/frontend/build/index.html'))
);

const port= process.env.PORT ||  3000;


app.use((err, req, res, next) => {
	res.status(500).send({ message: err.message });
  });

app.listen(port, ()=> { console.log("server started at port ",port)})


