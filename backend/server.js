import express from 'express';
import dotenv from 'dotenv';
 import config from './config.js';
 import db from './config.js';
 import mongoose from 'mongoose';
 import path from 'path';
import bodyParser from 'body-parser';
import nodemailer from 'nodemailer';
import localStorage from 'localStorage'
import Axios from 'axios'
import moment from 'moment';
import MongoData from 'mongodb';
import _ from 'underscore';


var Schema = mongoose.Schema;
// dotenv.config();
 const mongodbUrl= config.MONGODB_URL;
mongoose.Promise = global.Promise;


const options = {
    useMongoClient: true,
    autoIndex: false, // Don't build indexes
    reconnectTries: Number.MAX_VALUE, // Never stop trying to reconnect
    reconnectInterval: 500, // Reconnect every 500ms
    poolSize: 10, // Maintain up to 10 socket connections
    // If not connected, return errors immediately rather than waiting for reconnect
    bufferMaxEntries: 0
  };
mongoose.connect(mongodbUrl, {
	useNewUrlParser:true,
	useUnifiedTopology: true,
	promiseLibrary: global.Promise,
	//useCreateIndex:true,
	autoIndex: false, // Don't build indexes
	//useMongoClient: true,
// 	reconnectTries: Number.MAX_VALUE, // Never stop trying to reconnect
// 	reconnectInterval: 500, // Reconnect every 500ms
	poolSize: 30, // Maintain up to 10 socket connections
	autoReconnect:true,
	socketTimeoutMS:360000,
	connectTimeoutMS:360000,
        // If not connected, return errors immediately rather than waiting for reconnect
	bufferMaxEntries: 0
}).catch(error => console.log(error.reason));
const app=express();
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
//app.use(bodyParser());

const __dirname = path.resolve();
app.use('/uploads', express.static(path.join(__dirname, '/uploads')));
app.get("/", (req, res) => {
  res.json({ message: "Welcome to WMS Api application." });
});

const navcams = new Schema({
    SchemeCode: { type: String },
    ISINDivPayoutISINGrowth: { type: String },
    ISINDivReinvestment: { type: String },
    SchemeName: { type: String, required: true },
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

const transcams2A = new Schema({
    AMC_CODE: { type: String },
    FOLIO_NO: { type: String },
    PRODCODE: { type: String },
    SCHEME: { type: String },
    INV_NAME: { type: String },
    TRXNNO: { type: String },
    TRXNMODE: { type: String },
    TRXNSTAT: { type: String },
    TRADDATE: { type: Date },
    POSTDATE: { type: Date },
    PURPRICE: { type: Number },
    UNITS: { type: Number },
    AMOUNT: { type: Number },
    TRXN_NATUR: { type: String },
    MICR_NO: { type: String },
    SWFLAG: { type: String },
    SEQ_NO: { type: String },
    LOCATION: { type: String },
    SCHEME_TYP: { type: String },
    TAX_STATUS: { type: String },
    LOAD: { type: Number },
    PAN: { type: String },
    TRXN_TYPE_: { type: String },
    EUIN: { type: String },
    AC_NO: { type: String },
    BANK_NAME: { type: String },
    OLD_FOLIO: { type: String},
    FOLIO_OLD: { type: String},
    SCHEME_FOL: { type: String},
    AMC_REF_NO: { type: String},
    REQUEST_RE: { type: String},
}, { versionKey: false });

const foliocams = new Schema({
    FOLIOCHK: { type: String },
    INV_NAME: { type: String },
    ADDRESS1: { type: String },
    ADDRESS2: { type: String },
    ADDRESS3: { type: String },
    CITY: { type: String },
    PINCODE: { type: String },
    PRODUCT: { type: String },
    SCH_NAME: { type: String },
    CLOS_BAL: { type: Number },
    RUPEE_BAL: { type: Number },
    JNT_NAME1: { type: String },
    JNT_NAME2: { type: String },
    PHONE_OFF: { type: String },
    PHONE_RES: { type: String },
    EMAIL: { type: String },
    HOLDING_NA: { type: String },
    UIN_NO: { type: String },
    PAN_NO: { type: String },
    JOINT1_PAN: { type: String },
    JOINT2_PAN: { type: String },
    GUARD_PAN: { type: String },
    TAX_STATUS: { type: String },
    REINV_FLAG: { type: String },
    BANK_NAME: { type: String },
    BRANCH: { type: String },
    AC_TYPE: { type: String },
    AC_NO: { type: String },
    INV_DOB: { type: String },
    MOBILE_NO: { type: String },
    OCCUPATION: { type: String },
    INV_IIN: { type: String },
    NOM_NAME: { type: String },
    RELATION: { type: String },
    GUARD_NAME: { type: String },
    FOLIO_DATE: { type: Date },
    AMC_CODE: { type: String },
    NOM2_RELAT: { type: String },
    NOM3_RELAT: { type: String },
    NOM2_NAME: { type: String },
    NOM3_NAME: { type: String },
    FOLIO_OLD: { type: String},
    SCHEME_FOL: { type: String},
}, { versionKey: false });


const foliokarvy = new Schema({
    PRCODE: { type: String },
    FUND: { type: String },
    FUNDDESC: { type: String },
    ACNO: { type: String, length: 800 },
    INVNAME: { type: String ,index: true,},
    JTNAME1: { type: String },
    JTNAME2: { type: String },
    ADD1: { type: String },
    ADD2: { type: String },
    ADD3: { type: String },
    CITY: { type: String },
    PIN: { type: String },
    STATE: { type: String },
    COUNTRY: { type: String },
    DOB: { type: String },
    FNAME: { type: String },
    MNAME: { type: String },
    RPHONE: { type: String },
    OPHONE: { type: String },
    STATUS: { type: String },
    OCCPN: { type: String },
    EMAIL: { type: String },
    BNKACNO: { type: String },
    BNAME: { type: String },
    BNKACTYPE: { type: String },
    BADD1: { type: String },
    PANGNO: { type: String },
    MOBILE: { type: String },
    DIVOPT: { type: String },
    OCCP_DESC: { type: String },
    MODEOFHOLD: { type: String },
    PAN2: { type: String },
    PAN3: { type: String },
    IMCATEGORY: { type: String },
    GUARDIANN0: { type: String },
    NOMINEE: { type: String },
    STATUSDESC: { type: String },
    NOMINEE2: { type: String },
    NOMINEE3: { type: String },
    NOMINEEREL: { type: String },
    NOMINEE2R2: { type: String },
    NOMINEE3R3: { type: String },
    GUARDPANNO: { type: String },
    LASTUPDAT1: { type: Date },
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
    TRXNNO: { type: String },
    TRXNMODE: { type: String },
    TRXNSTAT: { type: String },
    TRADDATE: { type: Date },
    POSTDATE: { type: Date },
    PURPRICE: { type: Number },
    UNITS: { type: Number },
    AMOUNT: { type: Number },
    TRXN_NATUR: { type: String },
    MICR_NO: { type: String },
    SWFLAG: { type: String },
    SEQ_NO: { type: String },
    LOCATION: { type: String },
    SCHEME_TYP: { type: String },
    TAX_STATUS: { type: String },
    LOAD: { type: Number },
    PAN: { type: String },
    TRXN_TYPE_: { type: String },
    EUIN: { type: String },
    AC_NO: { type: String },
    BANK_NAME: { type: String },
    OLD_FOLIO: { type: String},
    FOLIO_OLD: { type: String},
    SCHEME_FOL: { type: String},
    AMC_REF_NO: { type: String},
    REQUEST_RE: { type: String},
}, { versionKey: false });

const transkarvy = new Schema({
    FMCODE: { type: String },
    TD_FUND: { type: String },
    TD_ACNO: { type: String , length: 800},
    SCHPLN: { type: String },
    FUNDDESC: { type: String },
    TD_TRNO: { type: String },
    SMCODE: { type: String },
    INVNAME: { type: String },
    TRNMODE: { type: String },
    TRNSTAT: { type: String },
    TD_BRANCH: { type: String },
    ISCTRNO: { type: String },
    TD_TRDT: { type: Date },
    TD_PRDT: { type: Date },
    TD_POP: { type: Number },
    TD_UNITS: { type: Number },
    TD_AMT: { type: Number },
    TD_APPNO: { type: String },
    UNQNO: { type: String },
    TRDESC: { type: String },
    TD_TRTYPE: { type: String },
    TD_NAV: { type: Number },
    IHNO: { type: String },
    BRANCHCODE: { type: String },
    PAN1: { type: String },
    NAVDATE: { type: Date },
    PAN2: { type: String },
    PAN3: { type: String },
    STATUS: { type: String },
    SCHEMEISIN: { type: String },
    PORTDT: { type: Date },
    NEWUNQNO: { type: String },
    EUIN: { type: String },
    ASSETTYPE: { type: String },
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

const members = new Schema({
    memberPan: { type: String },
    adminPan: { type: String },
    memberRelation: { type: String },
}, { versionKey: false });


  var transc2A = mongoose.model('trans_cams2A', transcams2A, 'trans_cams2A');
  var transc = mongoose.model('trans_cams', transcams, 'trans_cams');   
  var transk = mongoose.model('trans_karvy', transkarvy, 'trans_karvy'); 
  var transf = mongoose.model('trans_franklin', transfranklin, 'trans_franklin');  
  var folioc = mongoose.model('folio_cams', foliocams, 'folio_cams'); 
  var foliok = mongoose.model('folio_karvy', foliokarvy, 'folio_karvy');  
  var foliof = mongoose.model('folio_franklin', foliofranklin, 'folio_franklin');
  var camsn = mongoose.model('cams_nav', navcams, 'cams_nav');  
  var family = mongoose.model('familymember', members, 'familymember');
  var allcamsn = mongoose.model('allnav', navcams, 'allnav');
  var data="";var karvydata="";var camsdata="";var frankdata="";var datacon="";
var i=0;var resdata="";var foliokarvydata="";var foliocamsdata="";var foliofranklindata="";
var pipeline="";var pipeline1="";var pipeline2="";var pipeline3="";
var foliokarvydata="";var foliocamsdata="";var foliofranklindata="";

var temp22=0; var temp33 = 0;var cagr=0;


  const portfolioApisnapshot = (datacon, cb) => {
    const data = _.groupBy(datacon, "RTA");
    try {
        if (data.KARVY != undefined) {
            let match = data.KARVY.map((val) => {
                return { FMCODE: val.PRODCODE, PAN1: val.PAN, TD_ACNO: val.FOLIO }
            })
            const pipeline1 = [  //trans_karvy   
                { $match: { $or: match } },
                { $group: { _id: { TD_ACNO: "$TD_ACNO", FMCODE: "$FMCODE", FUNDDESC: "$FUNDDESC", TD_NAV: "$TD_NAV", TD_TRTYPE: "$TD_TRTYPE", NAVDATE: "$NAVDATE",  TD_UNITS: "$TD_UNITS", TD_AMT: "$TD_AMT", ASSETTYPE: "$ASSETTYPE", TD_TRNO: "$TD_TRNO", UNQNO: "$UNQNO" } } },
                { $lookup: { from: 'masterdata', localField: '_id.FMCODE', foreignField: 'SCH_CODE', as: 'master' } },
                { $unwind: "$master" },
                { $group: { _id: { TD_ACNO: "$_id.TD_ACNO",FMCODE: "$_id.FMCODE", FUNDDESC: "$_id.FUNDDESC", TD_NAV: "$_id.TD_NAV", TD_TRTYPE: "$_id.TD_TRTYPE", NAVDATE: "$_id.NAVDATE", ISIN: "$master.ISIN", AMFI: "$master.AMFI_Code", TD_UNITS: "$_id.TD_UNITS", TD_AMT: "$_id.TD_AMT", ASSETTYPE: "$_id.ASSETTYPE", TD_TRNO: "$_id.TD_TRNO", UNQNO:"$_id.UNQNO" } } },
                {
                    $lookup:
                    {
                        from: "allnav",
                        let: {isin: "$_id.ISIN", amfi:"$_id.AMFI"},
                        pipeline: [
                            { $match:  { $expr:  {  $or:  [ { $eq: ["$ISINDivPayoutISINGrowth", "$$isin"] } , { $eq: ["$SchemeCode", "$$amfi"] } ]}}},
                            { $project: { _id: 0, NetAssetValue: "$NetAssetValue", Date: "$Date" } },
                            { $sort: { Date: -1 } },
                            { $limit: 2 }
                        ],
                        as: "oldnav"
                    }
                },
               // { $unwind: "$oldnav" },
                { $project: { _id: 0, FOLIO: "$_id.TD_ACNO", PRODCODE: "$_id.FMCODE", SCHEME: "$_id.FUNDDESC", TD_NAV: "$_id.TD_NAV", NATURE: "$_id.TD_TRTYPE",newdate:"$_id.NAVDATE", TD_TRDT: { $dateToString: { format:"%d-%m-%Y", date: "$_id.NAVDATE" } },  ISIN: "$_id.SCHEMEISIN", cnav: "$oldnav.NetAssetValue", daybefore: "$oldnav.NetAssetValue", navdate: "$oldnav.Date", UNITS: "$_id.TD_UNITS", AMOUNT: "$_id.TD_AMT", TYPE: "$_id.ASSETTYPE", TRXNNO: "$_id.TD_TRNO", UNQNO: "$_id.UNQNO" ,RTA:"KARVY"} },
            ]
            transk.aggregate(pipeline1, (err, karvy) => {
                var datacon1 = karvy;
                for (var i = 0; i < datacon1.length; i++) {
                    if (datacon1[i]['NATURE'] === "Redemption" || datacon1[i]['NATURE'] === "RED" ||
                        datacon1[i]['NATURE'] === "SIPR" || datacon1[i]['NATURE'] === "Full Redemption" ||
                        datacon1[i]['NATURE'] === "Partial Redemption" || datacon1[i]['NATURE'] === "Lateral Shift Out" ||
                        datacon1[i]['NATURE'] === "Switchout" || datacon1[i]['NATURE'] === "Transfer-Out" ||
                        datacon1[i]['NATURE'] === "Transmission Out" || datacon1[i]['NATURE'] === "Switch Over Out" ||
                        datacon1[i]['NATURE'] === "LTOP" || datacon1[i]['NATURE'] === "LTOF" || datacon1[i]['NATURE'] === "FULR" || datacon1[i]['NATURE'] === "SWOP" ||
                        datacon1[i]['NATURE'] === "Partial Switch Out" || datacon1[i]['NATURE'] === "Full Switch Out" || datacon1[i]['NATURE'] === "FUL" ||
                        datacon1[i]['NATURE'] === "STPO" || datacon1[i]['NATURE'] === "SWOF" ||
                        datacon1[i]['NATURE'] === "SWD" || datacon1[i]['NATURE'] === "Full Switch Out") {
                        datacon1[i]['NATURE'] = "Switch Out";
                    }
                    if (datacon1[i]['NATURE'].match(/Systematic Investment.*/) ||
                        datacon1[i]['NATURE'] === "SIN" ||
                        datacon1[i]['NATURE'].match(/Systematic - Instalment.*/) ||
                        datacon1[i]['NATURE'].match(/Systematic - To.*/) ||
                        datacon1[i]['NATURE'].match(/Systematic-NSE.*/) ||
                        datacon1[i]['NATURE'].match(/Systematic Physical.*/) ||
                        datacon1[i]['NATURE'].match(/Systematic.*/) ||
                        datacon1[i]['NATURE'].match(/Systematic-Normal.*/) ||
                        datacon1[i]['NATURE'].match(/Systematic (ECS).*/)) {
                        datacon1[i]['NATURE'] = "SIP";
                    }
                    if (datacon1[i]['NATURE'] === "ADDPUR" || datacon1[i]['NATURE'] === "Additional Purchase" || datacon1[i]['NATURE'] === "NEW" || datacon1[i]['NATURE'] === "ADD" || datacon1[i]['NATURE'] === "Fresh Purchase") {
                        datacon1[i]['NATURE'] = "Purchase";
                    }
                    if (datacon1[i]['NATURE'] === "Lateral Shift In" || datacon1[i]['NATURE'] === "Switch-In"
                        || datacon1[i]['NATURE'] === "Transfer-In" || datacon1[i]['NATURE'] === "Switch Over In"
                        || datacon1[i]['NATURE'] === "LTIN" || datacon1[i]['NATURE'] === "LTIA") {
                        datacon1[i]['NATURE'] = "Switch In";
                    }
                    if (datacon1[i]['TYPE'] === "Balanced" || datacon1[i]['TYPE'] === "Equity(S)" || datacon1[i]['TYPE'] === "Equity(G)" || datacon1[i]['TYPE'] === "EQUITY FUND" || datacon1[i]['TYPE'] === "EQUITY FUN" || datacon1[i]['TYPE'] === "EQUITY-MF" || datacon1[i]['TYPE'] === "Balanced" || datacon1[i]['TYPE'] === "EQUITY" || datacon1[i]['TYPE'] === "Equity Fund" || datacon1[i]['TYPE'] === "Equity" || datacon1[i]['TYPE'] === "ELSS") {
                        datacon1[i]['TYPE'] = "EQUITY";
                    }
                    if (datacon1[i]['TYPE'] === "Gold FOF" || datacon1[i]['TYPE'] === "Arbitrage Fund") {
                        datacon1[i]['TYPE'] = "GOLD";
                    }
                    if (datacon1[i]['TYPE'] === "DEBT FUND" || datacon1[i]['TYPE'] === "LIQUID" || datacon1[i]['TYPE'] === "DEBT" || datacon1[i]['TYPE'] === "LIQUID FUND" || datacon1[i]['TYPE'] === "INCOME FUND" || datacon1[i]['TYPE'] === "GILT FUND" || datacon1[i]['TYPE'] === "Cash" || datacon1[i]['TYPE'] === "Bond" || datacon1[i]['TYPE'] === "Ultra Liquid" || datacon1[i]['TYPE'] === "Liquid Fund") {
                        datacon1[i]['TYPE'] = "DEBT";
                    }
                }
                datacon1 = datacon1.sort((a, b) => (a.SCHEME > b.SCHEME) ? 1 : -1);

                if (data.CAMS != undefined) {
                     let camsMatch = data.CAMS.map((val) => {
                        return { PRODCODE: val.PRODCODE, PAN: val.PAN, FOLIO_NO: val.FOLIO, }
                    })
                    const pipeline2 = [  //trans_cams
                        { $match: { $or: camsMatch } },
                        { $group: { _id: { FOLIO_NO: "$FOLIO_NO", SCHEME: "$SCHEME", PURPRICE: "$PURPRICE", TRXN_TYPE_: "$TRXN_TYPE_", TRADDATE: "$TRADDATE", PRODCODE: "$PRODCODE", UNITS: "$UNITS", AMOUNT: "$AMOUNT", SCHEME_TYP: "$SCHEME_TYP", TRXNNO: "$TRXNNO" } } },
                        { $lookup: { from: 'masterdata', localField: '_id.PRODCODE', foreignField: 'Merged', as: 'master' } },
                        { $unwind: "$master" },
                        { $group: { _id: { FOLIO_NO: "$_id.FOLIO_NO", SCHEME: "$_id.SCHEME", PURPRICE: "$_id.PURPRICE", TRXN_TYPE_: "$_id.TRXN_TYPE_", TRADDATE: "$_id.TRADDATE", ISIN: "$master.ISIN", AMFI:"$master.AMFI_Code", UNITS: "$_id.UNITS", AMOUNT: "$_id.AMOUNT", SCHEME_TYP: "$_id.SCHEME_TYP", TRXNNO: "$_id.TRXNNO", PRODCODE: "$_id.PRODCODE" } } },
                        {
                            $lookup:
                            {
                                from: "allnav",
                                let: { isin: "$_id.ISIN",amfi:"$_id.AMFI" },
                                pipeline: [
                                    { $match:  {$expr:{ $or: [  
                                        { $eq: ["$ISINDivPayoutISINGrowth", "$$isin"]  } ,
                                        { $eq: ["$SchemeCode", "$$amfi"]  } ] } } },
                                    { $project: { _id: 0, NetAssetValue: "$NetAssetValue", Date: "$Date" } },
                                    { $sort: { Date: -1 } },
                                    { $limit: 2 }
                                ],
                                as: "oldnav"
                            }
                        },
                      //  { $unwind: "$oldnav" },
                        { $project: { _id: 0, FOLIO: "$_id.FOLIO_NO", PRODCODE: "$_id.PRODCODE", SCHEME: "$_id.SCHEME", TD_NAV: "$_id.PURPRICE", newdate:"$_id.TRADDATE",NATURE: "$_id.TRXN_TYPE_", TD_TRDT: { $dateToString: { format: "%m/%d/%Y", date: "$_id.TRADDATE" } },  ISIN: "$master.ISIN", cnav: "$oldnav.NetAssetValue", daybefore: "$oldnav.NetAssetValue", navdate: "$oldnav.Date", UNITS: "$_id.UNITS", AMOUNT: "$_id.AMOUNT", TYPE: "$_id.SCHEME_TYP", TRXNNO: "$_id.TRXNNO",RTA:"CAMS" } },
                    ]
                    transc.aggregate(pipeline2, (err, cams) => {
                        cams = cams.sort((a, b) => new Date(a.TD_TRDT.split("-").reverse().join("/")).getTime() - new Date(b.TD_TRDT.split("-").reverse().join("/")).getTime());
                        const pipeline22 = [  //trans_cams2A
                            { $match: { $or: camsMatch } },
                            { $group: { _id: { FOLIO_NO: "$FOLIO_NO", SCHEME: "$SCHEME", PURPRICE: "$PURPRICE", TRXN_TYPE_: "$TRXN_TYPE_", TRADDATE: "$TRADDATE", PRODCODE: "$PRODCODE", UNITS: "$UNITS", AMOUNT: "$AMOUNT", SCHEME_TYP: "$SCHEME_TYP", TRXNNO: "$TRXNNO" } } },
                            { $lookup: { from: 'masterdata', localField: '_id.PRODCODE', foreignField: 'Merged', as: 'master' } },
                            { $unwind: "$master" },
                            { $group: { _id: { FOLIO_NO: "$_id.FOLIO_NO", SCHEME: "$_id.SCHEME", PURPRICE: "$_id.PURPRICE", TRXN_TYPE_: "$_id.TRXN_TYPE_", TRADDATE: "$_id.TRADDATE", ISIN: "$master.ISIN", AMFI:"$master.AMFI_Code", UNITS: "$_id.UNITS", AMOUNT: "$_id.AMOUNT", SCHEME_TYP: "$_id.SCHEME_TYP", TRXNNO: "$_id.TRXNNO", PRODCODE: "$_id.PRODCODE" } } },
                            {
                                $lookup:
                                {
                                    from: "allnav",
                                    let: { isin: "$_id.ISIN",amfi:"$_id.AMFI" },
                                    pipeline: [
                                        { $match:  {$expr:{ $or: [  
                                            { $eq: ["$ISINDivPayoutISINGrowth", "$$isin"]  } ,
                                            { $eq: ["$SchemeCode", "$$amfi"]  } ] } } },
                                        { $project: { _id: 0, NetAssetValue: "$NetAssetValue", Date: "$Date" } },
                                        { $sort: { Date: -1 } },
                                        { $limit: 2 }
                                    ],
                                    as: "oldnav"
                                }
                            },
                         //   { $unwind: "$oldnav" },
                            { $project: { _id: 0, FOLIO: "$_id.FOLIO_NO", PRODCODE: "$_id.PRODCODE", SCHEME: "$_id.SCHEME", TD_NAV: "$_id.PURPRICE", newdate:"$_id.TRADDATE",NATURE: "$_id.TRXN_TYPE_", TD_TRDT: { $dateToString: { format: "%m/%d/%Y", date: "$_id.TRADDATE" } },  ISIN: "$master.ISIN", cnav: "$oldnav.NetAssetValue", daybefore: "$oldnav.NetAssetValue", navdate: "$oldnav.Date", UNITS: "$_id.UNITS", AMOUNT: "$_id.AMOUNT", TYPE: "$_id.SCHEME_TYP", TRXNNO: "$_id.TRXNNO",RTA:"CAMS2A" } },
                        ]
                        transc2A.aggregate(pipeline22, (err, cams2a) => { 
                            cams2a = cams2a.sort((a, b) => new Date(a.TD_TRDT.split("-").reverse().join("/")).getTime() - new Date(b.TD_TRDT.split("-").reverse().join("/")).getTime());
                            var datacon2="";
                            if(cams2a != 0){
                            
                                datacon2 = cams2a.concat(cams)
                             }else{
                                 datacon2 = cams
                                }
                        for (var i = 0; i < datacon2.length; i++) {
                            if (datacon2[i]['NATURE'] === "Redemption" || datacon2[i]['NATURE'] === "R" || datacon2[i]['NATURE'] === "RED" ||
                                datacon2[i]['NATURE'] === "SIPR" || datacon2[i]['NATURE'] === "Full Redemption" ||
                                datacon2[i]['NATURE'] === "Partial Redemption" || datacon2[i]['NATURE'] === "Lateral Shift Out" ||
                                datacon2[i]['NATURE'] === "Switchout" || datacon2[i]['NATURE'] === "Transfer-Out" ||
                                datacon2[i]['NATURE'] === "Transmission Out" || datacon2[i]['NATURE'] === "Switch Over Out" || datacon2[i]['NATURE'] === "SWOP" ||
                                datacon2[i]['NATURE'] === "LTOP" || datacon2[i]['NATURE'] === "LTOF" || datacon2[i]['NATURE'] === "FULR" ||
                                datacon2[i]['NATURE'] === "Partial Switch Out" || datacon2[i]['NATURE'] === "Full Switch Out" || datacon2[i]['NATURE'] === "FUL" ||
                                datacon2[i]['NATURE'] === "STPO" || datacon2[i]['NATURE'] === "SWOF" ||
                                datacon2[i]['NATURE'] === "SWD" || datacon2[i]['NATURE'] === "Full Switch Out") {
                                datacon2[i]['NATURE'] = "Switch Out";
                            }
                            if (datacon2[i]['NATURE'].match(/Systematic Investment.*/) ||
                                datacon2[i]['NATURE'] === "SIN" ||
                                datacon2[i]['NATURE'].match(/Systematic - Instalment.*/) ||
                                datacon2[i]['NATURE'].match(/Systematic - To.*/) ||
                                datacon2[i]['NATURE'].match(/Systematic-NSE.*/) ||
                                datacon2[i]['NATURE'].match(/Systematic Physical.*/) ||
                                datacon2[i]['NATURE'].match(/Systematic.*/) ||
                                datacon2[i]['NATURE'].match(/Systematic-Normal.*/) ||
                                datacon2[i]['NATURE'].match(/Systematic (ECS).*/)) {
                                datacon2[i]['NATURE'] = "SIP";
                            }
                            if (datacon2[i]['NATURE'] === "ADDPUR" || datacon2[i]['NATURE'] === "Additional Purchase" || datacon2[i]['NATURE'] === "NEW" || datacon2[i]['NATURE'] === "ADD"|| datacon2[i]['NATURE'] === "Fresh Purchase") {
                                datacon2[i]['NATURE'] = "Purchase";
                            }
                            if (datacon2[i]['NATURE'] === "Lateral Shift In" || datacon2[i]['NATURE'] === "Switch-In"
                                || datacon2[i]['NATURE'] === "Transfer-In" || datacon2[i]['NATURE'] === "Switch Over In"
                                || datacon2[i]['NATURE'] === "LTIN" || datacon2[i]['NATURE'] === "LTIA") {
                                datacon2[i]['NATURE'] = "Switch In";
                            }
                            if (datacon2[i]['TYPE'] === "Balanced" || datacon2[i]['TYPE'] === "Equity(S)" || datacon2[i]['TYPE'] === "Equity(G)" || datacon2[i]['TYPE'] === "EQUITY FUND" || datacon2[i]['TYPE'] === "EQUITY FUN" || datacon2[i]['TYPE'] === "EQUITY-MF" || datacon2[i]['TYPE'] === "Balanced" || datacon2[i]['TYPE'] === "EQUITY" || datacon2[i]['TYPE'] === "Equity Fund" || datacon2[i]['TYPE'] === "Equity" || datacon2[i]['TYPE'] === "ELSS") {
                                datacon2[i]['TYPE'] = "EQUITY";
                            }
                            if (datacon2[i]['TYPE'] === "DEBT FUND" || datacon2[i]['TYPE'] === "LIQUID" || datacon2[i]['TYPE'] === "DEBT" || datacon2[i]['TYPE'] === "LIQUID FUND" || datacon2[i]['TYPE'] === "INCOME FUND" || datacon2[i]['TYPE'] === "GILT FUND" || datacon2[i]['TYPE'] === "Cash" || datacon2[i]['TYPE'] === "Bond" || datacon2[i]['TYPE'] === "Ultra Liquid"|| datacon2[i]['TYPE'] === "Liquid Fund") {
                                datacon2[i]['TYPE'] = "DEBT";
                            }
                            if (datacon2[i]['TYPE'] === "Gold FOF" || datacon2[i]['TYPE'] === "Arbitrage Fund") {
                                datacon2[i]['TYPE'] = "GOLD";
                            }
                        }
                        var datacon = datacon1.concat(datacon2)
                        datacon = datacon.sort((a, b) => (a.SCHEME > b.SCHEME) ? 1 : -1);
                        cb(datacon);
                 //   }
                  
                     });
                    });

                } else {
                    var datacon = datacon1;
                    datacon = datacon.sort((a, b) => (a.SCHEME > b.SCHEME) ? 1 : -1);
                    cb(datacon);
                }
            });
        } else {
            if (data.CAMS != undefined) {
                let camsMatch = data.CAMS.map((val) => {
                    return { PRODCODE: val.PRODCODE, PAN: val.PAN, FOLIO_NO: val.FOLIO }
                })
                const pipeline2 = [  //trans_cams
                    { $match: { $or: camsMatch } },
                    { $group: { _id: { FOLIO_NO: "$FOLIO_NO", SCHEME: "$SCHEME", PURPRICE: "$PURPRICE", TRXN_TYPE_: "$TRXN_TYPE_", TRADDATE: "$TRADDATE", PRODCODE: "$PRODCODE", UNITS: "$UNITS", AMOUNT: "$AMOUNT", SCHEME_TYP: "$SCHEME_TYP", TRXNNO: "$TRXNNO" } } },
                    { $lookup: { from: 'masterdata', localField: '_id.PRODCODE', foreignField: 'Merged', as: 'master' } },
                    { $unwind: "$master" },
                    { $group: { _id: { FOLIO_NO: "$_id.FOLIO_NO", SCHEME: "$_id.SCHEME", PURPRICE: "$_id.PURPRICE", TRXN_TYPE_: "$_id.TRXN_TYPE_", TRADDATE: "$_id.TRADDATE", ISIN: "$master.ISIN", AMFI:"$master.AMFI_Code", UNITS: "$_id.UNITS", AMOUNT: "$_id.AMOUNT", SCHEME_TYP: "$_id.SCHEME_TYP", TRXNNO: "$_id.TRXNNO", PRODCODE: "$_id.PRODCODE" } } },
                    {
                        $lookup:
                        {
                            from: "allnav",
                            let: { isin: "$_id.ISIN",amfi:"$_id.AMFI" },
                            pipeline: [
                                { $match:  {$expr:{ $or: [  
                                    { $eq: ["$ISINDivPayoutISINGrowth", "$$isin"]  } ,
                                    { $eq: ["$SchemeCode", "$$amfi"]  } ] } } },
                                { $project: { _id: 0, NetAssetValue: "$NetAssetValue", Date: "$Date" } },
                                { $sort: { Date: -1 } },
                                { $limit: 2 }
                            ],
                            as: "oldnav"
                        }
                    },
                   // { $unwind: "$oldnav" },
                    { $project: { _id: 0, FOLIO: "$_id.FOLIO_NO", PRODCODE: "$_id.PRODCODE", SCHEME: "$_id.SCHEME", TD_NAV: "$_id.PURPRICE", newdate:"$_id.TRADDATE",NATURE: "$_id.TRXN_TYPE_", TD_TRDT: { $dateToString: { format: "%m/%d/%Y", date: "$_id.TRADDATE" } },  ISIN: "$master.ISIN", cnav: "$oldnav.NetAssetValue", daybefore: "$oldnav.NetAssetValue", navdate: "$oldnav.Date", UNITS: "$_id.UNITS", AMOUNT: "$_id.AMOUNT", TYPE: "$_id.SCHEME_TYP", TRXNNO: "$_id.TRXNNO",RTA:"CAMS" } },
                ]
                transc.aggregate(pipeline2, (err, cams) => {
                    cams = cams.sort((a, b) => new Date(a.TD_TRDT.split("-").reverse().join("/")).getTime() - new Date(b.TD_TRDT.split("-").reverse().join("/")).getTime())
                    const pipeline22 = [  //trans_cams
                        { $match: { $or: camsMatch } },
                        { $group: { _id: { FOLIO_NO: "$FOLIO_NO", SCHEME: "$SCHEME", PURPRICE: "$PURPRICE", TRXN_TYPE_: "$TRXN_TYPE_", TRADDATE: "$TRADDATE", PRODCODE: "$PRODCODE", UNITS: "$UNITS", AMOUNT: "$AMOUNT", SCHEME_TYP: "$SCHEME_TYP", TRXNNO: "$TRXNNO" } } },
                        { $lookup: { from: 'masterdata', localField: '_id.PRODCODE', foreignField: 'Merged', as: 'master' } },
                        { $unwind: "$master" },
                        { $group: { _id: { FOLIO_NO: "$_id.FOLIO_NO", SCHEME: "$_id.SCHEME", PURPRICE: "$_id.PURPRICE", TRXN_TYPE_: "$_id.TRXN_TYPE_", TRADDATE: "$_id.TRADDATE", ISIN: "$master.ISIN", AMFI:"$master.AMFI_Code", UNITS: "$_id.UNITS", AMOUNT: "$_id.AMOUNT", SCHEME_TYP: "$_id.SCHEME_TYP", TRXNNO: "$_id.TRXNNO", PRODCODE: "$_id.PRODCODE" } } },
                        {
                            $lookup:
                            {
                                from: "allnav",
                                let: { isin: "$_id.ISIN",amfi:"$_id.AMFI" },
                                pipeline: [
                                    { $match:  {$expr:{ $or: [  
                                        { $eq: ["$ISINDivPayoutISINGrowth", "$$isin"]  } ,
                                        { $eq: ["$SchemeCode", "$$amfi"]  } ] } } },
                                    { $project: { _id: 0, NetAssetValue: "$NetAssetValue", Date: "$Date" } },
                                    { $sort: { Date: -1 } },
                                    { $limit: 2 }
                                ],
                                as: "oldnav"
                            }
                        },
                       // { $unwind: "$oldnav" },
                        { $project: { _id: 0, FOLIO: "$_id.FOLIO_NO", PRODCODE: "$_id.PRODCODE", SCHEME: "$_id.SCHEME", TD_NAV: "$_id.PURPRICE", newdate:"$_id.TRADDATE",NATURE: "$_id.TRXN_TYPE_", TD_TRDT: { $dateToString: { format: "%m/%d/%Y", date: "$_id.TRADDATE" } },  ISIN: "$master.ISIN", cnav: "$oldnav.NetAssetValue", daybefore: "$oldnav.NetAssetValue", navdate: "$oldnav.Date", UNITS: "$_id.UNITS", AMOUNT: "$_id.AMOUNT", TYPE: "$_id.SCHEME_TYP", TRXNNO: "$_id.TRXNNO",RTA:"CAMS2A" } },
                    ]
                    transc2A.aggregate(pipeline22, (err, cams2a) => {
                        cams2a = cams2a.sort((a, b) => new Date(a.TD_TRDT.split("-").reverse().join("/")).getTime() - new Date(b.TD_TRDT.split("-").reverse().join("/")).getTime())
                     var datacon2="";
                        if(cams2a != 0){
                           // cams.shift();
                            datacon2 = cams2a.concat(cams)
                         }else{
                             datacon2 = cams
                            }
                    for (var i = 0; i < datacon2.length; i++) {
                        if (datacon2[i]['NATURE'] === "Redemption" || datacon2[i]['NATURE'] === "R" || datacon2[i]['NATURE'] === "RED" ||
                            datacon2[i]['NATURE'] === "SIPR" || datacon2[i]['NATURE'] === "Full Redemption" ||
                            datacon2[i]['NATURE'] === "Partial Redemption" || datacon2[i]['NATURE'] === "Lateral Shift Out" ||
                            datacon2[i]['NATURE'] === "Switchout" || datacon2[i]['NATURE'] === "Transfer-Out" ||
                            datacon2[i]['NATURE'] === "Transmission Out" || datacon2[i]['NATURE'] === "Switch Over Out" || datacon2[i]['NATURE'] === "SWOP" ||
                            datacon2[i]['NATURE'] === "LTOP" || datacon2[i]['NATURE'] === "LTOF" || datacon2[i]['NATURE'] === "FULR" ||
                            datacon2[i]['NATURE'] === "Partial Switch Out" || datacon2[i]['NATURE'] === "Full Switch Out" || datacon2[i]['NATURE'] === "FUL" ||
                            datacon2[i]['NATURE'] === "STPO" || datacon2[i]['NATURE'] === "SWOF" ||
                            datacon2[i]['NATURE'] === "SWD" || datacon2[i]['NATURE'] === "Full Switch Out") {
                            datacon2[i]['NATURE'] = "Switch Out";
                        }
                        if (datacon2[i]['NATURE'].match(/Systematic Investment.*/) ||
                            datacon2[i]['NATURE'] === "SIN" ||
                            datacon2[i]['NATURE'].match(/Systematic - Instalment.*/) ||
                            datacon2[i]['NATURE'].match(/Systematic - To.*/) ||
                            datacon2[i]['NATURE'].match(/Systematic-NSE.*/) ||
                            datacon2[i]['NATURE'].match(/Systematic Physical.*/) ||
                            datacon2[i]['NATURE'].match(/Systematic.*/) ||
                            datacon2[i]['NATURE'].match(/Systematic-Normal.*/) ||
                            datacon2[i]['NATURE'].match(/Systematic (ECS).*/)) {
                            datacon2[i]['NATURE'] = "SIP";
                        }
                        if (datacon2[i]['NATURE'] === "ADDPUR" || datacon2[i]['NATURE'] === "Additional Purchase" || datacon2[i]['NATURE'] === "NEW" || datacon2[i]['NATURE'] === "ADD"|| datacon2[i]['NATURE'] === "Fresh Purchase") {
                            datacon2[i]['NATURE'] = "Purchase";
                        }
                        if (datacon2[i]['NATURE'] === "Lateral Shift In" || datacon2[i]['NATURE'] === "Switch-In"
                            || datacon2[i]['NATURE'] === "Transfer-In" || datacon2[i]['NATURE'] === "Switch Over In"
                            || datacon2[i]['NATURE'] === "LTIN" || datacon2[i]['NATURE'] === "LTIA") {
                            datacon2[i]['NATURE'] = "Switch In";
                        }
                        if (datacon2[i]['TYPE'] === "Balanced" || datacon2[i]['TYPE'] === "Equity(S)" || datacon2[i]['TYPE'] === "Equity(G)" || datacon2[i]['TYPE'] === "EQUITY FUND" || datacon2[i]['TYPE'] === "EQUITY FUN" || datacon2[i]['TYPE'] === "EQUITY-MF" || datacon2[i]['TYPE'] === "Balanced" || datacon2[i]['TYPE'] === "EQUITY" || datacon2[i]['TYPE'] === "Equity Fund" || datacon2[i]['TYPE'] === "Equity" || datacon2[i]['TYPE'] === "ELSS") {
                            datacon2[i]['TYPE'] = "EQUITY";
                        }
                        if (datacon2[i]['TYPE'] === "DEBT FUND" || datacon2[i]['TYPE'] === "LIQUID" || datacon2[i]['TYPE'] === "DEBT" || datacon2[i]['TYPE'] === "LIQUID FUND" || datacon2[i]['TYPE'] === "INCOME FUND" || datacon2[i]['TYPE'] === "GILT FUND" || datacon2[i]['TYPE'] === "Cash" || datacon2[i]['TYPE'] === "Bond" || datacon2[i]['TYPE'] === "Ultra Liquid" || datacon2[i]['TYPE'] === "Liquid Fund") {
                            datacon2[i]['TYPE'] = "DEBT";
                        }
                        if (datacon2[i]['TYPE'] === "Gold FOF" || datacon2[i]['TYPE'] === "Arbitrage Fund") {
                            datacon2[i]['TYPE'] = "GOLD";
                        }

                    }
                  //  datacon2 = datacon2.sort((a, b) => (a.SCHEME > b.SCHEME) ? 1 : -1);
                    var datacon = datacon2
                    datacon = datacon.sort((a, b) => (a.SCHEME > b.SCHEME) ? 1 : -1);
                    cb(datacon);
                });
            });
            }
        }
    } catch (err) {
        console.log(err)
    }

}
  
    const portfolioApiDetail = (datacon, cb) => {
    const data = _.groupBy(datacon, "RTA");
    try {
        if (data.KARVY != undefined) {
            let match = data.KARVY.map((val) => {
                return { FMCODE: val.PRODCODE, PAN1: val.PAN, TD_ACNO: val.FOLIO }
            })
            const pipeline1 = [  //trans_karvy   
                { $match: { $or: match } },
                { $group: { _id: { TD_ACNO: "$TD_ACNO",FMCODE:"$FMCODE",FUNDDESC: "$FUNDDESC", TD_NAV: "$TD_NAV", TD_TRTYPE: "$TD_TRTYPE", NAVDATE: "$NAVDATE", TD_UNITS:"$TD_UNITS" , TD_AMT: "$TD_AMT",ASSETTYPE:"$ASSETTYPE",TD_TRNO:"$TD_TRNO",UNQNO:"$UNQNO" } } },   
				  { $lookup: { from: 'masterdata', localField: '_id.FMCODE', foreignField: 'SCH_CODE', as: 'master' } },    
				  { $unwind: "$master" }, 
				  { $group: { _id: { TD_ACNO: "$_id.TD_ACNO",FMCODE:"$_id.FMCODE",FUNDDESC: "$_id.FUNDDESC", TD_NAV: "$_id.TD_NAV", TD_TRTYPE: "$_id.TD_TRTYPE", NAVDATE: "$_id.NAVDATE" , TD_UNITS:"$_id.TD_UNITS" , TD_AMT: "$_id.TD_AMT",ASSETTYPE:"$_id.ASSETTYPE",TD_TRNO:"$_id.TD_TRNO" ,UNQNO:"$_id.UNQNO",ISIN:"$master.ISIN",AMFI:"$master.AMFI_Code"} } }, 
                 {
                $lookup:
                {
                    from: "cams_nav",
                    let: { isin: "$_id.ISIN", amfi: "$_id.AMFI" },
                    pipeline: [
                        {
                            $match:
                            {
                                $expr:
                                {
                                    $or:
                                    [
                                        { $eq: ["$ISINDivPayoutISINGrowth", "$$isin"] },
                                        { $eq: ["$SchemeCode", "$$amfi"] }
                                    ]
                                }
                            }
                        },
                        { $project: { _id: 0 } }
                    ],
                    as: "nav"
                }
            },
             { $unwind: "$nav" },
              { $project: { _id: 0, FOLIO: "$_id.TD_ACNO",PRODCODE:"$_id.FMCODE", SCHEME: "$_id.FUNDDESC", TD_NAV: "$_id.TD_NAV", NATURE: "$_id.TD_TRTYPE",newdate:"$_id.NAVDATE", TD_TRDT: { $dateToString: { format: "%d-%m-%Y", date: "$_id.NAVDATE" } }, cnav: "$nav.NetAssetValue", navdate: "$nav.Date", UNITS: "$_id.TD_UNITS" , AMOUNT: "$_id.TD_AMT",TYPE:"$_id.ASSETTYPE",TRXNNO:"$_id.TD_TRNO",UNQNO:"$_id.UNQNO",RTA:"KARVY"}  },
            ]
            transk.aggregate(pipeline1, (err, karvy) => {
                var datacon1 = karvy;
                for (var i = 0; i < datacon1.length; i++) {
                    if (datacon1[i]['NATURE'] === "Redemption" || datacon1[i]['NATURE'] === "RED" ||
                        datacon1[i]['NATURE'] === "SIPR" || datacon1[i]['NATURE'] === "Full Redemption" ||
                        datacon1[i]['NATURE'] === "Partial Redemption" || datacon1[i]['NATURE'] === "Lateral Shift Out" ||
                        datacon1[i]['NATURE'] === "Switchout" || datacon1[i]['NATURE'] === "Transfer-Out" ||
                        datacon1[i]['NATURE'] === "Transmission Out" || datacon1[i]['NATURE'] === "Switch Over Out" ||
                        datacon1[i]['NATURE'] === "LTOP" || datacon1[i]['NATURE'] === "LTOF" || datacon1[i]['NATURE'] === "FULR" || datacon1[i]['NATURE'] === "SWOP" ||
                        datacon1[i]['NATURE'] === "Partial Switch Out" || datacon1[i]['NATURE'] === "Full Switch Out" || datacon1[i]['NATURE'] === "FUL" ||
                        datacon1[i]['NATURE'] === "STPO" || datacon1[i]['NATURE'] === "SWOF" ||
                        datacon1[i]['NATURE'] === "SWD" || datacon1[i]['NATURE'] === "Full Switch Out") {
                        datacon1[i]['NATURE'] = "Switch Out";
                    }
                    if (datacon1[i]['NATURE'].match(/Systematic Investment.*/) ||
                        datacon1[i]['NATURE'] === "SIN" ||
                        datacon1[i]['NATURE'].match(/Systematic - Instalment.*/) ||
                        datacon1[i]['NATURE'].match(/Systematic - To.*/) ||
                        datacon1[i]['NATURE'].match(/Systematic-NSE.*/) ||
                        datacon1[i]['NATURE'].match(/Systematic Physical.*/) ||
                        datacon1[i]['NATURE'].match(/Systematic.*/) ||
                        datacon1[i]['NATURE'].match(/Systematic-Normal.*/) ||
                        datacon1[i]['NATURE'].match(/Systematic (ECS).*/)) {
                        datacon1[i]['NATURE'] = "SIP";
                    }
                    if (datacon1[i]['NATURE'] === "ADDPUR" || datacon1[i]['NATURE'] === "Additional Purchase" || datacon1[i]['NATURE'] === "NEW" || datacon1[i]['NATURE'] === "ADD" || datacon1[i]['NATURE'] === "Fresh Purchase") {
                        datacon1[i]['NATURE'] = "Purchase";
                    }
                    if (datacon1[i]['NATURE'] === "Lateral Shift In" || datacon1[i]['NATURE'] === "Switch-In"
                        || datacon1[i]['NATURE'] === "Transfer-In" || datacon1[i]['NATURE'] === "Switch Over In"
                        || datacon1[i]['NATURE'] === "LTIN" || datacon1[i]['NATURE'] === "LTIA") {
                        datacon1[i]['NATURE'] = "Switch In";
                    }
                    if (datacon1[i]['TYPE'] === "Balanced" || datacon1[i]['TYPE'] === "Equity(S)" || datacon1[i]['TYPE'] === "Equity(G)" || datacon1[i]['TYPE'] === "EQUITY FUND" || datacon1[i]['TYPE'] === "EQUITY FUN" || datacon1[i]['TYPE'] === "EQUITY-MF" || datacon1[i]['TYPE'] === "Balanced" || datacon1[i]['TYPE'] === "EQUITY" || datacon1[i]['TYPE'] === "Equity Fund" || datacon1[i]['TYPE'] === "Equity" || datacon1[i]['TYPE'] === "ELSS") {
                        datacon1[i]['TYPE'] = "EQUITY";
                    }
                    if (datacon1[i]['TYPE'] === "Gold FOF" || datacon1[i]['TYPE'] === "Arbitrage Fund") {
                        datacon1[i]['TYPE'] = "GOLD";
                    }
                    if (datacon1[i]['TYPE'] === "DEBT FUND" || datacon1[i]['TYPE'] === "LIQUID" || datacon1[i]['TYPE'] === "DEBT" || datacon1[i]['TYPE'] === "LIQUID FUND" || datacon1[i]['TYPE'] === "INCOME FUND" || datacon1[i]['TYPE'] === "GILT FUND" || datacon1[i]['TYPE'] === "Cash" || datacon1[i]['TYPE'] === "Bond" || datacon1[i]['TYPE'] === "Ultra Liquid" || datacon1[i]['TYPE'] === "Liquid Fund") {
                        datacon1[i]['TYPE'] = "DEBT";
                    }

                }
                datacon1 = datacon1.sort((a, b) => (a.SCHEME > b.SCHEME) ? 1 : -1);

                if (data.CAMS != undefined) {
                    let camsMatch = data.CAMS.map((val) => {
                        return { PRODCODE: val.PRODCODE, PAN: val.PAN, FOLIO_NO: val.FOLIO, }
                    })
                    const pipeline2 = [  //trans_cams
                        { $match: {$or: camsMatch} },
                        { $group: { _id: { FOLIO_NO: "$FOLIO_NO", SCHEME: "$SCHEME", PURPRICE: "$PURPRICE", TRXN_TYPE_: "$TRXN_TYPE_", TRADDATE: "$TRADDATE", PRODCODE: "$PRODCODE",  UNITS: "$UNITS" , AMOUNT:  "$AMOUNT",SCHEME_TYP:"$SCHEME_TYP",TRXNNO:"$TRXNNO" }  } },
                        { $lookup: { from: 'masterdata', localField: '_id.PRODCODE', foreignField: 'Merged', as: 'master' } },  
                        { $unwind: "$master" }, 
                        { $group: { _id: { FOLIO_NO: "$_id.FOLIO_NO", SCHEME: "$_id.SCHEME", PURPRICE: "$_id.PURPRICE", TRXN_TYPE_: "$_id.TRXN_TYPE_", TRADDATE: "$_id.TRADDATE" , UNITS: "$_id.UNITS" , AMOUNT:  "$_id.AMOUNT",SCHEME_TYP:"$_id.SCHEME_TYP",TRXNNO:"$_id.TRXNNO",ISIN:"$master.ISIN",AMFI:"$master.AMFI_Code",PRODCODE: "$_id.PRODCODE" }  } },
                        {
                          $lookup:
                          {
                              from: "cams_nav",
                              let: { isin: "$_id.ISIN", amfi: "$_id.AMFI" },
                              pipeline: [
                                  {
                                      $match:
                                      {
                                          $expr:
                                          {
                                              $or:
                                              [
                                                  { $eq: ["$ISINDivPayoutISINGrowth", "$$isin"] },
                                                  { $eq: ["$SchemeCode", "$$amfi"] }
                                              ]
                                          }
                                      }
                                  },
                                  { $project: { _id: 0 } }
                              ],
                              as: "nav"
                          }
                      },
                      
                        { $unwind: "$nav" },
                        { $project: { _id: 0, FOLIO: "$_id.FOLIO_NO",PRODCODE:"$_id.PRODCODE", SCHEME: "$_id.SCHEME", TD_NAV: "$_id.PURPRICE", newdate:"$_id.TRADDATE",NATURE: "$_id.TRXN_TYPE_", TD_TRDT: { $dateToString: { format: "%m/%d/%Y", date: "$_id.TRADDATE" } }, cnav: "$nav.NetAssetValue", navdate:"$nav.Date", UNITS:"$_id.UNITS",AMOUNT: "$_id.AMOUNT",TYPE:"$_id.SCHEME_TYP",TRXNNO:"$_id.TRXNNO" ,RTA:"CAMS" } },
                    ]
                    transc.aggregate(pipeline2, (err, cams) => {
                        cams = cams.sort((a, b) => new Date(a.TD_TRDT.split("-").reverse().join("/")).getTime() - new Date(b.TD_TRDT.split("-").reverse().join("/")).getTime());
                        const pipeline22 = [  //trans_cams
                            { $match: {$or: camsMatch} },
                            { $group: { _id: { FOLIO_NO: "$FOLIO_NO", SCHEME: "$SCHEME", PURPRICE: "$PURPRICE", TRXN_TYPE_: "$TRXN_TYPE_", TRADDATE: "$TRADDATE", PRODCODE: "$PRODCODE",  UNITS: "$UNITS" , AMOUNT:  "$AMOUNT",SCHEME_TYP:"$SCHEME_TYP",TRXNNO:"$TRXNNO" }  } },
                            { $lookup: { from: 'masterdata', localField: '_id.PRODCODE', foreignField: 'Merged', as: 'master' } },  
                            { $unwind: "$master" }, 
                            { $group: { _id: { FOLIO_NO: "$_id.FOLIO_NO", SCHEME: "$_id.SCHEME", PURPRICE: "$_id.PURPRICE", TRXN_TYPE_: "$_id.TRXN_TYPE_", TRADDATE: "$_id.TRADDATE" , UNITS: "$_id.UNITS" , AMOUNT:  "$_id.AMOUNT",SCHEME_TYP:"$_id.SCHEME_TYP",TRXNNO:"$_id.TRXNNO",ISIN:"$master.ISIN",AMFI:"$master.AMFI_Code",PRODCODE: "$_id.PRODCODE" }  } },
                            {
                              $lookup:
                              {
                                  from: "cams_nav",
                                  let: { isin: "$_id.ISIN", amfi: "$_id.AMFI" },
                                  pipeline: [
                                      {
                                          $match:
                                          {
                                              $expr:
                                              {
                                                  $or:
                                                  [
                                                      { $eq: ["$ISINDivPayoutISINGrowth", "$$isin"] },
                                                      { $eq: ["$SchemeCode", "$$amfi"] }
                                                  ]
                                              }
                                          }
                                      },
                                      { $project: { _id: 0 } }
                                  ],
                                  as: "nav"
                              }
                          },
                          
                            { $unwind: "$nav" },
                            { $project: { _id: 0, FOLIO: "$_id.FOLIO_NO",PRODCODE:"$_id.PRODCODE", SCHEME: "$_id.SCHEME", TD_NAV: "$_id.PURPRICE", newdate:"$_id.TRADDATE",NATURE: "$_id.TRXN_TYPE_", TD_TRDT: { $dateToString: { format: "%m/%d/%Y", date: "$_id.TRADDATE" } }, cnav: "$nav.NetAssetValue", navdate:"$nav.Date", UNITS:"$_id.UNITS",AMOUNT: "$_id.AMOUNT",TYPE:"$_id.SCHEME_TYP",TRXNNO:"$_id.TRXNNO",RTA:"CAMS2A"  } },
                        ]
                       // console.log("line 387=",cams.length)
                                         
                        transc2A.aggregate(pipeline22, (err, cams2a) => { 
                     //       console.log("line 390=",cams2a.length)
                     cams2a = cams2a.sort((a, b) => new Date(a.TD_TRDT.split("-").reverse().join("/")).getTime() - new Date(b.TD_TRDT.split("-").reverse().join("/")).getTime());
                            if(cams2a != 0){
                            
                               datacon2 = cams2a.concat(cams)
                            }else{
                                datacon2 = cams
                               }
                               
                            //  for(var j=0;j<cams.length;j++){
                               
                            //     for(var k=0;k<cams2a.length;k++){
                            //         var camstemp = cams;
                            //         var cams2atemp = cams2a;
                            //         if(cams[j].FOLIO===cams2a[k].FOLIO && cams[j].PRODCODE===cams2a[k].PRODCODE){

                            //             console.log("cams2a[k].FOLIO=",cams2atemp[k].FOLIO)
                            //         }
                            //     }
                            //  }



                        for (var i = 0; i < datacon2.length; i++) {
                            if (datacon2[i]['NATURE'] === "Redemption" || datacon2[i]['NATURE'] === "R" || datacon2[i]['NATURE'] === "RED" ||
                                datacon2[i]['NATURE'] === "SIPR" || datacon2[i]['NATURE'] === "Full Redemption" ||
                                datacon2[i]['NATURE'] === "Partial Redemption" || datacon2[i]['NATURE'] === "Lateral Shift Out" ||
                                datacon2[i]['NATURE'] === "Switchout" || datacon2[i]['NATURE'] === "Transfer-Out" ||
                                datacon2[i]['NATURE'] === "Transmission Out" || datacon2[i]['NATURE'] === "Switch Over Out" || datacon2[i]['NATURE'] === "SWOP" ||
                                datacon2[i]['NATURE'] === "LTOP" || datacon2[i]['NATURE'] === "LTOF" || datacon2[i]['NATURE'] === "FULR" ||
                                datacon2[i]['NATURE'] === "Partial Switch Out" || datacon2[i]['NATURE'] === "Full Switch Out"  || datacon2[i]['NATURE'] === "FUL" ||
                                datacon2[i]['NATURE'] === "STPO" || datacon2[i]['NATURE'] === "SWOF" ||
                                datacon2[i]['NATURE'] === "SWD" || datacon2[i]['NATURE'] === "Full Switch Out") {
                                datacon2[i]['NATURE'] = "Switch Out";
                            }
                            if (datacon2[i]['NATURE'].match(/Systematic Investment.*/) ||
                                datacon2[i]['NATURE'] === "SIN" ||
                                datacon2[i]['NATURE'].match(/Systematic - Instalment.*/) ||
                                datacon2[i]['NATURE'].match(/Systematic - To.*/) ||
                                datacon2[i]['NATURE'].match(/Systematic-NSE.*/) ||
                                datacon2[i]['NATURE'].match(/Systematic Physical.*/) ||
                                datacon2[i]['NATURE'].match(/Systematic.*/) ||
                                datacon2[i]['NATURE'].match(/Systematic-Normal.*/) ||
                                datacon2[i]['NATURE'].match(/Systematic (ECS).*/)) {
                                datacon2[i]['NATURE'] = "SIP";
                            }
                            if (datacon2[i]['NATURE'] === "ADDPUR" || datacon2[i]['NATURE'] === "Additional Purchase" || datacon2[i]['NATURE'] === "NEW" || datacon2[i]['NATURE'] === "ADD"|| datacon2[i]['NATURE'] === "Fresh Purchase") {
                                datacon2[i]['NATURE'] = "Purchase";
                            }
                            if (datacon2[i]['NATURE'] === "Lateral Shift In" || datacon2[i]['NATURE'] === "Switch-In"
                                || datacon2[i]['NATURE'] === "Transfer-In" || datacon2[i]['NATURE'] === "Switch Over In"
                                || datacon2[i]['NATURE'] === "LTIN" || datacon2[i]['NATURE'] === "LTIA") {
                                datacon2[i]['NATURE'] = "Switch In";
                            }
                            if (datacon2[i]['TYPE'] === "Balanced" || datacon2[i]['TYPE'] === "Equity(S)" || datacon2[i]['TYPE'] === "Equity(G)" || datacon2[i]['TYPE'] === "EQUITY FUND" || datacon2[i]['TYPE'] === "EQUITY FUN" || datacon2[i]['TYPE'] === "EQUITY-MF" || datacon2[i]['TYPE'] === "Balanced" || datacon2[i]['TYPE'] === "EQUITY" || datacon2[i]['TYPE'] === "Equity Fund" || datacon2[i]['TYPE'] === "Equity" || datacon2[i]['TYPE'] === "ELSS") {
                                datacon2[i]['TYPE'] = "EQUITY";
                            }
                            if (datacon2[i]['TYPE'] === "DEBT FUND" || datacon2[i]['TYPE'] === "LIQUID" || datacon2[i]['TYPE'] === "DEBT" || datacon2[i]['TYPE'] === "LIQUID FUND" || datacon2[i]['TYPE'] === "INCOME FUND" || datacon2[i]['TYPE'] === "GILT FUND" || datacon2[i]['TYPE'] === "Cash" || datacon2[i]['TYPE'] === "Bond" || datacon2[i]['TYPE'] === "Ultra Liquid" || datacon2[i]['TYPE'] === "Liquid Fund") {
                                datacon2[i]['TYPE'] = "DEBT";
                            }
                            if (datacon2[i]['TYPE'] === "Gold FOF" || datacon2[i]['TYPE'] === "Arbitrage Fund") {
                                datacon2[i]['TYPE'] = "GOLD";
                            }
                        }
                        
                        var datacon = datacon1.concat(datacon2)
                        datacon = datacon.sort((a, b) => (a.SCHEME > b.SCHEME) ? 1 : -1);
                        cb(datacon);
                      });
                      
                    });

                } else {
                    var datacon = datacon1;
                    cb(datacon);
                }
            });
        } else {
            if (data.CAMS != undefined) {
                let camsMatch = data.CAMS.map((val) => {
                    return { PRODCODE: val.PRODCODE, PAN: val.PAN, FOLIO_NO: val.FOLIO, INV_NAME: { $regex: `^${val.NAME}.*`, $options: 'i' } }
                })
                const pipeline2 = [  //trans_cams
                    { $match: {$or: camsMatch} },
                    { $group: { _id: { FOLIO_NO: "$FOLIO_NO", SCHEME: "$SCHEME", PURPRICE: "$PURPRICE", TRXN_TYPE_: "$TRXN_TYPE_", TRADDATE: "$TRADDATE", PRODCODE: "$PRODCODE",  UNITS: "$UNITS" , AMOUNT:  "$AMOUNT",SCHEME_TYP:"$SCHEME_TYP",TRXNNO:"$TRXNNO" }  } },
                    { $lookup: { from: 'masterdata', localField: '_id.PRODCODE', foreignField: 'Merged', as: 'master' } },  
                    { $unwind: "$master" }, 
                    { $group: { _id: { FOLIO_NO: "$_id.FOLIO_NO", SCHEME: "$_id.SCHEME", PURPRICE: "$_id.PURPRICE", TRXN_TYPE_: "$_id.TRXN_TYPE_", TRADDATE: "$_id.TRADDATE" , UNITS: "$_id.UNITS" , AMOUNT:  "$_id.AMOUNT",SCHEME_TYP:"$_id.SCHEME_TYP",TRXNNO:"$_id.TRXNNO",ISIN:"$master.ISIN",AMFI:"$master.AMFI_Code",PRODCODE: "$_id.PRODCODE" }  } },
                    {
                      $lookup:
                      {
                          from: "cams_nav",
                          let: { isin: "$_id.ISIN", amfi: "$_id.AMFI" },
                          pipeline: [
                              {
                                  $match:
                                  {
                                      $expr:
                                      {
                                          $or:
                                          [
                                              { $eq: ["$ISINDivPayoutISINGrowth", "$$isin"] },
                                              { $eq: ["$SchemeCode", "$$amfi"] }
                                          ]
                                      }
                                  }
                              },
                              { $project: { _id: 0 } }
                          ],
                          as: "nav"
                      }
                  },
                  
                    { $unwind: "$nav" },
                    { $project: { _id: 0, FOLIO: "$_id.FOLIO_NO",PRODCODE:"$_id.PRODCODE", SCHEME: "$_id.SCHEME", TD_NAV: "$_id.PURPRICE", NATURE: "$_id.TRXN_TYPE_", TD_TRDT: { $dateToString: { format: "%m/%d/%Y", date: "$_id.TRADDATE" } },newdate:"$_id.TRADDATE", cnav: "$nav.NetAssetValue", navdate:"$nav.Date", UNITS:"$_id.UNITS",AMOUNT: "$_id.AMOUNT",TYPE:"$_id.SCHEME_TYP",TRXNNO:"$_id.TRXNNO" ,RTA:"CAMS" } },
                    
                ]
                transc.aggregate(pipeline2, (err, cams) => {
                    cams = cams.sort((a, b) => new Date(a.TD_TRDT.split("-").reverse().join("/")).getTime() - new Date(b.TD_TRDT.split("-").reverse().join("/")).getTime())
                    const pipeline22 = [  //trans_cams
                        { $match: {$or: camsMatch} },
                        { $group: { _id: { FOLIO_NO: "$FOLIO_NO", SCHEME: "$SCHEME", PURPRICE: "$PURPRICE", TRXN_TYPE_: "$TRXN_TYPE_", TRADDATE: "$TRADDATE", PRODCODE: "$PRODCODE",  UNITS: "$UNITS" , AMOUNT:  "$AMOUNT",SCHEME_TYP:"$SCHEME_TYP",TRXNNO:"$TRXNNO" }  } },
                        { $lookup: { from: 'masterdata', localField: '_id.PRODCODE', foreignField: 'Merged', as: 'master' } },  
                        { $unwind: "$master" }, 
                        { $group: { _id: { FOLIO_NO: "$_id.FOLIO_NO", SCHEME: "$_id.SCHEME", PURPRICE: "$_id.PURPRICE", TRXN_TYPE_: "$_id.TRXN_TYPE_", TRADDATE: "$_id.TRADDATE" , UNITS: "$_id.UNITS" , AMOUNT:  "$_id.AMOUNT",SCHEME_TYP:"$_id.SCHEME_TYP",TRXNNO:"$_id.TRXNNO",ISIN:"$master.ISIN",AMFI:"$master.AMFI_Code",PRODCODE: "$_id.PRODCODE" }  } },
                        {
                          $lookup:
                          {
                              from: "cams_nav",
                              let: { isin: "$_id.ISIN", amfi: "$_id.AMFI" },
                              pipeline: [
                                  {
                                      $match:
                                      {
                                          $expr:
                                          {
                                              $or:
                                              [
                                                  { $eq: ["$ISINDivPayoutISINGrowth", "$$isin"] },
                                                  { $eq: ["$SchemeCode", "$$amfi"] }
                                              ]
                                          }
                                      }
                                  },
                                  { $project: { _id: 0 } }
                              ],
                              as: "nav"
                          }
                      },
                      
                        { $unwind: "$nav" },
                        { $project: { _id: 0, FOLIO: "$_id.FOLIO_NO",PRODCODE:"$_id.PRODCODE", SCHEME: "$_id.SCHEME", TD_NAV: "$_id.PURPRICE", NATURE: "$_id.TRXN_TYPE_", TD_TRDT: { $dateToString: { format: "%m/%d/%Y", date: "$_id.TRADDATE" } },newdate:"$_id.TRADDATE", cnav: "$nav.NetAssetValue", navdate:"$nav.Date", UNITS:"$_id.UNITS",AMOUNT: "$_id.AMOUNT",TYPE:"$_id.SCHEME_TYP",TRXNNO:"$_id.TRXNNO",RTA:"CAMS2A"  } },
                        
                    ]
                    transc2A.aggregate(pipeline22, (err, cams2a) => {
                        cams2a = cams2a.sort((a, b) => new Date(a.TD_TRDT.split("-").reverse().join("/")).getTime() - new Date(b.TD_TRDT.split("-").reverse().join("/")).getTime())
                        if(cams2a != 0){
                           // cams.shift();
                            datacon2 = cams2a.concat(cams)
                         }else{
                             datacon2 = cams
                            }

                    for (var i = 0; i < datacon2.length; i++) {
                        if (datacon2[i]['NATURE'] === "Redemption" || datacon2[i]['NATURE'] === "R" || datacon2[i]['NATURE'] === "RED" ||
                            datacon2[i]['NATURE'] === "SIPR" || datacon2[i]['NATURE'] === "Full Redemption" ||
                            datacon2[i]['NATURE'] === "Partial Redemption" || datacon2[i]['NATURE'] === "Lateral Shift Out" ||
                            datacon2[i]['NATURE'] === "Switchout" || datacon2[i]['NATURE'] === "Transfer-Out" ||
                            datacon2[i]['NATURE'] === "Transmission Out" || datacon2[i]['NATURE'] === "Switch Over Out"  || datacon2[i]['NATURE'] === "SWOP" ||
                            datacon2[i]['NATURE'] === "LTOP" || datacon2[i]['NATURE'] === "LTOF" || datacon2[i]['NATURE'] === "FULR" ||
                            datacon2[i]['NATURE'] === "Partial Switch Out" || datacon2[i]['NATURE'] === "Full Switch Out"  || datacon2[i]['NATURE'] === "FUL" ||
                            datacon2[i]['NATURE'] === "STPO" || datacon2[i]['NATURE'] === "SWOF" ||
                            datacon2[i]['NATURE'] === "SWD" || datacon2[i]['NATURE'] === "Full Switch Out") {
                            datacon2[i]['NATURE'] = "Switch Out";
                        }
                        if (datacon2[i]['NATURE'].match(/Systematic Investment.*/) ||
                            datacon2[i]['NATURE'] === "SIN" ||
                            datacon2[i]['NATURE'].match(/Systematic - Instalment.*/) ||
                            datacon2[i]['NATURE'].match(/Systematic - To.*/) ||
                            datacon2[i]['NATURE'].match(/Systematic-NSE.*/) ||
                            datacon2[i]['NATURE'].match(/Systematic Physical.*/) ||
                            datacon2[i]['NATURE'].match(/Systematic.*/) ||
                            datacon2[i]['NATURE'].match(/Systematic-Normal.*/) ||
                            datacon2[i]['NATURE'].match(/Systematic (ECS).*/)) {
                            datacon2[i]['NATURE'] = "SIP";
                        }
                        if (datacon2[i]['NATURE'] === "ADDPUR" || datacon2[i]['NATURE'] === "Additional Purchase" || datacon2[i]['NATURE'] === "NEW" ||  datacon2[i]['NATURE'] === "ADD" || datacon2[i]['NATURE'] === "Fresh Purchase" ) {
                            datacon2[i]['NATURE'] = "Purchase";
                        }
                        if (datacon2[i]['NATURE'] === "Lateral Shift In" || datacon2[i]['NATURE'] === "Switch-In"
                            || datacon2[i]['NATURE'] === "Transfer-In" || datacon2[i]['NATURE'] === "Switch Over In"
                            || datacon2[i]['NATURE'] === "LTIN" || datacon2[i]['NATURE'] === "LTIA") {
                            datacon2[i]['NATURE'] = "Switch In";
                        }
                        if (datacon2[i]['TYPE'] === "Balanced" || datacon2[i]['TYPE'] === "Equity(S)" || datacon2[i]['TYPE'] === "Equity(G)" || datacon2[i]['TYPE'] === "EQUITY FUND" || datacon2[i]['TYPE'] === "EQUITY FUN" || datacon2[i]['TYPE'] === "EQUITY-MF" || datacon2[i]['TYPE'] === "Balanced" || datacon2[i]['TYPE'] === "EQUITY" || datacon2[i]['TYPE'] === "Equity Fund" || datacon2[i]['TYPE'] === "Equity" || datacon2[i]['TYPE'] === "ELSS") {
                            datacon2[i]['TYPE'] = "EQUITY";
                        }
                        if (datacon2[i]['TYPE'] === "DEBT FUND" || datacon2[i]['TYPE'] === "LIQUID" || datacon2[i]['TYPE'] === "DEBT" || datacon2[i]['TYPE'] === "LIQUID FUND" || datacon2[i]['TYPE'] === "INCOME FUND" || datacon2[i]['TYPE'] === "GILT FUND" || datacon2[i]['TYPE'] === "Cash" || datacon2[i]['TYPE'] === "Bond" || datacon2[i]['TYPE'] === "Ultra Liquid" || datacon2[i]['TYPE'] === "Liquid Fund") {
                            datacon2[i]['TYPE'] = "DEBT";
                        }
                        if (datacon2[i]['TYPE'] === "Gold FOF" || datacon2[i]['TYPE'] === "Arbitrage Fund") {
                            datacon2[i]['TYPE'] = "GOLD";
                        }

                    }
                    var datacon = datacon2;
                    datacon = datacon.sort((a, b) => (a.SCHEME > b.SCHEME) ? 1 : -1);
                    cb(datacon);
                 });
                });

            }
        }
    } catch (err) {
        console.log(err)
    }

}
    
app.post("/api/snapshot", function (req, res) {
	
    try {
        let regex = /^([a-zA-Z]){5}([0-9]){4}([a-zA-Z]){1}?$/;
        if (req.body.pan === "") {
            resdata = {
                status: 400,
                message: 'Please enter pan',
            }
            res.json(resdata);
            return resdata;
        } else if (!regex.test(req.body.pan)) {
            resdata = {
                status: 400,
                message: 'Please enter valid pan',
            }
            res.json(resdata);
            return resdata;
        } else {
            var resdata = ""; var arr1 = []; var arr2 = []; var arr3 = []; var panarray = [];
            var guardpan1 = []; var guardpan2 = [];
            family.find({ adminPan: { $regex: `^${req.body.pan}.*`, $options: 'i' } }, { _id: 0, memberPan: 1 }, function (err, member) {
                if (member != "") {
                    member = [...new Set(member.map(({ memberPan }) => memberPan.toUpperCase()))];
                    for (var j = 0; j < member.length; j++) {
                        panarray.push(member[j]);
                        guardpan1.push({ GUARD_PAN: member[j] });
                        guardpan2.push({ GUARDPANNO: member[j] });
                        arr1.push({ PAN: member[j] });
                        arr2.push({ PAN1: member[j] });
                    }
                    guardpan1.push({ GUARD_PAN: req.body.pan.toUpperCase() });
                    guardpan2.push({ GUARDPANNO: req.body.pan.toUpperCase() });
                    arr1.push({ PAN: req.body.pan.toUpperCase() });
                    arr2.push({ PAN1: req.body.pan.toUpperCase() });
                    panarray.push(req.body.pan);
                    var strPan1 = { $or: guardpan1 };
                    var strPan2 = { $or: guardpan2 };
                    folioc.find(strPan1).distinct("FOLIOCHK", function (err, member1) {
                        foliok.find(strPan2).distinct("ACNO", function (err, member2) {
                            var alldata = member1.concat(member2);
                            if (alldata.length != 0) {
                                panarray.push("");
                                for (var j = 0; j < alldata.length; j++) {
                                    arr1.push({ FOLIO_NO: alldata[j] });
                                    arr2.push({ TD_ACNO: alldata[j] });
                                }
                            }

                            var strFolio1 = { $or: arr1 };
                            var strFolio2 = { $or: arr2 };

                            pipeline1 = [  //trans_karvy
                                { $match: strFolio2 },
                                { $project: { _id: 0, NAME: "$INVNAME", PAN: "$PAN1", FOLIO: "$TD_ACNO", PRODCODE: "$FMCODE",SCHEME: "$FUNDDESC", RTA: "KARVY" } },
                             ]
                            pipeline2 = [  //trans_cams
                                { $match: strFolio1 },
                                { $project: { _id: 0, NAME: "$INV_NAME", PAN: "$PAN", FOLIO: "$FOLIO_NO", PRODCODE: "$PRODCODE",SCHEME: "$SCHEME", RTA: "CAMS" } },
                            ]
                            transk.aggregate(pipeline1, (err, data1) => {
                                transc.aggregate(pipeline2, (err, data2) => {
                                    if (data1 != 0 || data2 != 0) {
                                        resdata = {
                                            status: 200,
                                            message: 'Successfull',
                                            data: data2
                                        }
                                        let datacon = data1.concat(data2);
                                        datacon = datacon.filter(
                                            (temp => a =>
                                                (k => !temp[k] && (temp[k] = true))(a.PRODCODE + '|' + a.FOLIO)
                                            )(Object.create(null))
                                        );
                                        var dataarr = []; var lastarray = [];
                                        var finaldataarr=[];var hh=[];var gg=[];
                                        var newdataarr1 = [];var newdataarr2 = [];var newdataarr3 = [];
                                        datacon = datacon.sort((a, b) => (a.NAME > b.NAME) ? 1 : -1);
                                        portfolioApisnapshot(datacon, result => {
                                            lastarray.push(result);
                                            for (var j = 0; j < lastarray.length; j++) {
                                                for (var k = 0; k < lastarray[j].length; k++) {
                                                    dataarr.push(lastarray[j][k]);
                                                }
                                            }
                                            var amount = 0; var days = 0; var date1 = ""; var date2 = "";
                                            var arrdays = []; var alldays = []; var days = 0;
                                            var cnav = 0; var temp222 = 0; var finalarr = [];
                                            var navrate = 0; var finalsnapshotarr = [];
                                            var newsum1 = []; var newsum2 = []; var oldcnav = 0;

                                            if (dataarr != null && dataarr.length > 0) {
                                                for (var a = 0; a < datacon.length; a++) {
                                                    newdataarr1 =[];newdataarr2=[];newdataarr3=[];
                                                    dataarr = dataarr.sort((a, b) => new Date(a.TD_TRDT.split("-").reverse().join("/")).getTime() - new Date(b.TD_TRDT.split("-").reverse().join("/")).getTime());
                                                    for (var ii = 0; ii < dataarr.length; ii++) {
                                                         if (datacon[a].FOLIO === dataarr[ii].FOLIO && datacon[a].PRODCODE === dataarr[ii].PRODCODE) {
                                                         if(dataarr[ii].RTA === "CAMS"){
                                                                newdataarr1.push(dataarr[ii])
                                                          }else if(dataarr[ii].RTA === "CAMS2A"){
                                                                  newdataarr2.push(dataarr[ii])
                                                           }else{
                                                                  newdataarr3.push(dataarr[ii])
                                                           }
                                                         }
                                                        }
                                                      if(newdataarr2.length>0){
                                                            newdataarr1.shift();
                                                        }
                                                        hh = newdataarr2.concat(newdataarr1.concat(newdataarr3))
                                                        finaldataarr.push(hh);
                                                        
                                                }
                
                                                for (var j = 0; j < finaldataarr.length; j++) {
                                                    for (var k = 0; k < finaldataarr[j].length; k++) {
                                                        gg.push(finaldataarr[j][k]);
                                                    }
                                                }
                                              dataarr = gg;
                                                for (var c = 0; c < panarray.length; c++) {
                                                    let newarray = []; let oldarray = []; var purchase = []; var dayspurchase = [];
                                                    var name = "";
                                                    let cagrarray = []; let cagrsum1array = [];
                                                    let cagrsum2array = []; let finalsum1 = 0;
                                                    let finalsum2 = 0; var pan = "";
                                                    var equitytotal = 0; var debttotal = 0; var goldtotal = 0; var equitydebtgoldtotal = 0;
                                                    for (var a = 0; a < datacon.length; a++) {
                                                        var temp44 = 0; var temp444 = 0;
                                                        if (panarray[c] === datacon[a].PAN) {
                                                            name = datacon[a].NAME;
                                                            pan = panarray[c];
                                                            var unit = 0; var arrpurchase = []; var arrunit = [];var arrnav=[];var scheme="";
                                                            var temp4 = 0; var temp1, temp2 = 0; var temp3 = 0;var cnav = 0;
                                                            var cv = 0; var sum1 = []; var sum2 = [];
                                                            var temp222 = 0; var oldcv = 0;
                                                            dataarr = dataarr.sort((a, b) => new Date(a.TD_TRDT.split("-").reverse().join("/")).getTime() - new Date(b.TD_TRDT.split("-").reverse().join("/")).getTime());
                                                            var countlength = 0;var externelcount=0;
                                                            //loop for external counter
                                                            for (var ii = 0; ii < dataarr.length; ii++) {
                                                               if (datacon[a].FOLIO === dataarr[ii].FOLIO && datacon[a].PRODCODE === dataarr[ii].PRODCODE) {
                                                                   externelcount = externelcount+1;
                                                               }
                                                           }
                                                            for (var i = 0; i < dataarr.length; i++) {

                                                                var currentval = 0; var balance = 0;
                                                                var oldcurrentval = 0;
                                                                if (datacon[a].FOLIO === dataarr[i].FOLIO && datacon[a].PRODCODE === dataarr[i].PRODCODE) {
                                                                    countlength = countlength + 1;
                                                                    if (Math.sign(dataarr[i].UNITS) != -1) {
                                                                        if (dataarr[i].NATURE === "Switch Out")
                                                                            for (var jj = 0; jj < arrunit.length; jj++) {

                                                                                if (arrunit[jj] === 0)
                                                                                    arrunit.shift();
                                                                                if (arrpurchase[jj] === 0)
                                                                                    arrpurchase.shift();
                                                                                if (arrdays[jj] === 0)
                                                                                    arrdays.shift();
                                                                                if (alldays[jj] === 0)
                                                                                    alldays.shift();
                                                                                if (sum1[jj] === 0)
                                                                                    sum1.shift();
                                                                                if (sum2[jj] === 0)
                                                                                    sum2.shift();
                                                                                if (arrnav[jj] === 0)
                                                                                    arrnav.shift();
                                                                            }
                                                                    }
                                                                    cnav = dataarr[i].cnav[0];
                                                                    if (dataarr[i].NATURE != 'Switch Out' && dataarr[i].UNITS != 0) {

                                                                        unit = dataarr[i].UNITS
                                                                        amount = dataarr[i].AMOUNT;
                                                                        var date = dataarr[i].newdate;
                                                                        var navdate = dataarr[i].navdate[0];

                                                                        date1 = new Date(date);
                                                                        date2 = new Date(navdate);
                                                                        
                                                                        days = Math.round((date2-date1)/(1000*60*60*24));
                                                                        arrunit.push(dataarr[i].UNITS);
                                                                        if(dataarr[i].TD_NAV === 0){
                                                                            arrpurchase.push(0);
                                                                        }else{
                                                                            arrpurchase.push(dataarr[i].AMOUNT);
                                                                        }
                                                                        arrnav.push(dataarr[i].TD_NAV);
                                                                        var precagr = parseFloat( (parseFloat(Math.pow(parseFloat((cnav * dataarr[i].UNITS) / (dataarr[i].AMOUNT)), parseFloat(1 / parseFloat(days / 365)))) - 1) * 100) ;
                                                                        if(precagr=== Infinity){
                                                                            precagr=0;
                                                                        }else{
                                                                            precagr = precagr ;
                                                                        }
                                                                        //sum1(purchase cost*days*cagr)
                                                                        if (days === 0 && isNaN(days)) {
                                                                            sum1.push(0);
                                                                            arrdays.push(0);
                                                                            alldays.push(0);
                                                                            sum2.push(0);
                                                                        } else {
                                                                            arrdays.push(parseFloat(days) * dataarr[i].AMOUNT);

                                                                            alldays.push(parseFloat(days));

                                                                            sum1.push(parseFloat(dataarr[i].AMOUNT) * parseFloat(days) * parseFloat(precagr));

                                                                            sum2.push(parseFloat(dataarr[i].AMOUNT) * parseFloat(days));

                                                                        }

                                                                    } else {

                                                                        unit = "-" + dataarr[i].UNITS
                                                                        amount = "-" + dataarr[i].AMOUNT
                                                                         temp2 = dataarr[i].UNITS;
                                                                        for (var p = 0; p < arrunit.length; p++) {
                                                                            if(arrunit[p] === 0){
                                                                                continue;
                                                                                }
                                                                            temp3 = arrunit[p];
                                                                            if (temp4 === 0) {
                                                                                // console.log("if temp4 zero=", temp3 + "temp2=", temp2)
                                                                                temp4 = parseFloat(temp3) - parseFloat(temp2);
                                                                                temp4 = parseFloat(temp4.toFixed(4));
                                                                                // console.log("if temp4 calculated zero=", temp4)
                                                                                if (temp4 === 0) {
                                                                                    arrunit[p] = 0;
                                                                                    arrnav[p] = 0;
                                                                                    arrpurchase[p] = 0;
                                                                                    arrdays[p] = 0;
                                                                                    alldays[p] = 0;
                                                                                    sum1[p] = 0;
                                                                                    sum2[p] = 0;
                                                                                    break;
                                                                                } else if (temp4 > 0) {
                                                                                    arrunit[p] = temp4;
                                                                                    arrpurchase[p] = temp4 * arrnav[p];
                                                                                    arrdays[p] = parseFloat(alldays[p] * arrpurchase[p]);
                
                                                                                    sum1[p] = parseFloat(temp4 * arrnav[p]) * parseFloat(alldays[p]) * parseFloat((parseFloat(Math.pow(parseFloat((cnav * temp4) / (temp4 * arrnav[p])), parseFloat(1 / parseFloat(alldays[p] / 365)))) - 1) * 100);
                                                                                    sum2[p] = parseFloat(arrpurchase[p]) * parseFloat(alldays[p]);
                                                                                    // console.log("unit=", arrunit)
                                                                                    // console.log("purchase=", arrpurchase, dataarr[i].SCHEME)
                                                                                    break;
                                                                                } else {
                                                                                    arrunit[p] = 0;
                                                                                    arrnav[p] = 0;
                                                                                    arrpurchase[p] = 0;
                                                                                    arrdays[p] = 0;
                                                                                    alldays[p] = 0;
                                                                                    sum1[p] = 0;
                                                                                    sum2[p] = 0;
                                                                                }
                                                                            } else if (temp4 > 0) {//+ve
                                                                                // console.log("if temp4 +ve temp2=", temp4 + ",temp2=", temp2)
                                                                                // console.log("counter length =", countlength, externelcount,arrnav.length+1)
                                                                                if (countlength === arrnav.length+1) {
                                                                                //    console.log("temp4 +ve counter length =", countlength + ",i=", i, p, externelcount,arrnav.length+1)
                                                                                //    console.log("11235 line no", temp4 ,temp3)
                                                                                   
                                                                                    temp4 = parseFloat(temp4) + parseFloat(temp3);
                                                                                    temp4 = parseFloat(temp4.toFixed(4));
                                                                                //    console.log("11237 line no", temp4 ,temp3)
                                                                                //     console.log("11241 line=",arrunit)
                                                                                  
                                                                                }
                                                                                else {
                                                                                    temp4 = parseFloat(temp4) - parseFloat(temp2);
                                                                                    temp4 = parseFloat(temp4.toFixed(4));
                                                                                    // console.log("if temp4 +ve=", temp4)
                                                                                }
                
                                                                                if (temp4 === 0) {
                                                                                    arrunit[p] = 0;
                                                                                    arrnav[p] = 0;
                                                                                    arrpurchase[p] = 0;
                                                                                    arrdays[p] = 0;
                                                                                    alldays[p] = 0;
                                                                                    sum1[p] = 0;
                                                                                    sum2[p] = 0;
                                                                                    break;
                                                                                } else if (temp4 > 0) {
                                                                                    arrunit[p] = temp4;
                                                                                    //console.log("check nav +ve=",temp4,arrnav[p])
                                                                                    arrpurchase[p] = temp4 * arrnav[p];
                                                                                    arrdays[p] = parseFloat(alldays[p]) * arrpurchase[p];
                                                                                    sum1[p] = parseFloat(temp4 * arrnav[p]) * parseFloat(alldays[p]) * parseFloat((parseFloat(Math.pow(parseFloat((cnav * temp4) / (temp4 * arrnav[p])), parseFloat(1 / parseFloat(alldays[p] / 365)))) - 1) * 100);
                                                                                    sum2[p] = parseFloat(arrpurchase[p]) * parseFloat(alldays[p]);
                                                                                    // console.log("unit=", arrunit)
                                                                                    // console.log("purchase=", arrpurchase, dataarr[i].SCHEME)
                                                                                  //  console.log("10978 =", countlength, externelcount,i,p)
                                                                                    break;
                                                                                } else {
                                                                                    arrunit[p] = 0;
                                                                                    arrnav[p] = 0;
                                                                                    arrpurchase[p] = 0;
                                                                                    arrdays[p] = 0;
                                                                                    alldays[p] = 0;
                                                                                    sum1[p] = 0;
                                                                                    sum2[p] = 0;
                                                                                }
                                                                            } else {//-ve
                                                                                temp4 = parseFloat(temp4) + parseFloat(temp3);
                                                                                temp4 = parseFloat(temp4.toFixed(4));
                                                                             //  console.log("if temp4 -ve=", temp4)
                
                                                                                if (temp4 === 0) {
                                                                                    arrunit[p] = 0;
                                                                                    arrnav[p] = 0;
                                                                                    arrpurchase[p] = 0;
                                                                                    arrdays[p] = 0;
                                                                                    alldays[p] = 0;
                                                                                    sum1[p] = 0;
                                                                                    sum2[p] = 0;
                                                                                    // console.log("unit=", arrunit)
                                                                                    // console.log("purchase=", arrpurchase, dataarr[i].SCHEME)
                                                                                    break;
                                                                                } else if (temp4 > 0) {
                                                                                    arrunit[p] = temp4;
                                                                                 //   console.log("check nav +ve=",temp4,arrnav[p])
                                                                                    arrpurchase[p] = temp4 * arrnav[p];
                                                                                    arrdays[p] = parseFloat(alldays[p]) * arrpurchase[p];
                                                                                    sum1[p] = parseFloat(temp4 * arrnav[p]) * parseFloat(alldays[p]) * parseFloat((parseFloat(Math.pow(parseFloat((cnav * temp4) / (temp4 * arrnav[p])), parseFloat(1 / parseFloat(alldays[p] / 365)))) - 1) * 100);
                                                                                    sum2[p] = parseFloat(arrpurchase[p]) * parseFloat(alldays[p]);
                                                                                    // console.log("unit=", arrunit)
                                                                                    // console.log("purchase=", arrpurchase, dataarr[i].SCHEME)
                
                                                                                    // if (countlength === externelcount) {
                                                                                    //   // console.log("dataarr.length =", countlength + ",i=", i, p, externelcount,arrnav.length+1)
                                                                                    //     arrunit[p] = 0;
                                                                                    //     arrnav[p] = 0;
                                                                                    //     arrpurchase[p] = 0;
                                                                                    //     arrdays[p] = 0;
                                                                                    //     alldays[p] = 0;
                                                                                    //     sum1[p] = 0;
                                                                                    //     sum2[p] = 0;
                                                                                    // }
                                                                                    // else {
                                                                                   //     console.log("break dataarr.length =", countlength + ",i=", i, p, externelcount)
                                                                                        // arrunit[p] = 0;
                                                                                        // arrnav[p] = 0;
                                                                                        // arrpurchase[p] = 0;
                                                                                        // arrdays[p] = 0;
                                                                                        // alldays[p] = 0;
                                                                                        // sum1[p] = 0;
                                                                                        // sum2[p] = 0;
                                                                                        break;
                                                                                 //  }
                
                                                                                } else {
                                                                                    arrunit[p] = 0;
                                                                                    arrnav[p] = 0;
                                                                                    arrpurchase[p] = 0;
                                                                                    arrdays[p] = 0;
                                                                                    alldays[p] = 0;
                                                                                    sum1[p] = 0;
                                                                                    sum2[p] = 0;
                                                                                    // break;
                                                                                }
                                                                            }
                                                                        }
                                                                       

                                                                    }//else condition equal switch

                                                                    balance = parseFloat(unit) + parseFloat(balance);

                                                                    cnav = dataarr[i].cnav[0];
                                                                    oldcnav = dataarr[i].daybefore[1];
                                                                    if (cnav === "" || cnav === undefined || isNaN(balance) || isNaN(cnav)) {
                                                                        balance = 0;
                                                                        cnav = 0;
                                                                        oldcnav = 0;
                                                                    }
                                                                    currentval = cnav * balance
                                                                    oldcurrentval = oldcnav * balance
                                                                    if (dataarr[i].TYPE === "EQUITY") {
                                                                        equitytotal = parseFloat(currentval) + equitytotal;
                                                                    } else if (dataarr[i].TYPE === "DEBT") {
                                                                        debttotal = parseFloat(currentval) + debttotal;
                                                                    } else {
                                                                        goldtotal = parseFloat(currentval) + goldtotal;
                                                                    }

                                                                    cv = currentval + cv;
                                                                    cv= parseFloat(cv.toFixed(4));
                                                                    oldcv = oldcurrentval + oldcv;
                                                                    oldcv= parseFloat(oldcv.toFixed(4));
                                                                    scheme=  dataarr[i].SCHEME;
                                                                }//if match scheme & folio 

                                                            } //dataarr inner loop

                                                            var sum1all = 0; var sum2all = 0;

                                                            for (var kk = 0; kk < sum1.length; kk++) {
                                                                sum1all = sum1[kk] + sum1all;
                                                            }
                                                            for (var kkk = 0; kkk < sum2.length; kkk++) {
                                                                sum2all = sum2[kkk] + sum2all;
                                                            }
                                                            if (isNaN(sum2all) || (sum2all) === Infinity || isNaN(sum1all) || (sum1all) === Infinity) {
                                                                cagrsum2array.push(0);
                                                                cagrsum1array.push(0);
                                                            } else {
                                                                cagrsum2array.push(sum2all);
                                                                cagrsum1array.push(sum1all);
                                                            }

                                                            if (isNaN(cv) || cv < 0) {
                                                                newarray.push(0)
                                                                oldarray.push(0)
                                                            } else {
                                                                if(scheme != ""){
                                                                newarray.push(Math.round(cv))
                                                                oldarray.push(Math.round(oldcv))
                                                                }
                                                            }

                                                            temp22 = 0; temp33 = 0, temp222 = 0;

                                                            for (var k = 0; k < arrpurchase.length; k++) {
                                                                temp33 = arrpurchase[k];
                                                                temp22 = temp33 + temp22;
                                                                temp222 = Math.round(arrdays[k]) + temp222;
                                                            }
                                                            temp22 = parseFloat(temp22.toFixed(2));
                                                            if (temp22 > 0) {
                                                                purchase.push(temp22);
                                                            }
                                                            if (temp222 > 0) {
                                                                dayspurchase.push(temp222);
                                                            }
                                                        } // pan comparision


                                                    }
                                                    equitytotal = Math.round(equitytotal);
                                                    debttotal = Math.round(debttotal);
                                                    goldtotal = Math.round(goldtotal);
                                                    equitydebtgoldtotal = Math.round(parseFloat(equitytotal) + parseFloat(debttotal) + parseFloat(goldtotal));
                                                    sum1all = 0; sum2all = 0;
                                                    for (var kk = 0; kk < cagrsum1array.length; kk++) {
                                                        sum1all = cagrsum1array[kk] + sum1all;
                                                    }
                                                    for (var kkk = 0; kkk < cagrsum2array.length; kkk++) {
                                                        sum2all = cagrsum2array[kkk] + sum2all;
                                                    }
                                                    newsum1.push(sum1all);
                                                    newsum2.push(sum2all);
                                                    temp22 = 0; temp33 = 0;

                                                    for (var p = 0; p < cagrsum1array.length; p++) {
                                                        finalsum1 = cagrsum1array[p] + finalsum1;
                                                    }
                                                    for (var p = 0; p < cagrsum2array.length; p++) {
                                                        finalsum2 = cagrsum2array[p] + finalsum2;
                                                    }


                                                    cagr = finalsum1 / finalsum2;
                                                    sum1all = 0; sum2all = 0;
                                                    for (var p = 0; p < newsum1.length; p++) {
                                                        sum1all = newsum1[p] + sum1all;
                                                    }
                                                    for (var p = 0; p < newsum2.length; p++) {
                                                        sum2all = newsum2[p] + sum2all;
                                                    }


                                                    var finalcagr = sum1all / sum2all;

                                                    for (var p = 0; p < newarray.length; p++) {
                                                        temp44 = newarray[p] + temp44;
                                                        temp444 = oldarray[p] + temp444;
                                                    }
                                                    for (var k = 0; k < purchase.length; k++) {
                                                        temp33 = purchase[k];
                                                        temp22 = temp33 + temp22;

                                                    }
                                                 
                                                    temp22 = parseFloat(temp22.toFixed(2));
                                                    if (temp22 === 0 && temp44 === 0) {
                                                        equitytotal = 0;
                                                        debttotal = 0;
                                                        goldtotal = 0;
                                                        equitydebtgoldtotal = 0;
                                                        cagr = 0;
                                                        temp444 = 0;
                                                        finalarr.push({ name: name, purchasecost: temp22, currentvalue: Math.round(temp44), days_change: temp444, equitytotal: equitytotal, debttotal: debttotal, goldtotal: goldtotal, equitydebtgoldtotal: equitydebtgoldtotal, cagr: cagr.toFixed(1), pan: pan })
                                                        var pamt = 0; var mamt = 0; var camt = 0; var eamt = 0; var dayschangeamt = 0;
                                                        var damt = 0; var gamt = 0; var tamt = 0;
                                                        for (var p = 0; p < finalarr.length; p++) {
                                                            pamt = finalarr[p].purchasecost + pamt;
                                                            mamt = finalarr[p].currentvalue + mamt;
                                                            eamt = finalarr[p].equitytotal + eamt;
                                                            damt = finalarr[p].debttotal + damt;
                                                            gamt = finalarr[p].goldtotal + gamt;
                                                            dayschangeamt = finalarr[p].days_change + dayschangeamt;
                                                            tamt = finalarr[p].equitydebtgoldtotal + tamt;
                                                        }

                                                    } else {
                                                        finalarr.push({ name: name, purchasecost: temp22, currentvalue: Math.round(temp44), days_change: Math.round(temp44) - Math.round(temp444), equitytotal: equitytotal, debttotal: debttotal, goldtotal: goldtotal, equitydebtgoldtotal: equitydebtgoldtotal, cagr: cagr.toFixed(1), pan: pan })
                                                        var pamt = 0; var mamt = 0; var camt = 0; var eamt = 0;
                                                        var damt = 0; var gamt = 0; var tamt = 0;
                                                        var dayschangeamt = 0;
                                                        for (var p = 0; p < finalarr.length; p++) {
                                                            pamt = finalarr[p].purchasecost + pamt;
                                                            mamt = finalarr[p].currentvalue + mamt;
                                                            eamt = finalarr[p].equitytotal + eamt;
                                                            damt = finalarr[p].debttotal + damt;
                                                            gamt = finalarr[p].goldtotal + gamt;
                                                            dayschangeamt = finalarr[p].days_change + dayschangeamt;
                                                            tamt = finalarr[p].equitydebtgoldtotal + tamt;
                                                        }
                                                    }
                                                    mamt = parseFloat(mamt.toFixed(2));
                                                    pamt = parseFloat(pamt.toFixed(2));
                                                    if (finalarr.length === panarray.length) {
                                                        console.log("finalarr=", finalarr)
                                                    }
                                                    resdata = {
                                                        status: 200,
                                                        message: "Successfull",
                                                        data: temp22
                                                    }

                                                    if (finalarr.length === panarray.length) {

                                                        finalsnapshotarr.push({
                                                            purchasecost: pamt, currentvalue: mamt, gain: mamt - pamt,
                                                            dividend: 0, days_change: dayschangeamt, equity_perc: ((eamt / tamt) * 100).toFixed(1), debt_perc: ((damt / tamt) * 100).toFixed(1), gold_perc: ((gamt / tamt) * 100).toFixed(1), cagr: finalcagr.toFixed(2)
                                                        })
                                                        resdata.data = finalsnapshotarr[0];
                                                        res.json(resdata);

                                                    }

                                                }
                                            } else {
                                                console.log("purchase=", "Data Not Found!");
                                                resdata = {
                                                    status: 400,
                                                    message: 'Data not found',
                                                }
                                                res.json(resdata);
                                            }

                                        })

                                    } else {
                                        resdata = {
                                            status: 400,
                                            message: 'Data not found',
                                        }
                                        res.json(resdata);

                                    }
                                });
                            })
                        });
                    });
                } else {
                    var dataarr = []; var lastarray = []; let newarray = []; let oldarray = [];
                    let cagrsum1array = []; let cagrsum2array = []; let finalsum1 = 0; let finalsum2 = 0;
                   var finaldataarr=[];var hh=[];var gg=[];
                    var newdataarr1 = [];var newdataarr2 = [];var newdataarr3 = [];
                    pipeline1 = [  //trans_cams
                        { $match: { PAN: req.body.pan } },
                        { $project: { _id: 0, NAME: { "$toUpper": ["$INV_NAME"] }, PAN: "$PAN", FOLIO: "$FOLIO_NO",SCHEME: "$SCHEME", PRODCODE: "$PRODCODE", RTA: "CAMS" } }
                    ]
                    pipeline2 = [  //trans_karvy
                        { $match: { PAN1: req.body.pan } },
                        { $project: { _id: 0, NAME: { "$toUpper": ["$INVNAME"] }, PAN: "$PAN1", FOLIO: "$TD_ACNO",SCHEME: "$FUNDDESC", PRODCODE: "$FMCODE", RTA: "KARVY" } }
                    ]

                    transc.aggregate(pipeline1, (err, data1) => {
                        transk.aggregate(pipeline2, (err, data2) => {
                            if (data1.length != 0 || data2.length != 0) {
                                resdata = {
                                    status: 200,
                                    message: 'Successfull',
                                    data: data2
                                }
                                let datacon = data1.concat(data2);

                                datacon = datacon.filter(
                                    (temp => a =>
                                        (k => !temp[k] && (temp[k] = true))(a.PRODCODE + '|' + a.FOLIO)
                                    )(Object.create(null))
                                );

                                var uniquename = datacon.filter(
                                    (temp => a =>
                                        (k => !temp[k] && (temp[k] = true))(a.NAME + '|' + a.PAN)
                                    )(Object.create(null))
                                );
                                datacon = datacon.sort((a, b) => (a.SCHEME > b.SCHEME) ? 1 : -1);
                                var equitytotal = 0; var debttotal = 0; var goldtotal = 0; var equitydebtgoldtotal = 0;

                                portfolioApisnapshot(datacon, result => {
                                    lastarray.push(result);
                                    for (var j = 0; j < lastarray.length; j++) {
                                        for (var k = 0; k < lastarray[j].length; k++) {
                                            dataarr.push(lastarray[j][k]);
                                        }
                                    }
                                    //  console.log("dataarr",dataarr)
                                    var amount = 0; var days = 0; var date1 = ""; var date2 = "";
                                   
                                    var temp44 = 0;var purchase = []; 
                                    var cnav = 0; var temp222 = 0; var finalarr = [];
                                    var oldcnav = 0;

                                    if (dataarr != null && dataarr.length > 0) {
                                        var currentval = 0; var oldcurrentval = 0;
                                        for (var a = 0; a < datacon.length; a++) {
                                            newdataarr1 =[];newdataarr2=[];newdataarr3=[];
                                            dataarr = dataarr.sort((a, b) => new Date(a.TD_TRDT.split("-").reverse().join("/")).getTime() - new Date(b.TD_TRDT.split("-").reverse().join("/")).getTime());
                                            for (var ii = 0; ii < dataarr.length; ii++) {
                                                 if (datacon[a].FOLIO === dataarr[ii].FOLIO && datacon[a].PRODCODE === dataarr[ii].PRODCODE) {
                                                 if(dataarr[ii].RTA === "CAMS"){
                                                        newdataarr1.push(dataarr[ii])
                                                  }else if(dataarr[ii].RTA === "CAMS2A"){
                                                          newdataarr2.push(dataarr[ii])
                                                   }else{
                                                          newdataarr3.push(dataarr[ii])
                                                   }
                                                 }
                                                }
                                              if(newdataarr2.length>0){
                                                    newdataarr1.shift();
                                                }
                                                hh = newdataarr2.concat(newdataarr1.concat(newdataarr3))
                                                finaldataarr.push(hh);
                                                
                                        }
        
                                        for (var j = 0; j < finaldataarr.length; j++) {
                                            for (var k = 0; k < finaldataarr[j].length; k++) {
                                                gg.push(finaldataarr[j][k]);
                                            }
                                        }
                                      dataarr = gg;
                                        for (var a = 0; a < datacon.length; a++) {
                                            var unit = 0; var arrpurchase = []; var arrunit = [];
                                            var temp4 = 0; var temp1, temp2 = 0; var temp3 = 0;
                                            var cv = 0; var sum1 = []; var sum2 = [];
                                            var balance=0; var days=0;var arrdays = []; var alldays = []; var arrnav=[];var scheme=""; var cnav=0;
                                            var oldcv = 0;
                                            dataarr = dataarr.sort((a, b) => new Date(a.TD_TRDT.split("-").reverse().join("/")).getTime() - new Date(b.TD_TRDT.split("-").reverse().join("/")).getTime());
                                            var countlength = 0;var externelcount=0;
                                             //loop for external counter
                                             for (var ii = 0; ii < dataarr.length; ii++) {
                                                if (datacon[a].FOLIO === dataarr[ii].FOLIO && datacon[a].PRODCODE === dataarr[ii].PRODCODE) {
                                                    externelcount = externelcount+1;
                                                }
                                            }
                                            for (var i = 0; i < dataarr.length; i++) {
                                              //  var currentval = 0; var balance = 0;

                                              //  var oldcurrentval = 0;

                                             if (datacon[a].FOLIO === dataarr[i].FOLIO && datacon[a].PRODCODE === dataarr[i].PRODCODE) {

                                                    countlength = countlength + 1;
                                                    if (Math.sign(dataarr[i].UNITS) != -1) {
                                                        if (dataarr[i].NATURE === "Switch Out")
                                                            for (var jj = 0; jj < arrunit.length; jj++) {

                                                                if (arrunit[jj] === 0)
                                                                    arrunit.shift();
                                                                if (arrpurchase[jj] === 0)
                                                                    arrpurchase.shift();
                                                                if (arrdays[jj] === 0)
                                                                    arrdays.shift();
                                                                if (alldays[jj] === 0)
                                                                    alldays.shift();
                                                                if (sum1[jj] === 0)
                                                                    sum1.shift();
                                                                if (sum2[jj] === 0)
                                                                    sum2.shift();
                                                                if (arrnav[jj] === 0)
                                                                    arrnav.shift();
                                                            }
                                                    }
                                                   // cnav = dataarr[i].cnav[0];
                                                    cnav = dataarr[i].cnav[0];
                                                    oldcnav = dataarr[i].daybefore[1];
                                                    if (dataarr[i].NATURE != 'Switch Out' && dataarr[i].UNITS != 0) {

                                                        unit = dataarr[i].UNITS
                                                        amount = dataarr[i].AMOUNT;
                                                        var date = dataarr[i].newdate;
                                                        var navdate = dataarr[i].navdate[0];

                                                        date1 = new Date(date);
                                                        date2 = new Date(navdate);
                                                        
                                                         days = Math.round((date2-date1)/(1000*60*60*24));

                                                        arrunit.push(dataarr[i].UNITS);
                                                        if(dataarr[i].TD_NAV === 0){
                                                            arrpurchase.push(0);
                                                        }else{
                                                            arrpurchase.push(dataarr[i].AMOUNT);
                                                        }
                                                        arrnav.push(dataarr[i].TD_NAV)
                                                        //console.log("hello nav=",arrnav)
                                                        var precagr = parseFloat( (parseFloat(Math.pow(parseFloat((cnav * dataarr[i].UNITS) / (dataarr[i].AMOUNT)), parseFloat(1 / parseFloat(days / 365)))) - 1) * 100) ;
                                                            if(precagr=== Infinity){
                                                                precagr=0;
                                                            }else{
                                                                precagr = precagr ;
                                                            }
                                                        //sum1(purchase cost*days*cagr)
                                                        if (days === 0 && isNaN(days)) {
                                                            sum1.push(0);
                                                            arrdays.push(0);
                                                            alldays.push(0);
                                                            sum2.push(0);
                                                        } else {
                                                            arrdays.push(parseFloat(days) * parseFloat(dataarr[i].AMOUNT));

                                                            alldays.push(parseFloat(days));

                                                            sum1.push(parseFloat(dataarr[i].AMOUNT) * parseFloat(days) * parseFloat(precagr));

                                                            sum2.push(parseFloat(dataarr[i].AMOUNT) * parseFloat(days));

                                                        }

                                                    } else {

                                                        unit = "-" + dataarr[i].UNITS
                                                        amount = "-" + dataarr[i].AMOUNT
                                                        
                                                        temp2 = dataarr[i].UNITS;
                                                        for (var p = 0; p < arrunit.length; p++) {
                                                            if(arrunit[p] === 0){
                                                                continue;
                                                                }
                                                            temp3 = arrunit[p];
                                                            if (temp4 === 0) {
                                                                //   console.log("if temp4 zero line 8942=", temp3 + "temp2=", temp2)
                                                                 temp4 = parseFloat(temp3) - parseFloat(temp2);
                                                                 temp4 = parseFloat(temp4.toFixed(4));
                                                                //   console.log("if temp4 calculated zero 8945=", temp4)
                                                                
                                                                if (temp4 === 0) {
                                                                    arrunit[p] = 0;
                                                                    arrnav[p] = 0;
                                                                    arrpurchase[p] = 0;
                                                                    arrdays[p] = 0;
                                                                    alldays[p] = 0;
                                                                    sum1[p] = 0;
                                                                    sum2[p] = 0;
                                                                    break;
                                                                } else if (temp4 > 0) {
                                                                    arrunit[p] = temp4;
                                                                    arrpurchase[p] = temp4 * arrnav[p];
                                                                    arrdays[p] = parseFloat(alldays[p] * arrpurchase[p]);

                                                                    sum1[p] = parseFloat(temp4 * arrnav[p]) * parseFloat(alldays[p]) * parseFloat((parseFloat(Math.pow(parseFloat((cnav * temp4) / (temp4 * arrnav[p])), parseFloat(1 / parseFloat(alldays[p] / 365)))) - 1) * 100);
                                                                    sum2[p] = parseFloat(arrpurchase[p]) * parseFloat(alldays[p]);
                                                                   
                                                                    break;
                                                                } else {
                                                                    arrunit[p] = 0;
                                                                    arrnav[p] = 0;
                                                                    arrpurchase[p] = 0;
                                                                    arrdays[p] = 0;
                                                                    alldays[p] = 0;
                                                                    sum1[p] = 0;
                                                                    sum2[p] = 0;
                                                                }
                                                            } else if (temp4 > 0) {//+ve
                                                                
                                                                if (countlength === arrnav.length+1) {
                                                                 //   console.log("temp4 +ve counter length line 8977 =", countlength + ",i=", i, p, externelcount,arrnav.length+1)
                                                                 //  console.log("11235 line no", temp4 ,temp3)
                                                                   
                                                                    temp4 = parseFloat(temp4) + parseFloat(temp3);
                                                                    temp4 = parseFloat(temp4.toFixed(4));
                                                                   // console.log("11237 line no", temp4 ,temp3)
                                                                //     console.log("11241 line=",arrunit)
                                                                  
                                                                }
                                                                else {
                                                                   // console.log("temp2 >> if temp4 +ve line 8987=", temp4,temp2)
                                                                    temp4 = parseFloat(temp4) - parseFloat(temp2);
                                                                    temp4 = parseFloat(temp4.toFixed(4));
                                                                     //console.log("if temp4 +ve line 8989=", temp4)
                                                                }

                                                                if (temp4 === 0) {
                                                                    arrunit[p] = 0;
                                                                    arrnav[p] = 0;
                                                                    arrpurchase[p] = 0;
                                                                    arrdays[p] = 0;
                                                                    alldays[p] = 0;
                                                                    sum1[p] = 0;
                                                                    sum2[p] = 0;
                                                                    break;
                                                                } else if (temp4 > 0) {
                                                                    arrunit[p] = temp4;
                                                                    //console.log("line 9003 check nav +ve=",temp4,arrnav[p])
                                                                    arrpurchase[p] = temp4 * arrnav[p];
                                                                    arrdays[p] = parseFloat(alldays[p]) * arrpurchase[p];
                                                                    sum1[p] = parseFloat(temp4 * arrnav[p]) * parseFloat(alldays[p]) * parseFloat((parseFloat(Math.pow(parseFloat((cnav * temp4) / (temp4 * arrnav[p])), parseFloat(1 / parseFloat(alldays[p] / 365)))) - 1) * 100);
                                                                    sum2[p] = parseFloat(arrpurchase[p]) * parseFloat(alldays[p]);
                                                                   
                                                                    // if (countlength === externelcount) {
                                                                    //     //console.log(" line 9010 dataarr.length =", countlength + ",i=", i, p, externelcount,arrnav.length+1)
                                                                    //      arrunit[p] = 0;
                                                                    //      arrnav[p] = 0;
                                                                    //      arrpurchase[p] = 0;
                                                                    //      arrdays[p] = 0;
                                                                    //      alldays[p] = 0;
                                                                    //      sum1[p] = 0;
                                                                    //      sum2[p] = 0;
                                                                    //  }
                                                                    //  else {
                                                                         break;
                                                                   // }
                                                                } else {
                                                                    arrunit[p] = 0;
                                                                    arrnav[p] = 0;
                                                                    arrpurchase[p] = 0;
                                                                    arrdays[p] = 0;
                                                                    alldays[p] = 0;
                                                                    sum1[p] = 0;
                                                                    sum2[p] = 0;
                                                                    
                                                                }
                                                            } else {//-ve
                                                                // console.log("arrunit >>>>>>>>>line 9033=",arrunit)
                                                                // console.log("if temp4 -ve & temp3 line 9033=", temp4,temp3)
                                                                temp4 = parseFloat(temp4) + parseFloat(temp3);
                                                                temp4 = parseFloat(temp4.toFixed(4));
                                                            //    console.log("if temp4 -ve line 9036=", temp4)
                                                                if (temp4 === 0) {
                                                                    arrunit[p] = 0;
                                                                    arrnav[p] = 0;
                                                                    arrpurchase[p] = 0;
                                                                    arrdays[p] = 0;
                                                                    alldays[p] = 0;
                                                                    sum1[p] = 0;
                                                                    sum2[p] = 0;
                                                                    
                                                                    break;
                                                                } else if (temp4 > 0) {
                                                                    arrunit[p] = temp4;
                                                                    // console.log("check nav +ve line 9049=",temp4,arrnav[p])
                                                                    arrpurchase[p] = temp4 * arrnav[p];
                                                                    arrdays[p] = parseFloat(alldays[p]) * arrpurchase[p];
                                                                    sum1[p] = parseFloat(temp4 * arrnav[p]) * parseFloat(alldays[p]) * parseFloat((parseFloat(Math.pow(parseFloat((cnav * temp4) / (temp4 * arrnav[p])), parseFloat(1 / parseFloat(alldays[p] / 365)))) - 1) * 100);
                                                                    sum2[p] = parseFloat(arrpurchase[p]) * parseFloat(alldays[p]);
                                                                    
                                                                //     if (countlength === externelcount) {
                                                                //     //    console.log("dataarr.length line 9056 =", countlength + ",i=", i, p, externelcount,arrnav.length+1)
                                                                //         arrunit[p] = 0;
                                                                //         arrnav[p] = 0;
                                                                //         arrpurchase[p] = 0;
                                                                //         arrdays[p] = 0;
                                                                //         alldays[p] = 0;
                                                                //         sum1[p] = 0;
                                                                //         sum2[p] = 0;
                                                                //        // break;
                                                                //     }
                                                                //    else {
                                                                        break;
                                                                //   }

                                                                } else {
                                                                    arrunit[p] = 0;
                                                                    arrnav[p] = 0;
                                                                    arrpurchase[p] = 0;
                                                                    arrdays[p] = 0;
                                                                    alldays[p] = 0;
                                                                    sum1[p] = 0;
                                                                    sum2[p] = 0;
                                                                    // break;
                                                                }
                                                            }
                                                        }

                                                    }//else condition equal switch

                                                    balance = parseFloat(unit) + parseFloat(balance);

                                                    if (cnav === "" || cnav === undefined || isNaN(balance) || isNaN(cnav) || balance === 0) {
                                                        balance = 0;
                                                        cnav = 0;
                                                        currentval = 0;
                                                        oldcurrentval = 0;
                                                    }

                                                        currentval = Math.round(cnav * balance);
                                                        currentval = parseFloat(currentval.toFixed(4));
                                                        oldcurrentval = Math.round(oldcnav * balance);
                                                        oldcurrentval = parseFloat(oldcurrentval.toFixed(4));

                                                    if (dataarr[i].TYPE === "EQUITY") {
                                                        equitytotal = parseFloat(currentval) + equitytotal;
                                                    } else if (dataarr[i].TYPE === "DEBT") {
                                                        debttotal = parseFloat(currentval) + debttotal;
                                                    } else {
                                                        goldtotal = parseFloat(currentval) + goldtotal;
                                                    }

                                                    scheme=  dataarr[i].SCHEME;
                                                }//if match two scheme and folio array 
                                            } //dataarr inner loop
                                            balance = parseFloat(balance.toFixed(3))

                                            if (isNaN(currentval) || currentval < 0) {
                                                newarray.push(0)
                                                oldarray.push(0)
                                            } else {
                                                if(scheme != ""){
                                                    newarray.push(Math.round(currentval))
                                                    oldarray.push(Math.round(oldcurrentval))
                                                    }      
                                            }

                                            temp22 = 0; temp33 = 0

                                            for (var k = 0; k < arrpurchase.length; k++) {
                                                temp33 = arrpurchase[k];
                                                temp22 = temp33 + temp22;
                                                temp222 = Math.round(arrdays[k]) + temp222;
                                            }
                                            temp22 = parseFloat(temp22.toFixed(2));
                                            if(temp222 === 0 && temp22 === 0){
                                                days = 0;
                                            }else{
                                                days = temp222 / temp22;
                                            }
                                            console.log("ssssscv=",temp22, currentval, cv,scheme)
                                            if (currentval === 0 || balance === 0 || currentval === -0 || balance === -0) {
                                                temp22 = 0;
                                            }else{
                                                purchase.push(temp22);
                                            }

                                            var sum1all = 0; var sum2all = 0;

                                            for (var kk = 0; kk < sum1.length; kk++) {
                                                sum1all = sum1[kk] + sum1all;
                                            }
                                            for (var kkk = 0; kkk < sum2.length; kkk++) {
                                                sum2all = sum2[kkk] + sum2all;
                                            }


                                            if (isNaN(sum2all) || (sum2all) === Infinity || isNaN(sum1all) || (sum1all) === Infinity) {
                                                cagrsum2array.push(0);
                                                cagrsum1array.push(0);
                                            } else {
                                                cagrsum2array.push(sum2all);
                                                cagrsum1array.push(sum1all);
                                            }

                                        } // datascheme first loop
                                        equitytotal = Math.round(equitytotal);
                                        debttotal = Math.round(debttotal);
                                        equitydebtgoldtotal = parseFloat(equitytotal) + parseFloat(debttotal) + parseFloat(goldtotal);

                                        temp22 = 0; temp33 = 0
                                        for (var p = 0; p < cagrsum1array.length; p++) {
                                            finalsum1 = cagrsum1array[p] + finalsum1;
                                        }
                                        for (var p = 0; p < cagrsum2array.length; p++) {
                                            finalsum2 = cagrsum2array[p] + finalsum2;
                                        }
                                        cagr = finalsum1 / finalsum2;
                                        temp44 = 0; var temp444 = 0;

                                        for (var p = 0; p < newarray.length; p++) {
                                            temp44 = newarray[p] + temp44;
                                            temp444 = oldarray[p] + temp444;
                                        }
                                        for (var k = 0; k < purchase.length; k++) {
                                            temp33 = purchase[k];
                                            temp22 = temp33 + temp22;
                                        }
                                        temp22 = parseFloat(temp22.toFixed(2));
                                        resdata = {
                                            status: 200,
                                            message: "Successfull",
                                            data: temp22
                                        }
                                       
                                        if (temp22 != 0 && temp44 != 0) {
                                            gain = parseFloat(temp44.toFixed(2))- parseFloat(temp22.toFixed(2)) ;
                                            gain = parseFloat(gain.toFixed(2));
                                            finalarr.push({ name: uniquename[0].NAME, purchasecost: temp22, currentvalue: Math.round(temp44), gain: gain, dividend: 0, days_change: temp44 - temp444, cagr: parseFloat(cagr.toFixed(1)), equity_perc: ((equitytotal / equitydebtgoldtotal) * 100).toFixed(1), debt_perc: ((debttotal / equitydebtgoldtotal) * 100).toFixed(1), gold_perc: ((goldtotal / equitydebtgoldtotal) * 100).toFixed(1) })
                                        }
                                        
                                        console.log("purchase=", finalarr)

                                        resdata.data = finalarr[0];
                                        res.json(resdata);


                                    } else {
                                        console.log("purchase=", "Data Not Found!")
                                        resdata = {
                                            status: 400,
                                            message: 'Data not found',
                                        }
                                        res.json(resdata);
                                    }

                                })

                            } else {
                                resdata = {
                                    status: 400,
                                    message: 'Data not found',
                                }
                                res.json(resdata);
                            }
                        });
                    })
                }
            });
        }
    } catch (err) {
        console.log(err)
    }
})


app.post("/api/portfolio_api_data", function (req, res) {
    try {
        let regex = /^([a-zA-Z]){5}([0-9]){4}([a-zA-Z]){1}?$/;
        if (req.body.pan === "") {
            resdata = {
                status: 400,
                message: 'Please enter pan',
            }
            res.json(resdata);
            return resdata;
        } else if (!regex.test(req.body.pan)) {
            resdata = {
                status: 400,
                message: 'Please enter valid pan',
            }
            res.json(resdata);
            return resdata;
        } else {
            var resdata = ""; var arr1 = []; var arr2 = []; var arr3 = []; var panarray = [];
            var guardpan1 = []; var guardpan2 = [];
            family.find({ adminPan: { $regex: `^${req.body.pan}.*`, $options: 'i' } }, { _id: 0, memberPan: 1 }, function (err, member) {
                if (member != "") {
                    member = [...new Set(member.map(({ memberPan }) => memberPan.toUpperCase()))];
                    for (var j = 0; j < member.length; j++) {
                        panarray.push(member[j])
                        guardpan1.push({ GUARD_PAN: member[j] });//folio_cams
                        guardpan2.push({ GUARDPANNO: member[j] });//folio_karvy
                        arr1.push({ PAN: member[j] });
                        arr2.push({ PAN1: member[j] });

                    }
                    guardpan1.push({ GUARD_PAN: req.body.pan.toUpperCase() });//folio_cams
                    guardpan2.push({ GUARDPANNO: req.body.pan.toUpperCase() });//folio_karvy
                    arr1.push({ PAN: req.body.pan.toUpperCase() });
                    arr2.push({ PAN1: req.body.pan.toUpperCase() });
                    panarray.push(req.body.pan);
                    var strPan = { $or: guardpan1 };
                    var strPan1 = { $or: guardpan2 };
                    folioc.find(strPan).distinct("FOLIOCHK", function (err, member1) {
                        foliok.find(strPan1).distinct("ACNO", function (err, member2) {
                            var alldata = member1.concat(member2);
                            if (alldata.length != 0) {
                                panarray.push("");
                                for (var j = 0; j < alldata.length; j++) {
                                    arr1.push({ FOLIO_NO: alldata[j] });
                                    arr2.push({ TD_ACNO: alldata[j] });
                                }
                            }

                            var strFolio1 = { $or: arr1 };
                            var strFolio2 = { $or: arr2 };
                            pipeline1 = [  //trans_karvy
                                { $match: strFolio2 },
                                { $project: { _id: 0, NAME: "$INVNAME", PAN: "$PAN1", PRODCODE: "$FMCODE" ,SCHEME: "$FUNDDESC", FOLIO: "$TD_ACNO", RTA: "KARVY" } },
                            ]
                            pipeline2 = [  //trans_cams
                                { $match: strFolio1 },
                                { $project: { _id: 0, NAME: "$INV_NAME", PAN: "$PAN", PRODCODE: "$PRODCODE" ,SCHEME: "$SCHEME", FOLIO: "$FOLIO_NO", RTA: "CAMS" } },
                            ]
                            transk.aggregate(pipeline1, (err, data1) => {
                                transc.aggregate(pipeline2, (err, data2) => {

                                    if (data1.length != 0 || data2.length != 0) {
                                        resdata = {
                                            status: 200,
                                            message: 'Successfull',
                                            data: data2
                                        }
                                        let datacon = data1.concat(data2);

                                       
                                        datacon = datacon.filter(
                                            (temp => a =>
                                                (k => !temp[k] && (temp[k] = true))(a.PRODCODE + '|' + a.FOLIO)
                                            )(Object.create(null))
                                        );
                                        var dataarr = []; var lastarray = [];
                                        var finaldataarr=[];var hh=[];var gg=[];
                                        var newdataarr1 = [];var newdataarr2 = [];var newdataarr3 = [];
                                        datacon = datacon.sort((a, b) => (a.SCHEME > b.SCHEME) ? 1 : -1);

                                        portfolioApiDetail(datacon, result => {
                                            lastarray.push(result);
                                            for (var j = 0; j < lastarray.length; j++) {
                                                for (var k = 0; k < lastarray[j].length; k++) {
                                                    dataarr.push(lastarray[j][k]);
                                                }
                                            }
                                            var amount = 0; var days = 0; var date1 = ""; var date2 = "";
                                            var arrdays = []; var alldays = [];
                                            var cnav = 0; var temp222 = 0; var finalarr = [];
                                            var navrate = 0;

                                            if (dataarr != null && dataarr.length > 0) {
                                                for (var a = 0; a < datacon.length; a++) {
                                                    newdataarr1 =[];newdataarr2=[];newdataarr3=[];
                                                    dataarr = dataarr.sort((a, b) => new Date(a.TD_TRDT.split("-").reverse().join("/")).getTime() - new Date(b.TD_TRDT.split("-").reverse().join("/")).getTime());
                                                    for (var ii = 0; ii < dataarr.length; ii++) {
                                                         if (datacon[a].FOLIO === dataarr[ii].FOLIO && datacon[a].PRODCODE === dataarr[ii].PRODCODE) {
                                                         if(dataarr[ii].RTA === "CAMS"){
                                                                newdataarr1.push(dataarr[ii])
                                                          }else if(dataarr[ii].RTA === "CAMS2A"){
                                                                  newdataarr2.push(dataarr[ii])
                                                           }else{
                                                                  newdataarr3.push(dataarr[ii])
                                                           }
                                                         }
                                                        }
                                                      if(newdataarr2.length>0){
                                                            newdataarr1.shift();
                                                        }
                                                        hh = newdataarr2.concat(newdataarr1.concat(newdataarr3))
                                                        finaldataarr.push(hh);
                                                        
                                                }
                
                                                for (var j = 0; j < finaldataarr.length; j++) {
                                                    for (var k = 0; k < finaldataarr[j].length; k++) {
                                                        gg.push(finaldataarr[j][k]);
                                                    }
                                                }
                                              dataarr = gg;
                                                for (var c = 0; c < panarray.length; c++) {
                                                    let newarray = []; var purchase = []; var name = "";
                                                    let cagrarray = []; let cagrsum1array = [];
                                                    let cagrsum2array = []; let finalsum1 = 0;
                                                    let finalsum2 = 0; var pan = "";
                                                    
                                                    for (var a = 0; a < datacon.length; a++) {
                                                        var temp44 = 0;
                                                        if (panarray[c] === datacon[a].PAN) {
                                                            name = datacon[a].NAME;
                                                            pan = panarray[c];
                                                            var unit = 0; var arrpurchase = []; var arrunit = [];var arrnav=[];var scheme="";
                                                            var temp4 = 0; var temp1, temp2 = 0; var temp3 = 0;var cnav = 0;
                                                            var cv = 0; var sum1 = []; var sum2 = [];

                                                            dataarr = dataarr.sort((a, b) => new Date(a.TD_TRDT.split("-").reverse().join("/")).getTime() - new Date(b.TD_TRDT.split("-").reverse().join("/")).getTime());
                                                            var countlength = 0;var externelcount=0;
                                                            //loop for external counter
                                                            for (var ii = 0; ii < dataarr.length; ii++) {
                                                               if (datacon[a].FOLIO === dataarr[ii].FOLIO && datacon[a].PRODCODE === dataarr[ii].PRODCODE) {
                                                                   externelcount = externelcount+1;
                                                               }
                                                           }
                                                            for (var i = 0; i < dataarr.length; i++) {
                                                                var currentval = 0; var balance = 0;

                                                                if (datacon[a].FOLIO === dataarr[i].FOLIO && datacon[a].PRODCODE === dataarr[i].PRODCODE) {
                                                                    countlength = countlength + 1;
                                                                    if (Math.sign(dataarr[i].UNITS) != -1) {
                                                                        if (dataarr[i].NATURE === "Switch Out")
                                                                            for (var jj = 0; jj < arrunit.length; jj++) {

                                                                                if (arrunit[jj] === 0)
                                                                                    arrunit.shift();
                                                                                if (arrpurchase[jj] === 0)
                                                                                    arrpurchase.shift();
                                                                                if (arrdays[jj] === 0)
                                                                                    arrdays.shift();
                                                                                if (alldays[jj] === 0)
                                                                                    alldays.shift();
                                                                                if (sum1[jj] === 0)
                                                                                    sum1.shift();
                                                                                if (sum2[jj] === 0)
                                                                                    sum2.shift();
                                                                                if (arrnav[jj] === 0)
                                                                                    arrnav.shift();
                                                                            }
                                                                    }
                                                                    cnav = dataarr[i].cnav;
                                                                    if (dataarr[i].NATURE != 'Switch Out' && dataarr[i].UNITS != 0) {

                                                                        unit = dataarr[i].UNITS
                                                                        amount = dataarr[i].AMOUNT;
                                                                        var date = dataarr[i].TD_TRDT;
                                                                        var navdate = dataarr[i].navdate;

                                                                        var d = new Date(date.split("-").reverse().join("-"));
                                                                        var dd = d.getDate();
                                                                        var mm = d.getMonth() + 1;
                                                                        var yy = d.getFullYear();
                                                                        var newdate = mm + "/" + dd + "/" + yy;


                                                                        var navd = new Date(navdate);
                                                                        var navdd = navd.getDate();
                                                                        var navmm = navd.getMonth() + 1;
                                                                        var navyy = navd.getFullYear();
                                                                        var newnavdate = navmm + "/" + navdd + "/" + navyy;
                                                                        date1 = new Date(newdate);
                                                                        date2 = new Date(newnavdate);
                                                                        days = moment(date2).diff(moment(date1), 'days');

                                                                        arrunit.push(dataarr[i].UNITS);
                                                                        
                                                                        if(dataarr[i].TD_NAV === 0){
                                                                            arrpurchase.push(0);
                                                                        }else{
                                                                            arrpurchase.push(dataarr[i].AMOUNT);
                                                                        }
                                                                        arrnav.push(dataarr[i].TD_NAV);
                                                                        
                                                                        var precagr = parseFloat( (parseFloat(Math.pow(parseFloat((cnav * dataarr[i].UNITS) / (dataarr[i].AMOUNT)), parseFloat(1 / parseFloat(days / 365)))) - 1) * 100) ;
                                                                        if(precagr=== Infinity){
                                                                            precagr=0;
                                                                        }else{
                                                                            precagr = precagr ;
                                                                        }
                                                                        //sum1(purchase cost*days*cagr)
                                                                        if (days === 0 && isNaN(days)) {
                                                                            sum1.push(0);
                                                                            arrdays.push(0);
                                                                            alldays.push(0);
                                                                            sum2.push(0);
                                                                        } else {
                                                                           
                                                                            arrdays.push(parseFloat(days) * (parseFloat(dataarr[i].AMOUNT)));

                                                                            alldays.push(parseFloat(days));
                                                                            sum1.push(parseFloat(dataarr[i].AMOUNT) * parseFloat(days) * parseFloat(precagr));

                                                                            sum2.push(parseFloat(dataarr[i].AMOUNT) * parseFloat(days));

                                                                        }

                                                                        // temp1 = dataarr[i].UNITS;
                                                                        // temp2 = temp1 + temp2;
                                                                        // navrate = dataarr[i].TD_NAV;


                                                                    } else {

                                                                        unit = "-" + dataarr[i].UNITS
                                                                        amount = "-" + dataarr[i].AMOUNT
                                                                        // if (temp4 != "" && temp4 != 0) {
                                                                        //     arrunit.splice(0, 0, temp4);
                                                                        // }
                                                                        temp2 = dataarr[i].UNITS;
                                                                        for (var p = 0; p < arrunit.length; p++) {
                                                                            if(arrunit[p] === 0){
                                                                                continue;
                                                                                }
                                                                            temp3 = arrunit[p];
                                                                            if (temp4 === 0) {
                                                                                // console.log("if temp4 zero=", temp3 + "temp2=", temp2)
                                                                                temp4 = parseFloat(temp3) - parseFloat(temp2);
                                                                                temp4 = parseFloat(temp4.toFixed(4));
                                                                                // console.log("if temp4 calculated zero=", temp4)
                                                                                if (temp4 === 0) {
                                                                                    arrunit[p] = 0;
                                                                                    arrnav[p] = 0;
                                                                                    arrpurchase[p] = 0;
                                                                                    arrdays[p] = 0;
                                                                                    alldays[p] = 0;
                                                                                    sum1[p] = 0;
                                                                                    sum2[p] = 0;
                                                                                    break;
                                                                                } else if (temp4 > 0) {
                                                                                    arrunit[p] = temp4;
                                                                                    arrpurchase[p] = temp4 * arrnav[p];
                                                                                    arrdays[p] = parseFloat(alldays[p] * arrpurchase[p]);
                
                                                                                    sum1[p] = parseFloat(temp4 * arrnav[p]) * parseFloat(alldays[p]) * parseFloat((parseFloat(Math.pow(parseFloat((cnav * temp4) / (temp4 * arrnav[p])), parseFloat(1 / parseFloat(alldays[p] / 365)))) - 1) * 100);
                                                                                    sum2[p] = parseFloat(arrpurchase[p]) * parseFloat(alldays[p]);
                                                                                    // console.log("unit=", arrunit)
                                                                                    // console.log("purchase=", arrpurchase, dataarr[i].SCHEME)
                                                                                    break;
                                                                                } else {
                                                                                    arrunit[p] = 0;
                                                                                    arrnav[p] = 0;
                                                                                    arrpurchase[p] = 0;
                                                                                    arrdays[p] = 0;
                                                                                    alldays[p] = 0;
                                                                                    sum1[p] = 0;
                                                                                    sum2[p] = 0;
                                                                                }
                                                                            } else if (temp4 > 0) {//+ve
                                                                                // console.log("if temp4 +ve temp2=", temp4 + ",temp2=", temp2)
                                                                                // console.log("counter length =", countlength, externelcount,arrnav.length+1)
                                                                                if (countlength === arrnav.length+1) {
                                                                                //    console.log("temp4 +ve counter length =", countlength + ",i=", i, p, externelcount,arrnav.length+1)
                                                                                //    console.log("11235 line no", temp4 ,temp3)
                                                                                   
                                                                                    temp4 = parseFloat(temp4) + parseFloat(temp3);
                                                                                    temp4 = parseFloat(temp4.toFixed(4));
                                                                                //    console.log("11237 line no", temp4 ,temp3)
                                                                                //     console.log("11241 line=",arrunit)
                                                                                  
                                                                                }
                                                                                else {
                                                                                    temp4 = parseFloat(temp4) - parseFloat(temp2);
                                                                                    temp4 = parseFloat(temp4.toFixed(4));
                                                                                    // console.log("if temp4 +ve=", temp4)
                                                                                }
                
                                                                                if (temp4 === 0) {
                                                                                    arrunit[p] = 0;
                                                                                    arrnav[p] = 0;
                                                                                    arrpurchase[p] = 0;
                                                                                    arrdays[p] = 0;
                                                                                    alldays[p] = 0;
                                                                                    sum1[p] = 0;
                                                                                    sum2[p] = 0;
                                                                                    break;
                                                                                } else if (temp4 > 0) {
                                                                                    arrunit[p] = temp4;
                                                                                    //console.log("check nav +ve=",temp4,arrnav[p])
                                                                                    arrpurchase[p] = temp4 * arrnav[p];
                                                                                    arrdays[p] = parseFloat(alldays[p]) * arrpurchase[p];
                                                                                    sum1[p] = parseFloat(temp4 * arrnav[p]) * parseFloat(alldays[p]) * parseFloat((parseFloat(Math.pow(parseFloat((cnav * temp4) / (temp4 * arrnav[p])), parseFloat(1 / parseFloat(alldays[p] / 365)))) - 1) * 100);
                                                                                    sum2[p] = parseFloat(arrpurchase[p]) * parseFloat(alldays[p]);
                                                                                    // console.log("unit=", arrunit)
                                                                                    // console.log("purchase=", arrpurchase, dataarr[i].SCHEME)
                                                                                  //  console.log("10978 =", countlength, externelcount,i,p)
                                                                                    break;
                                                                                } else {
                                                                                    arrunit[p] = 0;
                                                                                    arrnav[p] = 0;
                                                                                    arrpurchase[p] = 0;
                                                                                    arrdays[p] = 0;
                                                                                    alldays[p] = 0;
                                                                                    sum1[p] = 0;
                                                                                    sum2[p] = 0;
                                                                                }
                                                                            } else {//-ve
                                                                                temp4 = parseFloat(temp4) + parseFloat(temp3);
                                                                                temp4 = parseFloat(temp4.toFixed(4));
                                                                             //  console.log("if temp4 -ve=", temp4)
                
                                                                                if (temp4 === 0) {
                                                                                    arrunit[p] = 0;
                                                                                    arrnav[p] = 0;
                                                                                    arrpurchase[p] = 0;
                                                                                    arrdays[p] = 0;
                                                                                    alldays[p] = 0;
                                                                                    sum1[p] = 0;
                                                                                    sum2[p] = 0;
                                                                                    // console.log("unit=", arrunit)
                                                                                    // console.log("purchase=", arrpurchase, dataarr[i].SCHEME)
                                                                                    break;
                                                                                } else if (temp4 > 0) {
                                                                                    arrunit[p] = temp4;
                                                                                 //   console.log("check nav +ve=",temp4,arrnav[p])
                                                                                    arrpurchase[p] = temp4 * arrnav[p];
                                                                                    arrdays[p] = parseFloat(alldays[p]) * arrpurchase[p];
                                                                                    sum1[p] = parseFloat(temp4 * arrnav[p]) * parseFloat(alldays[p]) * parseFloat((parseFloat(Math.pow(parseFloat((cnav * temp4) / (temp4 * arrnav[p])), parseFloat(1 / parseFloat(alldays[p] / 365)))) - 1) * 100);
                                                                                    sum2[p] = parseFloat(arrpurchase[p]) * parseFloat(alldays[p]);
                                                                                    // console.log("unit=", arrunit)
                                                                                    // console.log("purchase=", arrpurchase, dataarr[i].SCHEME)
                
                                                                                    // if (countlength === externelcount) {
                                                                                    //   // console.log("dataarr.length =", countlength + ",i=", i, p, externelcount,arrnav.length+1)
                                                                                    //     arrunit[p] = 0;
                                                                                    //     arrnav[p] = 0;
                                                                                    //     arrpurchase[p] = 0;
                                                                                    //     arrdays[p] = 0;
                                                                                    //     alldays[p] = 0;
                                                                                    //     sum1[p] = 0;
                                                                                    //     sum2[p] = 0;
                                                                                    // }
                                                                                    // else {
                                                                                   //     console.log("break dataarr.length =", countlength + ",i=", i, p, externelcount)
                                                                                        // arrunit[p] = 0;
                                                                                        // arrnav[p] = 0;
                                                                                        // arrpurchase[p] = 0;
                                                                                        // arrdays[p] = 0;
                                                                                        // alldays[p] = 0;
                                                                                        // sum1[p] = 0;
                                                                                        // sum2[p] = 0;
                                                                                        break;
                                                                                 //  }
                
                                                                                } else {
                                                                                    arrunit[p] = 0;
                                                                                    arrnav[p] = 0;
                                                                                    arrpurchase[p] = 0;
                                                                                    arrdays[p] = 0;
                                                                                    alldays[p] = 0;
                                                                                    sum1[p] = 0;
                                                                                    sum2[p] = 0;
                                                                                    // break;
                                                                                }
                                                                            }
                                                                        }

                                                                    }//else condition equal switch

                                                                    balance = parseFloat(unit) + parseFloat(balance);

                                                                    cnav = dataarr[i].cnav;
                                                                    if (cnav === "" || cnav === undefined || isNaN(balance) || isNaN(cnav)) {
                                                                        balance = 0;
                                                                        cnav = 0;
                                                                    }
                                                                    currentval = cnav * balance
                                                                    cv = currentval + cv;
                                                                    cv= parseFloat(cv.toFixed(4));
                                                                }//if match scheme & folio 

                                                            } //dataarr inner loop

                                                            var sum1all = 0; var sum2all = 0;

                                                            for (var kk = 0; kk < sum1.length; kk++) {
                                                                sum1all = sum1[kk] + sum1all;
                                                            }
                                                            for (var kkk = 0; kkk < sum2.length; kkk++) {
                                                                sum2all = sum2[kkk] + sum2all;
                                                            }

                                                            if (isNaN(cv) || cv < 0) {
                                                                newarray.push(0)
                                                            } else {
                                                                newarray.push(Math.round(cv))
                                                            }

                                                            temp22 = 0; temp33 = 0

                                                            for (var k = 0; k < arrpurchase.length; k++) {
                                                                temp33 = Math.round(arrpurchase[k]);
                                                                temp22 = temp33 + temp22;
                                                            }
                                                            temp22 = parseFloat(temp22.toFixed(4));
                                                            purchase.push(temp22);

                                                        } // pan comparision

                                                        if (isNaN(sum2all) || (sum2all) === Infinity || isNaN(sum1all) || (sum1all) === Infinity) {
                                                            cagrsum2array.push(0);
                                                            cagrsum1array.push(0);
                                                        } else {
                                                            cagrsum2array.push(sum2all);
                                                            cagrsum1array.push(sum1all);
                                                        }

                                                    }
                                                    temp22 = 0; temp33 = 0;

                                                    for (var p = 0; p < cagrsum1array.length; p++) {
                                                        finalsum1 = cagrsum1array[p] + finalsum1;
                                                    }
                                                    for (var p = 0; p < cagrsum2array.length; p++) {
                                                        finalsum2 = cagrsum2array[p] + finalsum2;
                                                    }


                                                    cagr = finalsum1 / finalsum2;


                                                    for (var p = 0; p < newarray.length; p++) {
                                                        temp44 = newarray[p] + temp44;
                                                    }
                                                    for (var k = 0; k < purchase.length; k++) {
                                                        temp33 = Math.round(purchase[k]);
                                                        temp22 = temp33 + temp22;
                                                    }
                                                    if (temp22 === 0 && temp44 === 0) {
                                                        cagr = 0;
                                                        finalarr.push({ name: name, purchasecost: Math.round(temp22), currentvalue: Math.round(temp44), cagr: parseFloat(cagr.toFixed(1)), pan: pan })
                                                    } else {
                                                        finalarr.push({ name: name, purchasecost: Math.round(temp22), currentvalue: Math.round(temp44), cagr: parseFloat(cagr.toFixed(1)), pan: pan })
                                                    }
                                                    resdata = {
                                                        status: 200,
                                                        message: "Successfull",
                                                        data: temp22
                                                    }
                                                    resdata.data = finalarr;
                                                    if (finalarr.length === panarray.length) {
                                                        res.json(resdata);
                                                    }

                                                }
                                            } else {
                                                console.log("purchase=", "Data Not Found!")
                                                resdata = {
                                                    status: 400,
                                                    message: 'Data not found',
                                                }
                                                res.json(resdata);
                                            }

                                        })

                                    } else {
                                        resdata = {
                                            status: 400,
                                            message: 'Data not found',
                                        }
                                        res.json(resdata);
                                    }

                                });
                            })
                        });
                    });
                } else {
                    var dataarr = []; var lastarray = []; let newarray = [];
                    let cagrsum1array = []; let cagrsum2array = []; let finalsum1 = 0; let finalsum2 = 0;
                    var finaldataarr=[];var hh=[];var gg=[];
                    var newdataarr1 = [];var newdataarr2 = [];var newdataarr3 = [];
                    pipeline1 = [  //trans_cams
                        { $match: { PAN: req.body.pan } },
                        { $project: { _id: 0, NAME: { "$toUpper": ["$INV_NAME"] }, PAN: "$PAN", PRODCODE: "$PRODCODE", FOLIO: "$FOLIO_NO",SCHEME:"$SCHEME" , RTA: "CAMS" } }
                    ]
                    pipeline2 = [  //trans_karvy
                        { $match: { PAN1: req.body.pan } },
                        { $project: { _id: 0, NAME: { "$toUpper": ["$INVNAME"] }, PAN: "$PAN1", PRODCODE: "$FMCODE", FOLIO: "$TD_ACNO",SCHEME:"$FUNDDESC" , RTA: "KARVY" } }
                    ]

                    transc.aggregate(pipeline1, (err, data1) => {
                        transk.aggregate(pipeline2, (err, data2) => {
                            //console.log("DATA TESTING=", data1, data2);
                            if (data1.length != 0 || data2.length != 0) {
                                resdata = {
                                    status: 200,
                                    message: 'Successfull',
                                    data: data2
                                }
                                let datacon = data1.concat(data2);
                                datacon = datacon.filter(
                                    (temp => a =>
                                        (k => !temp[k] && (temp[k] = true))(a.PRODCODE + '|' + a.FOLIO)
                                    )(Object.create(null))
                                );

                                var uniquename = datacon.filter(
                                    (temp => a =>
                                        (k => !temp[k] && (temp[k] = true))(a.NAME + '|' + a.PAN)
                                    )(Object.create(null))
                                );
                                datacon = datacon.sort((a, b) => (a.SCHEME > b.SCHEME) ? 1 : -1);
                                portfolioApiDetail(datacon, result => {
                                    lastarray.push(result);
                                    for (var j = 0; j < lastarray.length; j++) {
                                        for (var k = 0; k < lastarray[j].length; k++) {
                                            dataarr.push(lastarray[j][k]);
                                        }
                                    }
                                    var amount = 0;var days=0; var date1 = ""; var date2 = "";
                                    
                                    var purchase = []; var temp44 = 0;
                                    var cnav = 0; var temp222 = 0; var finalarr = [];


                                    if (dataarr != null && dataarr.length > 0) {
                                        var currentval = 0;
                                        for (var a = 0; a < datacon.length; a++) {
                                            newdataarr1 =[];newdataarr2=[];newdataarr3=[];
                                            dataarr = dataarr.sort((a, b) => new Date(a.TD_TRDT.split("-").reverse().join("/")).getTime() - new Date(b.TD_TRDT.split("-").reverse().join("/")).getTime());
                                            for (var ii = 0; ii < dataarr.length; ii++) {
                                                 if (datacon[a].FOLIO === dataarr[ii].FOLIO && datacon[a].PRODCODE === dataarr[ii].PRODCODE) {
                                                 if(dataarr[ii].RTA === "CAMS"){
                                                        newdataarr1.push(dataarr[ii])
                                                  }else if(dataarr[ii].RTA === "CAMS2A"){
                                                          newdataarr2.push(dataarr[ii])
                                                   }else{
                                                          newdataarr3.push(dataarr[ii])
                                                   }
                                                 }
                                                }
                                              if(newdataarr2.length>0){
                                                    newdataarr1.shift();
                                                }
                                                hh = newdataarr2.concat(newdataarr1.concat(newdataarr3))
                                                finaldataarr.push(hh);
                                                
                                        }
        
                                        for (var j = 0; j < finaldataarr.length; j++) {
                                            for (var k = 0; k < finaldataarr[j].length; k++) {
                                                gg.push(finaldataarr[j][k]);
                                            }
                                        }
                                      dataarr = gg;
                                        for (var a = 0; a < datacon.length; a++) {
                                            var unit = 0; var arrpurchase = []; var arrunit = [];
                                            var temp4 = 0; var temp1, temp2 = 0; var temp3 = 0;
                                            var cv = 0; var sum1 = []; var sum2 = [];
                                            var balance=0;var days = 0; var arrdays = []; var alldays = []; var arrnav=[];var scheme="";var cnav = 0;
                                            dataarr = dataarr.sort((a, b) => new Date(a.TD_TRDT.split("-").reverse().join("/")).getTime() - new Date(b.TD_TRDT.split("-").reverse().join("/")).getTime());
                                            var countlength = 0;var externelcount=0;
                                            //loop for external counter
                                            for (var ii = 0; ii < dataarr.length; ii++) {
                                               if (datacon[a].FOLIO === dataarr[ii].FOLIO && datacon[a].PRODCODE === dataarr[ii].PRODCODE) {
                                                   externelcount = externelcount+1;
                                               }
                                           }
                                            for (var i = 0; i < dataarr.length; i++) {
                                              //  var currentval = 0; var balance = 0;

                                                if (datacon[a].FOLIO === dataarr[i].FOLIO && datacon[a].PRODCODE === dataarr[i].PRODCODE) {
                                                    countlength = countlength + 1;
                                                    if (Math.sign(dataarr[i].UNITS) != -1) {
                                                        if (dataarr[i].NATURE === "Switch Out")
                                                            for (var jj = 0; jj < arrunit.length; jj++) {

                                                                if (arrunit[jj] === 0)
                                                                    arrunit.shift();
                                                                if (arrpurchase[jj] === 0)
                                                                    arrpurchase.shift();
                                                                if (arrdays[jj] === 0)
                                                                    arrdays.shift();
                                                                if (alldays[jj] === 0)
                                                                    alldays.shift();
                                                                if (sum1[jj] === 0)
                                                                    sum1.shift();
                                                                if (sum2[jj] === 0)
                                                                    sum2.shift();
                                                                if (arrnav[jj] === 0)
                                                                    arrnav.shift();
                                                            }
                                                    }
                                                    cnav = dataarr[i].cnav;
                                                    if (dataarr[i].NATURE != 'Switch Out' && dataarr[i].UNITS != 0) {

                                                        unit = dataarr[i].UNITS
                                                        amount = dataarr[i].AMOUNT;
                                                        var date = dataarr[i].newdate;
                                                        var navdate = dataarr[i].navdate;

                                                        date1 = new Date(date);
                                                        date2 = new Date(navdate);
                                                        
                                                         days = Math.round((date2-date1)/(1000*60*60*24));

                                                        arrunit.push(dataarr[i].UNITS);

                                                        if(dataarr[i].TD_NAV === 0){
                                                             arrpurchase.push(0);
                                                         }else{
                                                              arrpurchase.push(dataarr[i].AMOUNT);
                                                        }
                                                        arrnav.push(dataarr[i].TD_NAV);

                                                         var precagr = parseFloat( (parseFloat(Math.pow(parseFloat((cnav * dataarr[i].UNITS) / (dataarr[i].AMOUNT)), parseFloat(1 / parseFloat(days / 365)))) - 1) * 100) ;
                                                        if(precagr=== Infinity){
                                                          precagr=0;
                                                         }else{
                                                          precagr = precagr ;
                                                        }

                                                        //sum1(purchase cost*days*cagr)
                                                        if (days === 0 && isNaN(days)) {
                                                            sum1.push(0);
                                                            arrdays.push(0);
                                                            alldays.push(0);
                                                            sum2.push(0);
                                                        } else {
                                                           
                                                            arrdays.push(parseFloat(days) * (parseFloat(dataarr[i].AMOUNT)));

                                                            alldays.push(parseFloat(days));

                                                            sum1.push(parseFloat(dataarr[i].AMOUNT) * parseFloat(days) * precagr);

                                                            sum2.push(parseFloat(dataarr[i].AMOUNT) * parseFloat(days));

                                                        }

                                                        // temp1 = dataarr[i].UNITS;
                                                        // temp2 = temp1 + temp2;
                                                        // navrate = dataarr[i].TD_NAV;


                                                    } else {

                                                        unit = "-" + dataarr[i].UNITS
                                                        amount = "-" + dataarr[i].AMOUNT
                                                        // if (temp4 != "" && temp4 != 0) {
                                                        //     arrunit.splice(0, 0, temp4);
                                                        // }
                                                        temp2 = dataarr[i].UNITS;
                                                        for (var p = 0; p < arrunit.length; p++) {
                                                            if(arrunit[p] === 0){
                                                                continue;
                                                                }
                                                            temp3 = arrunit[p];
                                                            if (temp4 === 0) {
                                                                //   console.log("if temp4 zero line 8942=", temp3 + "temp2=", temp2)
                                                                 temp4 = parseFloat(temp3) - parseFloat(temp2);
                                                                 temp4 = parseFloat(temp4.toFixed(4));
                                                                //   console.log("if temp4 calculated zero 8945=", temp4)
                                                                
                                                                if (temp4 === 0) {
                                                                    arrunit[p] = 0;
                                                                    arrnav[p] = 0;
                                                                    arrpurchase[p] = 0;
                                                                    arrdays[p] = 0;
                                                                    alldays[p] = 0;
                                                                    sum1[p] = 0;
                                                                    sum2[p] = 0;
                                                                    break;
                                                                } else if (temp4 > 0) {
                                                                    arrunit[p] = temp4;
                                                                    arrpurchase[p] = temp4 * arrnav[p];
                                                                    arrdays[p] = parseFloat(alldays[p] * arrpurchase[p]);

                                                                    sum1[p] = parseFloat(temp4 * arrnav[p]) * parseFloat(alldays[p]) * parseFloat((parseFloat(Math.pow(parseFloat((cnav * temp4) / (temp4 * arrnav[p])), parseFloat(1 / parseFloat(alldays[p] / 365)))) - 1) * 100);
                                                                    sum2[p] = parseFloat(arrpurchase[p]) * parseFloat(alldays[p]);
                                                                   
                                                                    break;
                                                                } else {
                                                                    arrunit[p] = 0;
                                                                    arrnav[p] = 0;
                                                                    arrpurchase[p] = 0;
                                                                    arrdays[p] = 0;
                                                                    alldays[p] = 0;
                                                                    sum1[p] = 0;
                                                                    sum2[p] = 0;
                                                                }
                                                            } else if (temp4 > 0) {//+ve
                                                                
                                                                if (countlength === arrnav.length+1) {
                                                                 //   console.log("temp4 +ve counter length line 8977 =", countlength + ",i=", i, p, externelcount,arrnav.length+1)
                                                                 //  console.log("11235 line no", temp4 ,temp3)
                                                                   
                                                                    temp4 = parseFloat(temp4) + parseFloat(temp3);
                                                                    temp4 = parseFloat(temp4.toFixed(4));
                                                                   // console.log("11237 line no", temp4 ,temp3)
                                                                //     console.log("11241 line=",arrunit)
                                                                  
                                                                }
                                                                else {
                                                                   // console.log("temp2 >> if temp4 +ve line 8987=", temp4,temp2)
                                                                    temp4 = parseFloat(temp4) - parseFloat(temp2);
                                                                    temp4 = parseFloat(temp4.toFixed(4));
                                                                     //console.log("if temp4 +ve line 8989=", temp4)
                                                                }

                                                                if (temp4 === 0) {
                                                                    arrunit[p] = 0;
                                                                    arrnav[p] = 0;
                                                                    arrpurchase[p] = 0;
                                                                    arrdays[p] = 0;
                                                                    alldays[p] = 0;
                                                                    sum1[p] = 0;
                                                                    sum2[p] = 0;
                                                                    break;
                                                                } else if (temp4 > 0) {
                                                                    arrunit[p] = temp4;
                                                                    //console.log("line 9003 check nav +ve=",temp4,arrnav[p])
                                                                    arrpurchase[p] = temp4 * arrnav[p];
                                                                    arrdays[p] = parseFloat(alldays[p]) * arrpurchase[p];
                                                                    sum1[p] = parseFloat(temp4 * arrnav[p]) * parseFloat(alldays[p]) * parseFloat((parseFloat(Math.pow(parseFloat((cnav * temp4) / (temp4 * arrnav[p])), parseFloat(1 / parseFloat(alldays[p] / 365)))) - 1) * 100);
                                                                    sum2[p] = parseFloat(arrpurchase[p]) * parseFloat(alldays[p]);
                                                                   
                                                                    // if (countlength === externelcount) {
                                                                    //     //console.log(" line 9010 dataarr.length =", countlength + ",i=", i, p, externelcount,arrnav.length+1)
                                                                    //      arrunit[p] = 0;
                                                                    //      arrnav[p] = 0;
                                                                    //      arrpurchase[p] = 0;
                                                                    //      arrdays[p] = 0;
                                                                    //      alldays[p] = 0;
                                                                    //      sum1[p] = 0;
                                                                    //      sum2[p] = 0;
                                                                    //  }
                                                                    //  else {
                                                                         break;
                                                                  //  }
                                                                } else {
                                                                    arrunit[p] = 0;
                                                                    arrnav[p] = 0;
                                                                    arrpurchase[p] = 0;
                                                                    arrdays[p] = 0;
                                                                    alldays[p] = 0;
                                                                    sum1[p] = 0;
                                                                    sum2[p] = 0;
                                                                    
                                                                }
                                                            } else {//-ve
                                                                // console.log("arrunit >>>>>>>>>line 9033=",arrunit)
                                                                // console.log("if temp4 -ve & temp3 line 9033=", temp4,temp3)
                                                                temp4 = parseFloat(temp4) + parseFloat(temp3);
                                                                temp4 = parseFloat(temp4.toFixed(4));
                                                            //    console.log("if temp4 -ve line 9036=", temp4)
                                                                if (temp4 === 0) {
                                                                    arrunit[p] = 0;
                                                                    arrnav[p] = 0;
                                                                    arrpurchase[p] = 0;
                                                                    arrdays[p] = 0;
                                                                    alldays[p] = 0;
                                                                    sum1[p] = 0;
                                                                    sum2[p] = 0;
                                                                    
                                                                    break;
                                                                } else if (temp4 > 0) {
                                                                    arrunit[p] = temp4;
                                                                    // console.log("check nav +ve line 9049=",temp4,arrnav[p])
                                                                    arrpurchase[p] = temp4 * arrnav[p];
                                                                    arrdays[p] = parseFloat(alldays[p]) * arrpurchase[p];
                                                                    sum1[p] = parseFloat(temp4 * arrnav[p]) * parseFloat(alldays[p]) * parseFloat((parseFloat(Math.pow(parseFloat((cnav * temp4) / (temp4 * arrnav[p])), parseFloat(1 / parseFloat(alldays[p] / 365)))) - 1) * 100);
                                                                    sum2[p] = parseFloat(arrpurchase[p]) * parseFloat(alldays[p]);
                                                                    
                                                                //     if (countlength === externelcount) {
                                                                //     //    console.log("dataarr.length line 9056 =", countlength + ",i=", i, p, externelcount,arrnav.length+1)
                                                                //         arrunit[p] = 0;
                                                                //         arrnav[p] = 0;
                                                                //         arrpurchase[p] = 0;
                                                                //         arrdays[p] = 0;
                                                                //         alldays[p] = 0;
                                                                //         sum1[p] = 0;
                                                                //         sum2[p] = 0;
                                                                //        // break;
                                                                //     }
                                                                //    else {
                                                                        break;
                                                                //   }

                                                                } else {
                                                                    arrunit[p] = 0;
                                                                    arrnav[p] = 0;
                                                                    arrpurchase[p] = 0;
                                                                    arrdays[p] = 0;
                                                                    alldays[p] = 0;
                                                                    sum1[p] = 0;
                                                                    sum2[p] = 0;
                                                                    // break;
                                                                }
                                                            }
                                                        }

                                                    }//else condition equal switch

                                                    balance = parseFloat(unit) + parseFloat(balance);

                                                   // cnav = dataarr[i].cnav;
                                                   
                                                    if (cnav === "" || cnav === undefined || isNaN(balance) || isNaN(cnav)) {
                                                        balance = 0;
                                                        cnav = 0;
                                                    }
                                                    currentval = Math.round(cnav * balance)
                                                    currentval = parseFloat(currentval.toFixed(4));
                                                    scheme = dataarr[i].SCHEME;
                                                    // cv = currentval + cv;
                                                    // cv = parseFloat(cv.toFixed(4));


                                                }//if match two scheme and folio array 

                                            } //dataarr inner loop
                                            //console.log("arrpurchase=",arrpurchase)
                                            balance = parseFloat(balance.toFixed(3))
                                            if (isNaN(currentval) || currentval < 0) {
                                                newarray.push(0)
                                            } else {
                                                if(scheme != ""){
                                                newarray.push(Math.round(currentval))
                                                }
                                            }

                                            temp22 = 0; temp33 = 0

                                            for (var k = 0; k < arrpurchase.length; k++) {
                                                temp33 = Math.round(arrpurchase[k]);
                                                temp22 = temp33 + temp22;
                                                temp222 = Math.round(arrdays[k]) + temp222;
                                            }
                                            temp22 = parseFloat(temp22.toFixed(4));
                                            if(temp222 === 0 && temp22 === 0){
                                                days = 0;
                                            }else{
                                                days = temp222 / temp22;
                                            }
                                            console.log("ssssscv=",temp22, cv,scheme)
                                            if (currentval === 0 || balance === 0 || currentval === -0 || balance === -0) {
                                                temp22 = 0;
                                            }else{
                                                purchase.push(temp22);
                                            }
                                           


                                            var sum1all = 0; var sum2all = 0;
                                            console.log("sum111111=",sum1)
                                            console.log("sum222222=",sum2)
                                            for (var kk = 0; kk < sum1.length; kk++) {
                                                sum1all = sum1[kk] + sum1all;
                                            }
                                            for (var kkk = 0; kkk < sum2.length; kkk++) {
                                                sum2all = sum2[kkk] + sum2all;
                                            }
                                            var cagr=0;
                                            // if(sum1all === 0 && sum2all === 0){
                                            //     cagr=0;
                                            // }else{
                                            //     cagr = sum1all / sum2all;
                                            // }
                                            if (isNaN(sum2all) || (sum2all) === Infinity || isNaN(sum1all) || (sum1all) === Infinity) {
                                                cagrsum2array.push(0);
                                                cagrsum1array.push(0);
                                            } else {
                                                cagrsum2array.push(sum2all);
                                                cagrsum1array.push(sum1all);
                                            }
                                           
                                        } // datascheme first loop
                                       // var cagr=sum1all/sum2all;
                                        temp22 = 0; temp33 = 0
                                        for (var p = 0; p < cagrsum1array.length; p++) {
                                            finalsum1 = cagrsum1array[p] + finalsum1;
                                        }
                                        for (var p = 0; p < cagrsum2array.length; p++) {
                                            finalsum2 = cagrsum2array[p] + finalsum2;
                                        }
                                        
                                        cagr = finalsum1 / finalsum2;
                                        for (var p = 0; p < newarray.length; p++) {
                                            temp44 = newarray[p] + temp44;
                                        }
                                        for (var k = 0; k < purchase.length; k++) {
                                            temp33 = purchase[k];
                                            temp22 = temp33 + temp22;
                                        }
                                        if (temp22 != 0 && temp44 != 0) {
                                            finalarr.push({ name: uniquename[0].NAME, purchasecost: Math.round(temp22), currentvalue: Math.round(temp44), cagr: parseFloat(cagr.toFixed(1)), pan: uniquename[0].PAN })
                                        }
                                        console.log("purchase=", finalarr)
                                        resdata = {
                                            status: 200,
                                            message: "Successfull",
                                            data: temp22
                                        }
                                        resdata.data = finalarr;
                                        res.json(resdata);

                                    } else {
                                        console.log("purchase=", "Data Not Found!");
                                        resdata = {
                                            status: 400,
                                            message: 'Data not found',
                                        }
                                        res.json(resdata);
                                    }

                                })

                            } else {
                                resdata = {
                                    status: 400,
                                    message: 'Data not found',
                                }
                                res.json(resdata);
                            }
                        });
                    })
                }
            });
        }
    } catch (err) {
        console.log(err)
    }
})



app.post("/api/portfolio_detailapi_data", function (req, res) {
    try {
        let regex = /^([a-zA-Z]){5}([0-9]){4}([a-zA-Z]){1}?$/;
        if (req.body.name === "") {
            resdata = {
                status: 400,
                message: 'Please enter name',
            }
            res.json(resdata);
            return resdata;
        } else {
            if (req.body.guard_pan === "") {
                if (req.body.pan === "") {
                    resdata = {
                        status: 400,
                        message: 'Please enter pan',
                    }
                    res.json(resdata);
                    return resdata;
                } else if (!regex.test(req.body.pan)) {
                    resdata = {
                        status: 400,
                        message: 'Please enter valid pan',
                    }
                    res.json(resdata);
                    return resdata;
                } else {
                    var dataarr = []; var lastarray = []; let newarray = []; var sum11 = []; var sum22 = [];var finaldataarr=[];var hh=[];var gg=[];
                    var newdataarr1 = [];var newdataarr2 = [];var newdataarr3 = [];
                    pipeline1 = [  //trans_cams
                        { $match: { PAN: req.body.pan, INV_NAME: { $regex: req.body.name, $options: 'i' } } },
                        { $project: { _id: 0, PRODCODE: "$PRODCODE", PAN: "$PAN", FOLIO: "$FOLIO_NO", NAME: { "$toUpper": ["$INV_NAME"] }, SCHEME: "$SCHEME", RTA: "CAMS", } },
                    ]
                    pipeline2 = [  //trans_karvy
                        { $match: { PAN1: req.body.pan, INVNAME: { $regex: req.body.name, $options: 'i' } } },
                        { $project: { _id: 0, PRODCODE: "$FMCODE", PAN: "$PAN1", FOLIO: "$TD_ACNO", NAME: { "$toUpper": ["$INVNAME"] }, SCHEME: "$FUNDDESC", RTA: "KARVY" } },
                    ]
                    transc.aggregate(pipeline1, (err, data1) => {
                        transk.aggregate(pipeline2, (err, data2) => {
                            if (data1 != 0 || data2 != 0) {
                                resdata = {
                                    status: 200,
                                    message: 'Successfull',
                                    data: data2
                                }
                                let datacon = data1.concat(data2);

                                datacon = datacon.filter(
                                    (temp => a =>
                                        (k => !temp[k] && (temp[k] = true))(a.PRODCODE + '|' + a.FOLIO)
                                    )(Object.create(null))
                                );
                                datacon = datacon.sort((a, b) => (a.SCHEME > b.SCHEME) ? 1 : -1);
                                portfolioApiDetail(datacon, result => {
                                    lastarray.push(result);
                                    for (var j = 0; j < lastarray.length; j++) {
                                        for (var k = 0; k < lastarray[j].length; k++) {
                                            dataarr.push(lastarray[j][k]);
                                        }
                                    }
                                   
                                    var amount = 0; var date1 = ""; var date2 = "";
                                    var navrate = 0;
                                    var purchase = []; var temp44 = 0;
                                    var cnav = 0; var temp222 = 0; var finalarr = [];
                                    var portfolio_data = []; var tot_mkt_value = [];
                                    var tot_gain = []; var tot_cost = []; var tot_cagr = []; var type = "";
                                    if (dataarr != null && dataarr.length > 0) {
                                        var currentval = 0;
                                       
                                        for (var a = 0; a < datacon.length; a++) {
                                            newdataarr1 =[];newdataarr2=[];newdataarr3=[];
                                            dataarr = dataarr.sort((a, b) => new Date(a.TD_TRDT.split("-").reverse().join("/")).getTime() - new Date(b.TD_TRDT.split("-").reverse().join("/")).getTime());
                                            for (var ii = 0; ii < dataarr.length; ii++) {
                                                 if (datacon[a].FOLIO === dataarr[ii].FOLIO && datacon[a].PRODCODE === dataarr[ii].PRODCODE) {
                                                 if(dataarr[ii].RTA === "CAMS"){
                                                        newdataarr1.push(dataarr[ii])
                                                     //   console.log("cams=",dataarr[ii].AMOUNT,dataarr[ii].PRODCODE,dataarr[ii].FOLIO)
                                                  }else if(dataarr[ii].RTA === "CAMS2A"){
                                                          newdataarr2.push(dataarr[ii])
                                                         // console.log("2a cams=",dataarr[ii].AMOUNT,dataarr[ii].PRODCODE,dataarr[ii].FOLIO)
                                                   }else{
                                                          newdataarr3.push(dataarr[ii])
                                                   }
                                                 }
                                                }
                                              if(newdataarr2.length>0){
                                                    newdataarr1.shift();
                                                }
                                                hh = newdataarr2.concat(newdataarr1.concat(newdataarr3))
                                                finaldataarr.push(hh);
                                                
                                        }
        
                                        for (var j = 0; j < finaldataarr.length; j++) {
                                            for (var k = 0; k < finaldataarr[j].length; k++) {
                                                gg.push(finaldataarr[j][k]);
                                            }
                                        }
                                      dataarr = gg;
                                        for (var a = 0; a < datacon.length; a++) {
                                            var unit = 0; var arrpurchase = []; var arrunit = [];
                                            var temp4 = 0; var temp1, temp2 = 0; var temp3 = 0;
                                            var cv = 0; var sum1 = []; var sum2 = [];
                                            var balance = 0; var days = 0; var arrdays = [];
                                            var gain = 0; var alldays = []; var arrnav = [];
                                            dataarr = dataarr.sort((a, b) => new Date(a.TD_TRDT.split("-").reverse().join("/")).getTime() - new Date(b.TD_TRDT.split("-").reverse().join("/")).getTime());
                                            var scheme = ""; var name = ""; var pan = ""; var folio = "";
                                            var countlength = 0;var externelcount=0;
                                            //loop for external counter
                                            for (var ii = 0; ii < dataarr.length; ii++) {
                                                if (datacon[a].FOLIO === dataarr[ii].FOLIO && datacon[a].PRODCODE === dataarr[ii].PRODCODE) {
                                                    externelcount = externelcount+1;
                                                }
                                            }
                                            
                                            for (var i = 0; i < dataarr.length; i++) {
                                                if (datacon[a].FOLIO === dataarr[i].FOLIO && datacon[a].PRODCODE === dataarr[i].PRODCODE) {
                                                    countlength = countlength + 1;
                                                    // if (datacon[a].FOLIO === "91319243433" && datacon[a].PRODCODE === "128TSGP") {
                                                    //         console.log(dataarr[i].FOLIO, dataarr[i].NATURE, dataarr[i].UNITS)
                                                    //    }
                                                    if (Math.sign(dataarr[i].UNITS) != -1) {
                                                        if (dataarr[i].NATURE === "Switch Out")
                                                            for (var jj = 0; jj < arrunit.length; jj++) {

                                                                if (arrunit[jj] === 0)
                                                                    arrunit.shift();
                                                                if (arrpurchase[jj] === 0)
                                                                    arrpurchase.shift();
                                                                if (arrdays[jj] === 0)
                                                                    arrdays.shift();
                                                                if (alldays[jj] === 0)
                                                                    alldays.shift();
                                                                if (sum1[jj] === 0)
                                                                    sum1.shift();
                                                                if (sum2[jj] === 0)
                                                                    sum2.shift();
                                                                if (arrnav[jj] === 0)
                                                                    arrnav.shift();
                                                            }
                                                    }
                                                    cnav = dataarr[i].cnav;
                                                    if (dataarr[i].NATURE != 'Switch Out' && dataarr[i].UNITS != 0) {

                                                        unit = dataarr[i].UNITS
                                                        amount = dataarr[i].AMOUNT;
                                                        var date = dataarr[i].newdate;
                                                        var navdate = dataarr[i].navdate;

                                                        date1 = new Date(date);
                                                        date2 = new Date(navdate);
                                                        
                                                         days = Math.round((date2-date1)/(1000*60*60*24));
                                                        arrunit.push(dataarr[i].UNITS);
                                                        if(dataarr[i].TD_NAV === 0){
                                                            arrpurchase.push(0);
                                                        }else{
                                                            arrpurchase.push(dataarr[i].AMOUNT);
                                                        }
                                                       
                                                        arrnav.push(dataarr[i].TD_NAV)
                                                        //console.log("hello nav=",arrnav)
                                                        var precagr = parseFloat( (parseFloat(Math.pow(parseFloat((cnav * dataarr[i].UNITS) / (dataarr[i].AMOUNT)), parseFloat(1 / parseFloat(days / 365)))) - 1) * 100) ;
                                                            if(precagr=== Infinity){
                                                                precagr=0;
                                                            }else{
                                                                precagr = precagr ;
                                                            }
                                                        //sum1(purchase cost*days*cagr)
                                                        if (days === 0 && isNaN(days)) {
                                                            sum1.push(0);
                                                            arrdays.push(0);
                                                            alldays.push(0);
                                                            sum2.push(0);
                                                        } else {
                                                            arrdays.push(parseFloat(days) * (parseFloat(dataarr[i].AMOUNT)));
                                                            alldays.push(parseFloat(days));
                                                            sum1.push(parseFloat(dataarr[i].AMOUNT) * parseFloat(days) * precagr);
                                                            sum2.push(parseFloat(dataarr[i].AMOUNT) * parseFloat(days));

                                                        }

                                                      //  temp1 = dataarr[i].UNITS;
                                                       
                                                    } else {

                                                        unit = "-" + dataarr[i].UNITS
                                                        amount = "-" + dataarr[i].AMOUNT
                                                        temp2 = dataarr[i].UNITS;
                                                       // console.log("switchout temp2 line 8938=",temp2)
                                                        for (var p = 0; p < arrunit.length; p++) {
                                                            if(arrunit[p] === 0){
                                                            continue;
                                                            }
                                                            temp3 = arrunit[p];
                                                            if (temp4 === 0) {
                                                                //   console.log("if temp4 zero line 8942=", temp3 + "temp2=", temp2)
                                                                 temp4 = parseFloat(temp3) - parseFloat(temp2);
                                                                 temp4 = parseFloat(temp4.toFixed(4));
                                                                //   console.log("if temp4 calculated zero 8945=", temp4)
                                                                
                                                                if (temp4 === 0) {
                                                                    arrunit[p] = 0;
                                                                    arrnav[p] = 0;
                                                                    arrpurchase[p] = 0;
                                                                    arrdays[p] = 0;
                                                                    alldays[p] = 0;
                                                                    sum1[p] = 0;
                                                                    sum2[p] = 0;
                                                                    break;
                                                                } else if (temp4 > 0) {
                                                                    arrunit[p] = temp4;
                                                                    arrpurchase[p] = temp4 * arrnav[p];
                                                                    arrdays[p] = parseFloat(alldays[p] * arrpurchase[p]);

                                                                    sum1[p] = parseFloat(temp4 * arrnav[p]) * parseFloat(alldays[p]) * parseFloat((parseFloat(Math.pow(parseFloat((cnav * temp4) / (temp4 * arrnav[p])), parseFloat(1 / parseFloat(alldays[p] / 365)))) - 1) * 100);
                                                                    sum2[p] = parseFloat(arrpurchase[p]) * parseFloat(alldays[p]);
                                                                   
                                                                    break;
                                                                } else {
                                                                    arrunit[p] = 0;
                                                                    arrnav[p] = 0;
                                                                    arrpurchase[p] = 0;
                                                                    arrdays[p] = 0;
                                                                    alldays[p] = 0;
                                                                    sum1[p] = 0;
                                                                    sum2[p] = 0;
                                                                }
                                                            } else if (temp4 > 0) {//+ve
                                                                
                                                                if (countlength === arrnav.length+1) {
                                                                    console.log("temp4 +ve counter length line 8977 =", countlength + ",i=", i, p, externelcount,arrnav.length+1)
                                                                   console.log("11235 line no", temp4 ,temp3)
                                                                   
                                                                    temp4 = parseFloat(temp4) + parseFloat(temp3);
                                                                    temp4 = parseFloat(temp4.toFixed(4));
                                                                    console.log("11237 line no", temp4 ,temp3)
                                                                //     console.log("11241 line=",arrunit)
                                                                  
                                                                }
                                                                else {
                                                                    console.log("temp2 >> if temp4 +ve line 8987=", temp4,temp2)
                                                                    temp4 = parseFloat(temp4) - parseFloat(temp2);
                                                                    temp4 = parseFloat(temp4.toFixed(4));
                                                                     console.log("if temp4 +ve line 8989=", temp4)
                                                                }

                                                                if (temp4 === 0) {
                                                                    arrunit[p] = 0;
                                                                    arrnav[p] = 0;
                                                                    arrpurchase[p] = 0;
                                                                    arrdays[p] = 0;
                                                                    alldays[p] = 0;
                                                                    sum1[p] = 0;
                                                                    sum2[p] = 0;
                                                                    break;
                                                                } else if (temp4 > 0) {
                                                                    arrunit[p] = temp4;
                                                                    console.log("line 9003 check nav +ve=",temp4,arrnav[p])
                                                                    arrpurchase[p] = temp4 * arrnav[p];
                                                                    arrdays[p] = parseFloat(alldays[p]) * arrpurchase[p];
                                                                    sum1[p] = parseFloat(temp4 * arrnav[p]) * parseFloat(alldays[p]) * parseFloat((parseFloat(Math.pow(parseFloat((cnav * temp4) / (temp4 * arrnav[p])), parseFloat(1 / parseFloat(alldays[p] / 365)))) - 1) * 100);
                                                                    sum2[p] = parseFloat(arrpurchase[p]) * parseFloat(alldays[p]);
                                                                   
                                                                    // if (countlength === externelcount) {
                                                                    //     console.log(" line 9010 dataarr.length =", countlength + ",i=", i, p, externelcount,arrnav.length+1)
                                                                    //      arrunit[p] = 0;
                                                                    //      arrnav[p] = 0;
                                                                    //      arrpurchase[p] = 0;
                                                                    //      arrdays[p] = 0;
                                                                    //      alldays[p] = 0;
                                                                    //      sum1[p] = 0;
                                                                    //      sum2[p] = 0;
                                                                    //  }
                                                                    //  else {
                                                                         break;
                                                                 //   }
                                                                } else {
                                                                    arrunit[p] = 0;
                                                                    arrnav[p] = 0;
                                                                    arrpurchase[p] = 0;
                                                                    arrdays[p] = 0;
                                                                    alldays[p] = 0;
                                                                    sum1[p] = 0;
                                                                    sum2[p] = 0;
                                                                    
                                                                }
                                                            } else {//-ve
                                                                // console.log("arrunit >>>>>>>>>line 9033=",arrunit)
                                                                // console.log("if temp4 -ve & temp3 line 9033=", temp4,temp3)
                                                                temp4 = parseFloat(temp4) + parseFloat(temp3);
                                                                temp4 = parseFloat(temp4.toFixed(4));
                                                            //    console.log("if temp4 -ve line 9036=", temp4)
                                                                if (temp4 === 0) {
                                                                    arrunit[p] = 0;
                                                                    arrnav[p] = 0;
                                                                    arrpurchase[p] = 0;
                                                                    arrdays[p] = 0;
                                                                    alldays[p] = 0;
                                                                    sum1[p] = 0;
                                                                    sum2[p] = 0;
                                                                    
                                                                    break;
                                                                } else if (temp4 > 0) {
                                                                    arrunit[p] = temp4;
                                                                    // console.log("check nav +ve line 9049=",temp4,arrnav[p])
                                                                    arrpurchase[p] = temp4 * arrnav[p];
                                                                    arrdays[p] = parseFloat(alldays[p]) * arrpurchase[p];
                                                                    sum1[p] = parseFloat(temp4 * arrnav[p]) * parseFloat(alldays[p]) * parseFloat((parseFloat(Math.pow(parseFloat((cnav * temp4) / (temp4 * arrnav[p])), parseFloat(1 / parseFloat(alldays[p] / 365)))) - 1) * 100);
                                                                    sum2[p] = parseFloat(arrpurchase[p]) * parseFloat(alldays[p]);
                                                                    
                                                                //     if (countlength === externelcount) {
                                                                //     //    console.log("dataarr.length line 9056 =", countlength + ",i=", i, p, externelcount,arrnav.length+1)
                                                                //         arrunit[p] = 0;
                                                                //         arrnav[p] = 0;
                                                                //         arrpurchase[p] = 0;
                                                                //         arrdays[p] = 0;
                                                                //         alldays[p] = 0;
                                                                //         sum1[p] = 0;
                                                                //         sum2[p] = 0;
                                                                //        // break;
                                                                //     }
                                                                //    else {
                                                                        break;
                                                                //   }

                                                                } else {
                                                                    arrunit[p] = 0;
                                                                    arrnav[p] = 0;
                                                                    arrpurchase[p] = 0;
                                                                    arrdays[p] = 0;
                                                                    alldays[p] = 0;
                                                                    sum1[p] = 0;
                                                                    sum2[p] = 0;
                                                                    // break;
                                                                }
                                                            }
                                                        }
                                                    }//else condition equal switch
                                                    //  console.log("temp4=",temp4)
                                                    
                                                    balance = parseFloat(unit) + parseFloat(balance);

                                                    //   cnav = dataarr[i].cnav;
                                                    if (cnav === "" || cnav === undefined || isNaN(balance) || isNaN(cnav)) {
                                                        balance = 0;
                                                        cnav = 0;
                                                    }
                                                    currentval = Math.round(cnav * balance);
                                                    currentval = parseFloat(currentval.toFixed(4));
                                                    type = dataarr[i].TYPE;
                                                    scheme = dataarr[i].SCHEME;
                                                }//if match two scheme and folio array 
                                                
                                            } //dataarr inner loop
                                            // console.log("final arrunit=",arrunit)
                                            // console.log("final arrnav=",arrnav)
                                             //console.log("final arrpurchase=",arrpurchase)
                                            // console.log("****************")
                                            
                                            balance = parseFloat(balance.toFixed(3))
                                            temp22 = 0; temp33 = 0; var temp222 = 0;

                                            for (var k = 0; k < arrpurchase.length; k++) {
                                                temp33 = Math.round(arrpurchase[k]);
                                                temp22 = temp33 + temp22;
                                                temp222 = Math.round(arrdays[k]) + temp222;
                                            }
                                            console.log("ssssscv=",temp22, currentval, cv,scheme)
                                            if(temp222 === 0 && temp22 === 0){
                                                days = 0;
                                            }else{
                                                days = temp222 / temp22;
                                            }
                                            
                                            var sum1all = 0; var sum2all = 0;

                                            for (var kk = 0; kk < sum1.length; kk++) {
                                                sum1all = sum1[kk] + sum1all;
                                            }
                                            for (var kkk = 0; kkk < sum2.length; kkk++) {
                                                sum2all = sum2[kkk] + sum2all;
                                            }

                                            var cagr =0;
                                            if(sum1all === 0 && sum2all === 0){
                                                cagr=0;
                                            }else{
                                                cagr = sum1all / sum2all;
                                            }
                                            
                                            if (currentval === 0 || balance === 0 || currentval === -0 || balance === -0) {
                                                temp22 = 0;
                                            }
                                            var absreturn = 0 ;
                                            if(temp22 === 0){
                                                absreturn = 0;
                                            }else{
                                                absreturn = (((parseFloat(currentval) - parseFloat(temp22)) / parseFloat(temp22)) * 100).toFixed(2);
                                            }
                                            
                                            gain = Math.round(currentval) - temp22;

                                            if (balance === 0 || balance < 0.02 || currentval === 0) {

                                            } else {
                                                currentval = Math.round(currentval);
                                                if (currentval != 0 || currentval != -0) {

                                                    tot_mkt_value.push(Math.round(currentval));
                                                    tot_cost.push(temp22);
                                                    tot_gain.push(gain);
                                                    purchase.push({ name: datacon[a].NAME, pan: datacon[a].PAN, scheme: datacon[a].SCHEME, product_code: datacon[a].PRODCODE, folio: datacon[a].FOLIO, absolute_return: absreturn, unit: balance.toFixed(3), purchase_cost: temp22, mkt_value: Math.round(currentval), gain: gain, cagr: cagr.toFixed(1), avg_days: Math.round(days), type: type });

                                                }
                                            }
                                        }
                                        
                                        // datascheme first loop
                                        for (var r = 0; r < purchase.length; r++) {
                                            sum11.push(purchase[r].purchase_cost * purchase[r].avg_days * purchase[r].cagr);
                                            sum22.push(purchase[r].purchase_cost * purchase[r].avg_days);
                                        }
                                        sum1all = 0; sum2all = 0;
                                        for (var kk = 0; kk < sum11.length; kk++) {
                                            sum1all = sum11[kk] + sum1all;
                                        }
                                        for (var kkk = 0; kkk < sum22.length; kkk++) {
                                            sum2all = sum22[kkk] + sum2all;
                                        }
                                        var sumcagr = sum1all / sum2all;
                                        var mkt = 0; var cost = 0; var totgain = 0;

                                        for (var p = 0; p < tot_mkt_value.length; p++) {
                                            mkt = tot_mkt_value[p] + mkt;
                                            cost = tot_cost[p] + cost;
                                            totgain = tot_gain[p] + totgain;

                                        }

                                        resdata.data = { portfolio_data: purchase, final_values: { tot_mkt_value: mkt, tot_cost: cost, tot_gain: totgain, tot_cagr: sumcagr.toFixed(1) } };

                                        res.json(resdata);

                                    } else {
                                        console.log("purchase=", "Data Not Found!")
                                        resdata = {
                                            status: 400,
                                            message: 'Data not found',
                                        }
                                        res.json(resdata);
                                    }

                                })


                            } else {
                                resdata = {
                                    status: 400,
                                    message: 'Data not found',
                                }
                                res.json(resdata);
                            }
                        });
                    })
                }
            } else {
                var arr1 = []; var arr2 = []; var dataarr = []; var lastarray = []; let newarray = []; var sum11 = []; var sum22 = [];
                folioc.find({ GUARD_PAN: req.body.guard_pan }).distinct("FOLIOCHK", function (err, member1) {
                    foliok.find({ GUARDPANNO: req.body.guard_pan }).distinct("ACNO", function (err, member2) {
                        var alldata = member1.concat(member2);
                        if (alldata.length != 0) {
                            for (var j = 0; j < alldata.length; j++) {
                                arr1.push({ FOLIO_NO: alldata[j] });
                                arr2.push({ TD_ACNO: alldata[j] });
                            }
                        }

                        var strFolio1 = { $or: arr1 };
                        var strFolio2 = { $or: arr2 };


                        pipeline1 = [  //trans_cams
                            { $match: strFolio1 },
                            { $project: { _id: 0, PRODCODE: "$PRODCODE", PAN: "$PAN", FOLIO: "$FOLIO_NO", NAME: { "$toUpper": ["$INV_NAME"] }, SCHEME: "$SCHEME", RTA: "CAMS", } },
                        ]
                        pipeline2 = [  //trans_karvy
                            { $match: strFolio2 },
                            { $project: { _id: 0, PRODCODE: "$FMCODE", PAN: "$PAN1", FOLIO: "$TD_ACNO", NAME: { "$toUpper": ["$INVNAME"] }, SCHEME: "$FUNDDESC", RTA: "KARVY" } },
                        ]
                        transk.aggregate(pipeline2, (err, data1) => {
                            transc.aggregate(pipeline1, (err, data2) => {

                                if (data1.length != 0 || data2.length != 0) {
                                    resdata = {
                                        status: 200,
                                        message: 'Successfull',
                                        data: data2
                                    }
                                    let datacon = data1.concat(data2);
                                    datacon = datacon.filter(
                                        (temp => a =>
                                            (k => !temp[k] && (temp[k] = true))(a.PRODCODE + '|' + a.FOLIO)
                                        )(Object.create(null))
                                    );
                                    var dataarr = []; var lastarray = [];
                                    datacon = datacon.sort((a, b) => (a.SCHEME > b.SCHEME) ? 1 : -1);
                                    portfolioApiDetail(datacon, result => {
                                        lastarray.push(result);
                                        for (var j = 0; j < lastarray.length; j++) {
                                            for (var k = 0; k < lastarray[j].length; k++) {
                                                dataarr.push(lastarray[j][k]);
                                            }
                                        }
                                        var amount = 0; var date1 = ""; var date2 = "";
                                        var navrate = 0;
                                        var purchase = []; var temp44 = 0;
                                        var temp222 = 0; var finalarr = [];
                                        var portfolio_data = []; var tot_mkt_value = [];
                                        var tot_gain = []; var tot_cost = []; var tot_cagr = []; var type = "";
                                        if (dataarr != null && dataarr.length > 0) {
                                            for (var a = 0; a < datacon.length; a++) {
                                                var unit = 0; var arrpurchase = []; var arrunit = [];
                                                var temp4 = 0; var temp1, temp2 = 0; var temp3 = 0;
                                                var cv = 0; var sum1 = []; var sum2 = []; var alldays = [];
                                                var balance = 0; var days = 0; var arrdays = [];
                                                var currentval = 0; var gain = 0; var arrnav = [];
                                                dataarr = dataarr.sort((a, b) => new Date(a.TD_TRDT.split("-").reverse().join("/")).getTime() - new Date(b.TD_TRDT.split("-").reverse().join("/")).getTime());
                                                var scheme = ""; var name = ""; var pan = "";
                                                var countlength = 0;
                                                //var externelcount=0;
                                                //loop for external counter
                                                // for (var ii = 0; ii < dataarr.length; ii++) {
                                                //     if (datacon[a].FOLIO === dataarr[ii].FOLIO && datacon[a].PRODCODE === dataarr[ii].PRODCODE) {
                                                //         externelcount = externelcount + 1;
                                                //     }
                                                // }
                                                for (var i = 0; i < dataarr.length; i++) {

                                                    var cnav = 0;
                                                    if (datacon[a].FOLIO === dataarr[i].FOLIO && datacon[a].PRODCODE === dataarr[i].PRODCODE) {
                                                        countlength = countlength + 1;
                                                     
                                                        if (Math.sign(dataarr[i].UNITS) != -1) {
                                                            if (dataarr[i].NATURE === "Switch Out")
                                                                for (var jj = 0; jj < arrunit.length; jj++) {

                                                                    if (arrunit[jj] === 0)
                                                                        arrunit.shift();
                                                                    if (arrpurchase[jj] === 0)
                                                                        arrpurchase.shift();
                                                                    if (arrdays[jj] === 0)
                                                                        arrdays.shift();
                                                                    if (alldays[jj] === 0)
                                                                        alldays.shift();
                                                                    if (sum1[jj] === 0)
                                                                        sum1.shift();
                                                                    if (sum2[jj] === 0)
                                                                        sum2.shift();
                                                                    if (arrnav[jj] === 0)
                                                                        arrnav.shift();
                                                                }
                                                        }
                                                        cnav = dataarr[i].cnav;
                                                        if (dataarr[i].NATURE != 'Switch Out' && dataarr[i].UNITS != 0) {

                                                            unit = dataarr[i].UNITS
                                                            amount = dataarr[i].AMOUNT;
                                                            var date = dataarr[i].newdate;
                                                            var navdate = dataarr[i].navdate;

                                                            date1 = new Date(date);
                                                            date2 = new Date(navdate);
                                                            days = Math.round((date2 - date1) / (1000 * 60 * 60 * 24));

                                                            arrunit.push(dataarr[i].UNITS);
                                                            arrnav.push(dataarr[i].TD_NAV);
                                                            if(dataarr[i].TD_NAV === 0){
                                                                arrpurchase.push(0);
                                                            }else{
                                                                arrpurchase.push(dataarr[i].AMOUNT);
                                                            }
                                                            var precagr = parseFloat( (parseFloat(Math.pow(parseFloat((cnav * dataarr[i].UNITS) / (dataarr[i].AMOUNT)), parseFloat(1 / parseFloat(days / 365)))) - 1) * 100) ;
                                                            if(precagr=== Infinity){
                                                                precagr=0;
                                                            }else{
                                                                precagr = precagr ;
                                                            }
                                                            //sum1(purchase cost*days*cagr)
                                                            if (days === 0 && isNaN(days)) {
                                                                sum1.push(0);
                                                                arrdays.push(0);
                                                                alldays.push(0);
                                                                sum2.push(0);
                                                            } else {
                                                                
                                                                
                                                                arrdays.push(parseFloat(days) * (parseFloat(dataarr[i].AMOUNT)));
                                                                alldays.push(parseFloat(days));
                                                                sum1.push(parseFloat(dataarr[i].AMOUNT) * parseFloat(days) * parseFloat(precagr ));
                                                                sum2.push(parseFloat(dataarr[i].AMOUNT) * parseFloat(days));

                                                            }
                                                            console.log("sum1=",parseFloat(cnav * dataarr[i].UNITS)/parseFloat(dataarr[i].AMOUNT),days,precagr)
                                                        } else {
                                                            unit = "-" + dataarr[i].UNITS
                                                            amount = "-" + dataarr[i].AMOUNT
                                                            temp2 = dataarr[i].UNITS;
                                                            //var cnav = dataarr[i].cnav;
                                                            for (var p = 0; p < arrunit.length; p++) {
                                                                if(arrunit[p] === 0){
                                                                    continue;
                                                                    }
                                                                temp3 = arrunit[p];
                                                                if (temp4 === 0) {
                                                                    console.log("if temp4 zero=", temp3 + "temp2=", temp2)
                                                                    temp4 = parseFloat(temp3) - parseFloat(temp2);
                                                                    temp4 = parseFloat(temp4.toFixed(4));
                                                                    console.log("if temp4 zero=", temp4)
                                                                    if (temp4 === 0) {
                                                                        arrunit[p] = 0;
                                                                        arrnav[p] = 0;
                                                                        arrpurchase[p] = 0;
                                                                        arrdays[p] = 0;
                                                                        alldays[p] = 0;
                                                                        sum1[p] = 0;
                                                                        sum2[p] = 0;
                                                                        break;
                                                                    } else if (temp4 > 0) {
                                                                        arrunit[p] = temp4;
                                                                        arrpurchase[p] = temp4 * arrnav[p];
                                                                        arrdays[p] = parseFloat(alldays[p] * arrpurchase[p]);

                                                                        sum1[p] = parseFloat(temp4 * arrnav[p]) * parseFloat(alldays[p]) * parseFloat((parseFloat(Math.pow(parseFloat((cnav * temp4) / (temp4 * arrnav[p])), parseFloat(1 / parseFloat(alldays[p] / 365)))) - 1) * 100);
                                                                        sum2[p] = parseFloat(arrpurchase[p]) * parseFloat(alldays[p]);
                                                                        console.log("unit=", arrunit)
                                                                        console.log("purchase=", arrpurchase, dataarr[i].SCHEME)
                                                                        break;
                                                                    } else {
                                                                        arrunit[p] = 0;
                                                                        arrnav[p] = 0;
                                                                        arrpurchase[p] = 0;
                                                                        arrdays[p] = 0;
                                                                        alldays[p] = 0;
                                                                        sum1[p] = 0;
                                                                        sum2[p] = 0;
                                                                    }
                                                                } else if (temp4 > 0) {//+ve
                                                                     console.log("if temp4 +ve temp2=", temp4 + ",temp2=", temp2)
                                                                    if (countlength === arrnav.length+1) {
                                                                       console.log("temp4 +ve counter length =", countlength + ",i=", i, p, arrnav.length+1)
                                                                       console.log("11235 line no", temp4 ,temp3)
                                                                       
                                                                        temp4 = parseFloat(temp4) + parseFloat(temp3);
                                                                        temp4 = parseFloat(temp4.toFixed(4));
                                                                       console.log("11237 line no", temp4 ,temp3)
                                                                        console.log("11241 line=",arrunit)
                                                                    }
                                                                    else {
                                                                        temp4 = parseFloat(temp4) - parseFloat(temp2);
                                                                        temp4 = parseFloat(temp4.toFixed(4));
                                                                        console.log("if temp4 +ve=", temp4)
                                                                    }

                                                                    if (temp4 === 0) {
                                                                        arrunit[p] = 0;
                                                                        arrnav[p] = 0;
                                                                        arrpurchase[p] = 0;
                                                                        arrdays[p] = 0;
                                                                        alldays[p] = 0;
                                                                        sum1[p] = 0;
                                                                        sum2[p] = 0;
                                                                        break;
                                                                    } else if (temp4 > 0) {
                                                                        arrunit[p] = temp4;
                                                                        arrpurchase[p] = temp4 * arrnav[p];
                                                                        arrdays[p] = parseFloat(alldays[p]) * arrpurchase[p];
                                                                        sum1[p] = parseFloat(temp4 * arrnav[p]) * parseFloat(alldays[p]) * parseFloat((parseFloat(Math.pow(parseFloat((cnav * temp4) / (temp4 * arrnav[p])), parseFloat(1 / parseFloat(alldays[p] / 365)))) - 1) * 100);
                                                                        sum2[p] = parseFloat(arrpurchase[p]) * parseFloat(alldays[p]);
                                                                        console.log("unit=", arrunit)
                                                                        console.log("purchase=", arrpurchase, dataarr[i].SCHEME)
                                                                        break;
                                                                    } else {
                                                                        arrunit[p] = 0;
                                                                        arrnav[p] = 0;
                                                                        arrpurchase[p] = 0;
                                                                        arrdays[p] = 0;
                                                                        alldays[p] = 0;
                                                                        sum1[p] = 0;
                                                                        sum2[p] = 0;
                                                                    }
                                                                } else {//-ve
                                                                    temp4 = parseFloat(temp4) + parseFloat(temp3);
                                                                    temp4 = parseFloat(temp4.toFixed(4));
                                                                    console.log("if temp4 -ve=", temp4)

                                                                    if (temp4 === 0) {
                                                                        arrunit[p] = 0;
                                                                        arrnav[p] = 0;
                                                                        arrpurchase[p] = 0;
                                                                        arrdays[p] = 0;
                                                                        alldays[p] = 0;
                                                                        sum1[p] = 0;
                                                                        sum2[p] = 0;
                                                                        console.log("unit=", arrunit)
                                                                        console.log("purchase=", arrpurchase, dataarr[i].SCHEME)
                                                                        break;
                                                                    } else if (temp4 > 0) {
                                                                        arrunit[p] = temp4;
                                                                        arrpurchase[p] = temp4 * arrnav[p];
                                                                        arrdays[p] = parseFloat(alldays[p]) * arrpurchase[p];
                                                                        sum1[p] = parseFloat(temp4 * arrnav[p]) * parseFloat(alldays[p]) * parseFloat((parseFloat(Math.pow(parseFloat((cnav * temp4) / (temp4 * arrnav[p])), parseFloat(1 / parseFloat(alldays[p] / 365)))) - 1) * 100);
                                                                        sum2[p] = parseFloat(arrpurchase[p]) * parseFloat(alldays[p]);
                                                                        console.log("unit=", arrunit)
                                                                        console.log("purchase=", arrpurchase, dataarr[i].SCHEME)

                                                                        if (countlength === arrnav.length+1) {
                                                                           console.log("dataarr.length =", countlength + ",i=", i, p, arrnav.length+1)
                                                                            arrunit[p] = 0;
                                                                            arrnav[p] = 0;
                                                                            arrpurchase[p] = 0;
                                                                            arrdays[p] = 0;
                                                                            alldays[p] = 0;
                                                                            sum1[p] = 0;
                                                                            sum2[p] = 0;
                                                                        }
                                                                        else {
                                                                            console.log("break dataarr.length =", countlength + ",i=", i, p, arrnav.length + 1)
                                                                            break;
                                                                        }

                                                                    } else {
                                                                        arrunit[p] = 0;
                                                                        arrnav[p] = 0;
                                                                        arrpurchase[p] = 0;
                                                                        arrdays[p] = 0;
                                                                        alldays[p] = 0;
                                                                        sum1[p] = 0;
                                                                        sum2[p] = 0;
                                                                        // break;
                                                                    }
                                                                }
                                                            }
                                                        }//else condition equal switch

                                                        balance = parseFloat(unit) + parseFloat(balance);

                                                        //cnav = dataarr[i].cnav;
                                                        if (cnav === "" || cnav === undefined || isNaN(balance) || isNaN(cnav)) {
                                                            balance = 0;
                                                            cnav = 0;
                                                        }
                                                        currentval = cnav * balance
                                                        type = dataarr[i].TYPE;
                                                    }//if match two scheme and folio array 

                                                } //dataarr inner loop
                                                console.log("****************")
                                                //console.log("sum1all=",sum1)
                                                temp22 = 0; temp33 = 0; var temp222 = 0;

                                                for (var k = 0; k < arrpurchase.length; k++) {
                                                    temp33 = Math.round(arrpurchase[k]);
                                                    temp22 = temp33 + temp22;
                                                    temp222 = Math.round(arrdays[k]) + temp222;
                                                }
                                                days = temp222 / temp22;
                                                var sum1all = 0; var sum2all = 0;
                                                
                                                for (var kk = 0; kk < sum1.length; kk++) {
                                                    sum1all = sum1[kk] + sum1all;
                                                }
                                                for (var kkk = 0; kkk < sum2.length; kkk++) {
                                                    sum2all = sum2[kkk] + sum2all;
                                                }

                                                var cagr = sum1all / sum2all;
                                                var absreturn = (((parseFloat(currentval) - parseFloat(temp22)) / parseFloat(temp22)) * 100).toFixed(2);
                                                gain = Math.round(currentval) - temp22;

                                                if (isNaN(cv) || cv < 0 || temp22 === 0 || balance === 0 || balance < 0 || days === 0 || isNaN(days)) {

                                                } else {
                                                    tot_mkt_value.push(Math.round(currentval));
                                                    tot_cost.push(temp22);
                                                    tot_gain.push(gain);
                                                    purchase.push({ name: datacon[a].NAME, pan: datacon[a].PAN, scheme: datacon[a].SCHEME, product_code: datacon[a].PRODCODE, folio: datacon[a].FOLIO, absolute_return: absreturn, unit: balance.toFixed(3), purchase_cost: temp22, mkt_value: Math.round(currentval), gain: gain, cagr: cagr.toFixed(1), avg_days: Math.round(days), type: type });
                                                }
                                            } // datascheme first loop
                                            for (var r = 0; r < purchase.length; r++) {
                                                sum11.push(purchase[r].purchase_cost * purchase[r].avg_days * purchase[r].cagr);
                                                sum22.push(purchase[r].purchase_cost * purchase[r].avg_days);
                                            }
                                            sum1all = 0; sum2all = 0;
                                            for (var kk = 0; kk < sum11.length; kk++) {
                                                sum1all = sum11[kk] + sum1all;
                                            }
                                            for (var kkk = 0; kkk < sum22.length; kkk++) {
                                                sum2all = sum22[kkk] + sum2all;
                                            }
                                            var sumcagr = sum1all / sum2all;
                                            var mkt = 0; var cost = 0; var totgain = 0;
                                            for (var p = 0; p < tot_mkt_value.length; p++) {
                                                mkt = tot_mkt_value[p] + mkt;
                                                cost = tot_cost[p] + cost;
                                                totgain = tot_gain[p] + totgain;
                                            }


                                            resdata.data = { portfolio_data: purchase, final_values: { tot_mkt_value: mkt, tot_cost: cost, tot_gain: totgain, tot_cagr: sumcagr.toFixed(1) } };

                                            res.json(resdata);

                                        } else {
                                            console.log("purchase=", "Data Not Found!")
                                            resdata = {
                                                status: 400,
                                                message: 'Data not found',
                                            }
                                            res.json(resdata);
                                        }
                                    })
                                }
                            })
                        })
                    });
                });
            }
        }

    } catch (err) {
        console.log(err)
    }
})

app.post("/api/getAmcListHoldingNatureWise", function (req, res) {
    try {
        let regex = /^([a-zA-Z]){5}([0-9]){4}([a-zA-Z]){1}?$/;
        if (req.body.pan === ""){
            resdata = {
                status: 400,
                message: 'Please enter pan !',
            }
            res.json(resdata);
            return resdata;
        }else if(!regex.test(req.body.pan)) {
            resdata = {
                status: 400,
                message: 'Please enter valid pan !',
            }
            res.json(resdata);
            return resdata;
        }else if(req.body.holder_nature === "") {
            resdata = {
                status: 400,
                message: 'Please enter holding nature !',
            }
            res.json(resdata);
            return resdata;
         } else{
                if(req.body.holder_nature ==="SI"){
                    pipeline = [//folio_franklin
                        { $match: { PANNO1: req.body.pan,HOLDING_T6:"Single" } },
                        { $group: { _id: { FOLIO_NO: "$FOLIO_NO", COMP_CODE: "$COMP_CODE" } } },
                        { $project: { _id: 0,folio: "$_id.FOLIO_NO",amc_code: "$_id.COMP_CODE" } },
                        { $sort: { amc_code: 1 } },
                    ]
                    pipeline1 = [  //folio_cams
                        { $match: { PAN_NO: req.body.pan ,HOLDING_NA:"SI"} },
                        { $group: { _id: { FOLIOCHK: "$FOLIOCHK", AMC_CODE: "$AMC_CODE" } } },
                        { $project: { _id: 0, folio: "$_id.FOLIOCHK", amc_code: "$_id.AMC_CODE", } },
                        { $sort: { amc_code: 1 } }
                    ]
                    pipeline2 = [ //folio_karvy
                        { $match: { PANGNO: req.body.pan, MODEOFHOLD:"SINGLE" } },
                        { $group: { _id: { ACNO: "$ACNO", FUND: "$FUND" } } },
                        { $project: { _id: 0, folio: "$_id.ACNO", amc_code: "$_id.FUND"  } },
                        { $sort: { amc_code: 1 } }
                    ]  
                    foliof.aggregate(pipeline, (err, newdata) => {
                        folioc.aggregate(pipeline1, (err, newdata1) => {
                            foliok.aggregate(pipeline2, (err, newdata2) => {
                                if (newdata2 != 0 || newdata1 != 0 || newdata != 0 ) {
                                    resdata = {
                                        status: 200,
                                        message: "Successfull",
                                        data: newdata2
                                    };
                               
                                var datacon = newdata.concat(newdata1.concat(newdata2));
                                datacon = datacon
                                    .map(JSON.stringify)
                                    .reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
                                    .filter(function (item, index, arr) {
                                        return arr.indexOf(item, index + 1) === -1;
                                    }) // check if there is any occurence of the item in whole array
                                    .reverse()
                                    .map(JSON.parse);
        
                                for (var i = 0; i < datacon.length; i++) {
                                    //console.log(datacon[i]['amc_code']);
                                    if (datacon[i]['amc_code'] != "" && datacon[i]['folio'] != "" && datacon[i]['scheme'] != "") {
        
                                        resdata.data = datacon[i];
        
                                    }
                                }
                                resdata.data = datacon.sort((a, b) => (a.amc_code > b.amc_code) ? 1 : -1);
                                res.json(resdata);
                                return resdata;
                            }else{
                                resdata = {
                                    status: 400,
                                    message: "Data not found!"
                                };
                                res.json(resdata);
                                return resdata;
                            }
                            });
                        });
                    });
            } else{
                if (req.body.holder_pan1 === ""){
                    resdata = {
                        status: 400,
                        message: 'Please enter joint holder pan!',
                    }
                    res.json(resdata);
                    return resdata;
                }else if(!regex.test(req.body.holder_pan1)) {
                    resdata = {
                        status: 400,
                        message: 'Please enter valid joint holder pan!',
                    }
                    res.json(resdata);
                    return resdata;

                }else{
                    pipeline = [//trans_franklin
                        { $match: {$and: [{ IT_PAN_NO1: req.body.pan},{IT_PAN_NO2:req.body.holder_pan1} ] } },
                        { $group: { _id: { FOLIO_NO: "$FOLIO_NO", COMP_CODE: "$COMP_CODE" } } },
                        { $project: { _id: 0,folio: "$_id.FOLIO_NO",amc_code: "$_id.COMP_CODE" } },
                        { $sort: { amc_code: 1 } },
                    ]
                    pipeline1 = [  //trans_cams
                        { $match: { PAN: req.body.pan } },
                        { $group: { _id: { FOLIO_NO: "$FOLIO_NO", AMC_CODE: "$AMC_CODE" } } },
                        { $project: { _id: 0, folio: "$_id.FOLIO_NO", amc_code: "$_id.AMC_CODE", } },
                        { $sort: { amc_code: 1 } }
                    ]
                    pipeline2 = [ //trans_karvy
                        { $match: {$and: [ { PAN1: req.body.pan},{PAN2:req.body.holder_pan1} ]  } },
                        { $group: { _id: { TD_ACNO: "$TD_ACNO", TD_FUND: "$TD_FUND" } } },
                        { $project: { _id: 0, folio: "$_id.TD_ACNO", amc_code: "$_id.TD_FUND"  } },
                        { $sort: { amc_code: 1 } }
                    ]  
                    transf.aggregate(pipeline, (err, newdata) => {
                        transc.aggregate(pipeline1, (err, newdata1) => {
                            transk.aggregate(pipeline2, (err, newdata2) => {
                                if (newdata2 != 0 || newdata1 != 0 || newdata != 0 ) {
                                    resdata = {
                                        status: 200,
                                        message: "Successfull",
                                        data: newdata2
                                    };
                               
                                var datacon = newdata.concat(newdata1.concat(newdata2));
                                datacon = datacon
                                    .map(JSON.stringify)
                                    .reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
                                    .filter(function (item, index, arr) {
                                        return arr.indexOf(item, index + 1) === -1;
                                    }) // check if there is any occurence of the item in whole array
                                    .reverse()
                                    .map(JSON.parse);
        
                                for (var i = 0; i < datacon.length; i++) {
                                    //console.log(datacon[i]['amc_code']);
                                    if (datacon[i]['amc_code'] != "" && datacon[i]['folio'] != "" && datacon[i]['scheme'] != "") {
        
                                        resdata.data = datacon[i];
        
                                    }
                                }
                                resdata.data = datacon.sort((a, b) => (a.amc_code > b.amc_code) ? 1 : -1);
                                res.json(resdata);
                                return resdata;
                            } else {
                                resdata = {
                                    status: 400,
                                    message: "Data not found!"
                                };
                                res.json(resdata);
                                return resdata;
                            }
                            });
                        });
                    });
                }
            }
           
        }
    } catch (err) {
        console.log(e)
    }
});


app.post("/api/isPANexist", function (req, res) {
    try {
        if (req.body.memberPan === "") {
            resdata = {
                status: 400,
                message: 'Please enter pan',
            }
            res.json(resdata)
            return resdata;
        } else {
            let regex = /^([a-zA-Z]){5}([0-9]){4}([a-zA-Z]){1}?$/;
            if (!regex.test(req.body.memberPan)) {
                resdata = {
                    status: 400,
                    message: 'Please enter valid pan',
                }
                res.json(resdata);
                return resdata;
            } else {
                foliok.find({ PANGNO: req.body.memberPan }, { _id: 0, EMAIL: 1 }, function (err, foliokarvydata) {
                    folioc.find({ PAN_NO: req.body.memberPan }, { _id: 0, EMAIL: 1 }, function (err, foliocamsdata) {
                        foliof.find({ PANNO1: req.body.memberPan }, { _id: 0, EMAIL: 1 }, function (err, foliofranklindata) {
                            if (foliokarvydata != "" || foliocamsdata != "" || foliofranklindata != "") {
                                resdata = {
                                    status: 200,
                                    message: 'Successful',
                                    data: foliofranklindata,
                                }
                                datacon = foliokarvydata.concat(foliocamsdata.concat(foliofranklindata));
                                datacon = datacon.map(JSON.stringify).reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
                                    .filter(function (item, index, arr) { return arr.indexOf(item, index + 1) === -1; }) // check if there is any occurence of the item in whole array
                                    .reverse().map(JSON.parse);

                                res.json(resdata)
                                return resdata;
                            } else {
                                resdata = {
                                    status: 400,
                                    message: 'PAN not found!',
                                }
                                res.json(resdata)
                                return resdata;
                            }
                        });
                    });
                });
            }
        }
    } catch (err) {
        console.log(err)
    }
});

app.post("/api/userProfileMemberList", function (req, res) {
    try{
        var arr1=[];var arr2=[];var arr3=[];
        let regex = /^([a-zA-Z]){5}([0-9]){4}([a-zA-Z]){1}?$/;
        if(req.body.pan ===""){
            resdata = {
                status: 400,
                message: 'Please enter pan',
            }
            res.json(resdata);
            return resdata;
        }else if(!regex.test(req.body.pan)) {
            resdata = {
                status: 400,
                message: 'Please enter valid pan',
            }
            res.json(resdata);
            return resdata;
        }else{
            family.find({ adminPan:  {$regex : `^${req.body.pan}.*` , $options: 'i' }  },{_id:0,memberPan:1}, function (err, member) {
                if(member!=""){
                    member  = [...new Set(member.map(({memberPan}) => memberPan.toUpperCase()))];
                    arr1.push({PAN:req.body.pan.toUpperCase()});
                    arr2.push({PAN1:req.body.pan.toUpperCase()});
                    arr3.push({IT_PAN_NO1:req.body.pan.toUpperCase()});
                    for(var j=0;j<member.length;j++){     
                    arr1.push({PAN:member[j]}); 
                    arr2.push({PAN1:member[j]}); 
                    arr3.push({IT_PAN_NO1:member[j]});
                    }
                    var strPan = {$or:arr1};
                    var strPan1 = {$or:arr2};
                    var strPan2 = {$or:arr3};
     pipeline = [  ///trans_cams
        { $match: strPan},
        { $group: { _id: {PAN: "$PAN",  INV_NAME: "$INV_NAME", TAX_STATUS:"$TAX_STATUS",} } },
        { $lookup: { from: 'folio_cams', localField: '_id.PAN', foreignField: 'PAN_NO', as: 'detail' } },
        { $unwind: "$detail" },
        { $project: { _id: 0,PAN: "$_id.PAN",  INVNAME: { "$toUpper": ["$_id.INV_NAME"] } ,PER_STATUS:{ "$toUpper": ["$_id.TAX_STATUS" ] },JOINT_NAME1: { "$toUpper": ["$detail.JNT_NAME1"] },JOINT_NAME2: { "$toUpper": ["$detail.JNT_NAME2"] } } },     
    ]
     pipeline1 = [  ///trans_karvy
        { $match: strPan1},
        { $group: { _id: { PAN1: "$PAN1",  INVNAME: "$INVNAME" ,STATUS:"$STATUS" } } },
        { $lookup: { from: 'folio_karvy', localField: '_id.PAN1', foreignField: 'PANGNO', as: 'detail' } },
        { $unwind: "$detail" },
        { $project: { _id: 0,  PAN: "$_id.PAN1", INVNAME: { "$toUpper": ["$_id.INVNAME"] },PER_STATUS:{ "$toUpper": ["$_id.STATUS" ] },JOINT_NAME1: { "$toUpper": ["$detail.JTNAME1"] },JOINT_NAME2: { "$toUpper": ["$detail.JTNAME2"] } } },
       
    ]
     pipeline2 = [  ///trans_franklin
        { $match: strPan2},
        { $group: { _id: { IT_PAN_NO1: "$IT_PAN_NO1", INVESTOR_2:"$INVESTOR_2",SOCIAL_S18:"$SOCIAL_S18",JOINT_NAM1:"$JOINT_NAM1",JOINT_NAM2:"$JOINT_NAM2" } } },
        { $project: { _id: 0,PAN: "$_id.IT_PAN_NO1",  INVNAME: { "$toUpper": ["$_id.INVESTOR_2"] } ,PER_STATUS:{ "$toUpper": ["$_id.SOCIAL_S18"] },JOINT_NAME1:{ "$toUpper": ["$_id.JOINT_NAM1"] },JOINT_NAME2:{ "$toUpper": ["$_id.JOINT_NAM2"] } } },
       
    ]
    
   transc.aggregate(pipeline, (err, camsdata) => {
         transk.aggregate(pipeline1, (err, karvydata) => {
              transf.aggregate(pipeline2, (err, frankdata) => {
                if ( camsdata != 0 || karvydata != 0 || frankdata!= 0) {
                    resdata = {
                        status: 200,
                        message: 'Successfull',
                        data: camsdata
                    }
                
                var datacon = frankdata.concat(karvydata.concat(camsdata)) ;              
                         for (var i = 0; i < datacon.length; i++) {                                          
                          if (datacon[i]['PER_STATUS'] === "On Behalf Of Minor" || datacon[i]['PER_STATUS'] === "MINOR" || datacon[i]['PER_STATUS'] === "On Behalf of Minor" 
                          || datacon[i]['PER_STATUS'] === "ON BEHALF OF MINOR" )  {
                             datacon[i]['PER_STATUS'] = "MINOR";      
                         }if (datacon[i]['PER_STATUS'] === "INDIVIDUAL" || datacon[i]['PER_STATUS'] === "Individual"|| datacon[i]['PER_STATUS'] === "Resident Individual" || datacon[i]['PER_STATUS'] === "RESIDENT INDIVIDUAL" ) {
                             datacon[i]['PER_STATUS'] = "INDIVIDUAL";
                         }if (datacon[i]['PER_STATUS'] === "HINDU UNDIVIDED FAMI") {
                            datacon[i]['PER_STATUS'] = "HUF";
                          }
                     }
                     datacon = datacon.map(JSON.stringify).reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
                     .filter(function (item, index, arr) { return arr.indexOf(item, index + 1) === -1; }) // check if there is any occurence of the item in whole array
                     .reverse().map(JSON.parse);
                     datacon = Array.from(new Set(datacon));
                     var newdata1 = datacon.map(item=>{
                        return [JSON.stringify(item),item]
                         }); // creates array of array
                         var maparr1 = new Map(newdata1); // create key value pair from array of array
                         datacon = [...maparr1.values()];//converting back to array from mapobject 
                         var filtered = datacon.filter(
                            (temp => a =>
                                (k => !temp[k] && (temp[k] = true))(a.INVNAME + '|' + a.PER_STATUS)
                            )(Object.create(null))
                        );
                    
                     resdata.data = filtered;
                  res.json(resdata);
                return resdata;
            } else {
                resdata = {
                    status: 400,
                    message: 'Data not found',
                }
                res.json(resdata);
                return resdata;
            }
            });
         });
     });
}else{
  pipeline = [  ///trans_cams
        { $match: {PAN:{$regex : `^${req.body.pan}.*` , $options: 'i' } } },
        { $group: { _id: {PAN: "$PAN",  INV_NAME: "$INV_NAME", TAX_STATUS:"$TAX_STATUS",} } },
        { $lookup: { from: 'folio_cams', localField: '_id.PAN', foreignField: 'PAN_NO', as: 'detail' } },
        { $unwind: "$detail" },
        { $project: { _id: 0,PAN: "$_id.PAN",  INVNAME: { "$toUpper": ["$_id.INV_NAME"] } ,PER_STATUS:{ "$toUpper": ["$_id.TAX_STATUS" ] },JOINT_NAME1: { "$toUpper": ["$detail.JNT_NAME1"] },JOINT_NAME2: { "$toUpper": ["$detail.JNT_NAME2"] } } },     
    ]
     pipeline1 = [  ///trans_karvy
        { $match: {PAN1:{$regex : `^${req.body.pan}.*` , $options: 'i' } } },
        { $group: { _id: { PAN1: "$PAN1",  INVNAME: "$INVNAME" ,STATUS:"$STATUS" } } },
        { $lookup: { from: 'folio_karvy', localField: '_id.PAN1', foreignField: 'PANGNO', as: 'detail' } },
        { $unwind: "$detail" },
        { $project: { _id: 0,  PAN: "$_id.PAN1", INVNAME: { "$toUpper": ["$_id.INVNAME"] },PER_STATUS:{ "$toUpper": ["$_id.STATUS" ] },JOINT_NAME1: { "$toUpper": ["$detail.JTNAME1"] },JOINT_NAME2: { "$toUpper": ["$detail.JTNAME2"] } } },
       
    ]
     pipeline2 = [  ///trans_franklin
        { $match: {IT_PAN_NO1:{$regex : `^${req.body.pan}.*` , $options: 'i' } } },
        { $group: { _id: { IT_PAN_NO1: "$IT_PAN_NO1", INVESTOR_2:"$INVESTOR_2",SOCIAL_S18:"$SOCIAL_S18",JOINT_NAM1:"$JOINT_NAM1",JOINT_NAM2:"$JOINT_NAM2" } } },
        { $project: { _id: 0,PAN: "$_id.IT_PAN_NO1",  INVNAME: { "$toUpper": ["$_id.INVESTOR_2"] } ,PER_STATUS:{ "$toUpper": ["$_id.SOCIAL_S18"] },JOINT_NAME1:{ "$toUpper": ["$_id.JOINT_NAM1"] },JOINT_NAME2:{ "$toUpper": ["$_id.JOINT_NAM2"] } } },
       
    ]
    transc.aggregate(pipeline, (err, camsdata) => {
        transk.aggregate(pipeline1, (err, karvydata) => {
             transf.aggregate(pipeline2, (err, frankdata) => {
               if ( camsdata != 0 || karvydata != 0 || frankdata!= 0) {
                   resdata = {
                       status: 200,
                       message: 'Successfull',
                       data: camsdata
                   }
               
               var datacon = frankdata.concat(karvydata.concat(camsdata)) ;              
                        for (var i = 0; i < datacon.length; i++) {                                          
                         if (datacon[i]['PER_STATUS'] === "On Behalf Of Minor" || datacon[i]['PER_STATUS'] === "MINOR" || datacon[i]['PER_STATUS'] === "On Behalf of Minor" 
                         || datacon[i]['PER_STATUS'] === "ON BEHALF OF MINOR" )  {
                            datacon[i]['PER_STATUS'] = "MINOR";      
                        }if (datacon[i]['PER_STATUS'] === "INDIVIDUAL" || datacon[i]['PER_STATUS'] === "Individual"|| datacon[i]['PER_STATUS'] === "Resident Individual" || datacon[i]['PER_STATUS'] === "RESIDENT INDIVIDUAL" ) {
                            datacon[i]['PER_STATUS'] = "INDIVIDUAL";
                        }if (datacon[i]['PER_STATUS'] === "HINDU UNDIVIDED FAMI") {
                           datacon[i]['PER_STATUS'] = "HUF";
                         }
                    }
                    datacon = datacon.map(JSON.stringify).reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
                    .filter(function (item, index, arr) { return arr.indexOf(item, index + 1) === -1; }) // check if there is any occurence of the item in whole array
                    .reverse().map(JSON.parse);
                    datacon = Array.from(new Set(datacon));
                    var newdata1 = datacon.map(item=>{
                       return [JSON.stringify(item),item]
                        }); // creates array of array
                        var maparr1 = new Map(newdata1); // create key value pair from array of array
                        datacon = [...maparr1.values()];//converting back to array from mapobject 
                        var filtered = datacon.filter(
                           (temp => a =>
                               (k => !temp[k] && (temp[k] = true))(a.INVNAME + '|' + a.PER_STATUS)
                           )(Object.create(null))
                       );
                   
                    resdata.data = filtered;
                 res.json(resdata);
               return resdata;
           } else {
            resdata = {
                status: 400,
                message: 'Data not found',
            }
            res.json(resdata);
            return resdata;
           }
           });
        });
    });
}
            });
  }
} catch (err) {
    console.log(err)
}
})

app.post("/api/getfolio", function (req, res) {
    try{
        var perstatus = req.body.per_status;
        var statusvalue ="Minor";
        if(perstatus.toLowerCase() === statusvalue.toLowerCase()){

    pipeline1 = [  //folio_cams
        { $match:{  GUARD_PAN:req.body.pan,INV_NAME:{$regex : `^${req.body.name}.*` , $options: 'i' } } },
        { $group: { _id: { FOLIOCHK: "$FOLIOCHK", AMC_CODE:"$AMC_CODE" } } },
        { $project: { _id: 0, amc_code: "$_id.AMC_CODE" , folio:"$_id.FOLIOCHK"  } },
        { $sort: {amc_code:1}}
    ]
    pipeline2 = [  //trans_karvy
        { $match:{ GUARDPANNO:req.body.pan,INVNAME:{$regex : `^${req.body.name}.*` , $options: 'i' }} },
        { $group: { _id: { ACNO: "$ACNO", FUND:"$FUND" } } },
        { $project: { _id: 0, amc_code: "$_id.FUND" , folio:"$_id.ACNO"  } },
        { $sort: {amc_code:1}}
    ]
    pipeline3 = [  //trans_franklin
        { $match:{ GUARDIAN20:req.body.pan,INV_NAME:{$regex : `^${req.body.name}.*` , $options: 'i' }} },
        { $group: { _id: { FOLIO_NO: "$FOLIO_NO", COMP_CODE:"$COMP_CODE" } } },
        { $project: { _id: 0, amc_code: "$_id.COMP_CODE" , folio:"$_id.FOLIO_NO"  } },
        { $sort: {amc_code:1}}
    ]
    folioc.aggregate(pipeline1, (err, camsdata) => {
        foliok.aggregate(pipeline2, (err, karvydata) => {
            foliof.aggregate(pipeline3, (err, frankdata) => {
      if (frankdata.length != 0 || karvydata.length != 0 || camsdata.length != 0) {
            resdata = {
                status: 200,
                message: 'Successful',
                data:frankdata
            }
         var datacon = frankdata.concat(karvydata.concat(camsdata));
         var removeduplicates = Array.from(new Set(datacon));
         datacon = removeduplicates.map(JSON.stringify)
             .reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
             .filter(function (item, index, arr) {
                 return arr.indexOf(item, index + 1) === -1;
             }) // check if there is any occurence of the item in whole array
             .reverse()
             .map(JSON.parse);
             resdata.data = datacon.sort((a, b) => (a.amc_code > b.amc_code) ? 1 : -1);
         res.send(resdata);
         return resdata;
     }else{
        resdata = {
            status: 400,
            message: 'Data not found',
        }
        res.send(resdata);
        return resdata;
     }
 });
});
});
}else{
    pipeline1 = [  //trans_cams
        { $match:{  PAN:req.body.pan,INV_NAME:{$regex : `^${req.body.name}.*` , $options: 'i' } } },
        { $group: { _id: { FOLIO_NO: "$FOLIO_NO", AMC_CODE:"$AMC_CODE" } } },
        { $project: { _id: 0, amc_code: "$_id.AMC_CODE" , folio:"$_id.FOLIO_NO"  } },
        { $sort: {amc_code:1}}
    ]
    pipeline2 = [  //trans_karvy
        { $match:{ PAN1:req.body.pan,INVNAME:{$regex : `^${req.body.name}.*` , $options: 'i' }} },
        { $group: { _id: { TD_ACNO: "$TD_ACNO", TD_FUND:"$TD_FUND" } } },
        { $project: { _id: 0, amc_code: "$_id.TD_FUND" , folio:"$_id.TD_ACNO"  } },
        { $sort: {amc_code:1}}
    ]
    pipeline3 = [  //trans_franklin
        { $match:{ IT_PAN_NO1:req.body.pan,INVESTOR_2:{$regex : `^${req.body.name}.*` , $options: 'i' }} },
        { $group: { _id: { FOLIO_NO: "$FOLIO_NO", COMP_CODE:"$COMP_CODE" } } },
        { $project: { _id: 0, amc_code: "$_id.COMP_CODE" , folio:"$_id.FOLIO_NO"  } },
        { $sort: {amc_code:1}}
    ]
    transc.aggregate(pipeline1, (err, camsdata) => {
        transk.aggregate(pipeline2, (err, karvydata) => {
            transf.aggregate(pipeline3, (err, frankdata) => {
      if (frankdata.length != 0 || karvydata.length != 0 || camsdata.length != 0) {
        resdata = {
            status: 200,
            message: 'Successful',
            data:frankdata
        }
         var datacon = frankdata.concat(karvydata.concat(camsdata))
         var removeduplicates = Array.from(new Set(datacon));
         datacon = removeduplicates.map(JSON.stringify)
             .reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
             .filter(function (item, index, arr) {
                 return arr.indexOf(item, index + 1) === -1;
             }) // check if there is any occurence of the item in whole array
             .reverse()
             .map(JSON.parse);
             resdata.data = datacon.sort((a, b) => (a.amc_code > b.amc_code) ? 1 : -1);
         res.send(resdata);
         return resdata;
     }else{
        resdata = {
            status: 400,
            message: 'Data not found',
        }
        res.send(resdata);
        return resdata;
     }
 });
});
});

 }
} catch (err) {
    console.log(err)
}
})



app.post("/api/PANVerification", function (req, res) {
    try{
        let regex = /^([a-zA-Z]){5}([0-9]){4}([a-zA-Z]){1}?$/;
        if (req.body.memberPan === "") {
            resdata = {
                status: 400,
                message: 'Please enter member pan!',
            }
            res.json(resdata)
            return resdata;
        } else if (!regex.test(req.body.memberPan)) {
            resdata = {
                status: 400,
                message: 'Please enter valid member pan',
            }
            res.json(resdata);
            return resdata;
        }else if(req.body.adminPan === "") {
            resdata = {
                status: 400,
                message: 'Please enter admin pan!',
            }
            res.json(resdata);
            return resdata;
        } else if (!regex.test(req.body.adminPan)) {
            resdata = {
                status: 400,
                message: 'Please enter valid admin pan',
            }
            res.json(resdata);
            return resdata;
        }else if (req.body.memberPan === req.body.adminPan) {
            resdata = {
                status: 400,
                message: 'Admin Pan & Family member Pan must not be same!',
            }
            res.json(resdata);
        } else {
            //var mod = new family({ memberPan: req.body.memberPan });
            
            // family.find({ memberPan: req.body.memberPan }).distinct("memberPan", function (err, memberdata) {
                family.find({ memberPan: req.body.memberPan,adminPan:req.body.adminPan}, { _id: 0 }, function (err, memberdata) {
			if (memberdata.length === 0) {
         foliok.find({ PANGNO: req.body.memberPan }, { _id: 0, EMAIL: 1, Name: "$INVNAME",Phone:"$MOBILE" }, function (err, foliokarvydata) {
                    folioc.find({ PAN_NO: req.body.memberPan }, { _id: 0, EMAIL: 1, Name: "$INV_NAME",Phone:"$MOBILE_NO" }, function (err, foliocamsdata) {
                        foliof.find({ PANNO1: req.body.memberPan }, { _id: 0, EMAIL: 1, Name: "$INV_NAME",Phone:"$PHONE_RES" }, function (err, foliofranklindata) {
                    if(foliokarvydata !="" || foliocamsdata !="" || foliofranklindata !="") {
                        resdata = {
                            status: 200,
                            message: 'Successful',
                            data:foliofranklindata,
                        }
                        datacon = foliokarvydata.concat(foliocamsdata.concat(foliofranklindata));
                        datacon = datacon.map(JSON.stringify).reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
                        .filter(function (item, index, arr) { return arr.indexOf(item, index + 1) === -1; }) // check if there is any occurence of the item in whole array
                        .reverse().map(JSON.parse);
                        var digits = '0123456789';
                        let OTP = '';
                        for (let k = 0; k < 6; k++ ) {
                            OTP += digits[Math.floor(Math.random() * 10)];
                        }
                        
                        localStorage.setItem('otp', OTP);
                        localStorage.setItem('memberPan', req.body.memberPan );
			      var name =datacon[0].Name;
			    var phone= datacon[0].Phone;
			  //  var phone="8859908258";
// 				 for(var j=0;j<datacon.length;j++){
//                                     datacon[j].EMAIL = datacon[j].EMAIL.toUpperCase();
//                                     datacon[j].Name = datacon[j].Name.toUpperCase();
//                                 }
// 			    datacon = datacon.filter(
//                                     (temp => a =>
//                                         (k => !temp[k] && (temp[k] = true))(a.EMAIL + '|'+ a.Phone)
//                                     )(Object.create(null))
//                                 );
                             //var name= datacon[0].Name;
                      //  for(var j=0;j<datacon.length;j++){
                            //datacon[j].Phone=datacon[j].Phone;
// 				datacon[j].EMAIL = datacon[j].EMAIL.toUpperCase();
// 				datacon[j].Name = datacon[j].Name.toUpperCase();
                                    Axios.get("http://nimbusit.biz/api/SmsApi/SendSingleApi?UserID=bfccapital&Password=obmh6034OB&SenderID=BFCCAP&EntityID=1201160224111799498&TemplateID=1207162615727439769&Phno=" +phone+ "&Msg=Dear "+name+",Your Family Member has requested to link your Mutual Fund Investment Folio(s) to his/her portfolio.You can authorize the same by sharing the OTP - "+OTP+" with your family member. BFC Capital.",
                                ).then(function (result) {
                                    console.log('SMS send successfully',phone);
                                });
                     //   }
                       resdata.data  = [...new Set(datacon.map(({EMAIL}) => EMAIL.toLowerCase()))]
                        //resdata.data = datacon;
                        for(var j=0;j<resdata.data.length;j++){
                            var toemail = resdata.data[j];
                         var transporter = nodemailer.createTransport({ 
                           host: 'mail.bfccapital.com',
                            port: 465,
                            secure: true, 
                            auth: {
                              user: "customersupport@bfccapital.com",
                              pass: "customersupport@123"
                            }
                          }); 
                          transporter.verify(function (error, success) {
                            if (error) {
                              console.log("Error");
                            } else {
                              console.log("Server is ready to take our messages!");
                            }
                          });
  
                        let mailOptions = {
                        //    from: "customersupport@bfccapital.com",
			      from: {
				    name: 'BFC Capital',
				    address: 'customersupport@bfccapital.com'
			     },
                            to: toemail,
                            cc: "pallavisinghbfcinfotech@gmail.com",
                            subject: "Mapping of Family Member's Folio(s)",
                            html: "Dear "+name+",<br><br>Your Family Member has requested to link your Mutual Fund Investment Folio(s) to his/her portfolio.You can authorize the same by sharing the OTP- <b>"+OTP+"</b> with your family member. <br><br>Note - By accepting this request, your family member can view your investment folios and initiate transactions on your behalf and such transactions will be processed only after your approval subject to your payment confirmation or OTP confirmation wherever applicable.<br><br>In case you need any clarification, please contact us on 9506031502 or you can also email to customersupport@bfccapital.com, quoting your PAN, mobile no. and your query.<br><br>Thanks & Regards<br>Team BFC Capital"
                          }
                          transporter.sendMail(mailOptions, function (error, info) {
                            if (error) {
                              console.log(error);
                            } else {
                              console.log('Email sent: ' + info.response);
                            }
                          });
                        }
                        res.json(resdata)
                        return resdata;
                    }else{
                        resdata = {
                            status: 400,
                            message: 'Data not found',
                        }
                        res.json(resdata)
                        return resdata;
                    }
                 });
            });
       });           
		 } else{
                resdata = {
                    status: 400,
                       message: 'Member pan & admin pan already exists',
                }
                res.json(resdata)
                return resdata;
            }
		    });
    }
    } catch (err) {
        console.log(err)
    }
});

app.post("/api/verifiyPanOtpAddFamily", function (req, res) {
    try {
        if (req.body.adminPan === "") {
            resdata = {
                status: 400,
                message: 'Please enter admin pan!',
            }
            res.json(resdata);
            return resdata;
        } else if (req.body.memberPan === "") {
            resdata = {
                status: 400,
                message: 'Please enter member pan!',
            }
            res.json(resdata);
		return resdata;
        }else if (req.body.memberRelation === "") {
            resdata = {
                status: 400,
                message: 'Please enter member relation!',
            }
            res.json(resdata);
            return resdata;
         } else if (req.body.otp === "") {
            resdata = {
                status: 400,
                message: 'Please enter otp!',
            }
            res.json(resdata);
            return resdata;
        } else if (req.body.memberPan === req.body.adminPan) {
            resdata = {
                status: 400,
                message: 'Admin Pan & Family member Pan must not be same!',
            }
            res.json(resdata);
		return resdata
        }else {
            var memberdata="";
                 var OTP = localStorage.getItem('otp');
                 var memberPan = localStorage.getItem('memberPan'); 
                 if(memberPan!= req.body.memberPan){
                    resdata = {
                        status: 400,
                        message: 'Member Pan not match!',
                    }
                    res.json(resdata);
                    return resdata;
                    
                }if(OTP!= req.body.otp){
                    resdata = {
                        status: 400,
                        message: 'OTP not matched!',
                    }
                    res.json(resdata);
                    return resdata;
                }else{
                    try {
                     //   for (i = 0; i < req.body.length; i++) {
                            var mod = new family({memberPan:memberPan,adminPan:req.body.adminPan,memberRelation:req.body.memberRelation});
                            family.find({ memberPan: req.body.memberPan , adminPan:req.body.adminPan },{_id:0}, function (err, memberdata) {
                                if(memberdata !=""){
                                    resdata = {
                                        status: 400,
                                        message: 'Family member already exist!',
                                    }
                                    res.json(resdata)
                                    return resdata;
                                }else{
                                    mod.save(function (err, data) {
                                        if (err) {
                                            console.log(err);
                                        }
                                        else {
                                            console.log("insert successfully");
                                        }
                                    });
                                    resdata = {
                                        status: 200,
                                        message: 'insert successfully',
                                    }
                                    res.json(resdata)
                                    return resdata;
                                }
                            });
                    } catch (err) {
                        console.log(err)
                    }
                 }            
        }
        
    } catch (err) {
        console.log(err)
    }
});

app.post("/api/getfoliodetail", function (req, res) {
    try{
    var balance = 0; var currentvalue = 0; var amt = 0; var cnav = 0;
    var dataarr = [];var bank = "";var acno="";var joint1="";var joint2="";
    var nominee="";var invname="";var folio="";var scheme="";var nature="";
    var navdate="";var isin = "";
    var pcode = req.body.amc_code + req.body.prodcode;
    const pipeline1 = [  //trans_cams
        { $match: { FOLIO_NO: req.body.folio, AMC_CODE: req.body.amc_code, PRODCODE: pcode } },
        { $group: { _id: { INV_NAME: "$INV_NAME",FOLIO_NO:"$FOLIO_NO",SCHEME:"$SCHEME",TRXN_TYPE_:"$TRXN_TYPE_", BANK_NAME: "$BANK_NAME", AC_NO: "$AC_NO", AMC_CODE: "$AMC_CODE", PRODCODE: "$PRODCODE", code: { $substr: ["$PRODCODE", { $strLenCP: "$AMC_CODE" }, -1] } } , UNITS: { $sum: "$UNITS" }, AMOUNT: { $sum: "$AMOUNT" }  } },
        {
            $lookup:
            {
                from: "products",
                let: { ccc: "$_id.code", amc: "$_id.AMC_CODE" },
                pipeline: [
                    {
                        $match:
                        {
                            $expr:
                            {
                                $and:
                                    [
                                        { $eq: ["$PRODUCT_CODE", "$$ccc"] },
                                        { $eq: ["$AMC_CODE", "$$amc"] }
                                    ]
                            }
                        }
                    },
                    { $project: { _id: 0 } }
                ],
                as: "products"
            }
        },
        { $unwind: "$products" },
        { $lookup: { from: 'cams_nav', localField: 'products.ISIN', foreignField: 'ISINDivPayoutISINGrowth', as: 'nav' } },
        { $unwind: "$nav" },
        { $lookup: { from: 'folio_cams', localField: '_id.FOLIO_NO', foreignField: 'FOLIOCHK', as: 'detail' } },
        { $project: { _id: 0, FOLIO: "$_id.FOLIO_NO", INVNAME: "$_id.INV_NAME", SCHEME: "$_id.SCHEME", NATURE: "$_id.TRXN_TYPE_", navdate: { $dateToString: {  format: "%Y/%m/%d", date: "$nav.Date" } }, SCHEMEISIN: "$products.ISIN", NOMINEE: "$detail.NOM_NAME", JTNAME2: "$detail.JNT_NAME2", JTNAME1: "$detail.JNT_NAME1",BANK_NAME: "$_id.BANK_NAME", AC_NO: "$_id.AC_NO",  cnav: "$nav.NetAssetValue", UNITS: { $sum: "$UNITS" }, AMOUNT: { $sum: "$AMOUNT" } } },
    ]
   

    const pipeline2 = [  //trans_karvy
        { $match:  {TD_ACNO: req.body.folio ,SCHEMEISIN:req.body.isin } },
        { $group: { _id: { TD_ACNO: "$TD_ACNO", INVNAME: "$INVNAME", FUNDDESC: "$FUNDDESC", TD_TRTYPE: "$TD_TRTYPE", TD_TRDT: "$TD_TRDT", SCHEMEISIN: "$SCHEMEISIN" }, TD_UNITS: { $sum: "$TD_UNITS"}, TD_AMT: { $sum:"$TD_AMT" } } },
        {
            $lookup:
            {
                from: "cams_nav",
                let: { isin: "$_id.SCHEMEISIN" },
                pipeline: [
                    {
                        $match:
                        {
                            $expr:
                            {
                                $or:
                                    [
                                        { $eq: ["$ISINDivPayoutISINGrowth", "$$isin"] },
                                        { $eq: ["$ISINDivReinvestment", "$$isin"] }
                                    ]
                            }
                        }
                    },
                    { $project: { _id: 0 } }
                ],
                as: "nav"
            }
        },
        { $unwind: "$nav" },
        { $lookup: { from: 'folio_karvy', localField: '_id.TD_ACNO', foreignField: 'ACNO', as: 'detail' } },
        { $project: { _id: 0, FOLIO: "$_id.TD_ACNO", INVNAME: "$_id.INVNAME", SCHEME: "$_id.FUNDDESC", NATURE: "$_id.TD_TRTYPE", navdate: { $dateToString: { format: "%Y/%m/%d", date: "$nav.Date" } }, SCHEMEISIN: "$_id.SCHEMEISIN", NOMINEE: "$detail.NOMINEE", BANK_NAME: "$detail.BNAME", AC_NO: "$detail.BNKACNO", JTNAME2: "$detail.JTNAME2", JTNAME1: "$detail.JTNAME1", cnav: "$nav.NetAssetValue", UNITS: { $sum: "$TD_UNITS"} , AMOUNT: { $sum:"$TD_AMT" } } },
    ]

    
    transc.aggregate(pipeline1, (err, camsdata) => {
        transk.aggregate(pipeline2, (err, karvydata) => {
               if ( karvydata != 0 || camsdata != 0 ) {
                    resdata = {
                        status: 200,
                        message: "Successfull",
                        data: karvydata
                    };
                  
                    var datacon = camsdata.concat(karvydata);

                    datacon = datacon
                        .map(JSON.stringify)
                        .reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
                        .filter(function (item, index, arr) {
                            return arr.indexOf(item, index + 1) === -1;
                        }) // check if there is any occurence of the item in whole array
                        .reverse()
                        .map(JSON.parse);
                        datacon = Array.from(new Set(datacon));
                        var unit = 0;
                      for (var i = 0; i < datacon.length; i++) {
                        if (datacon[i]['NATURE'] === "RED" || datacon[i]['NATURE'] === "LTOP" || datacon[i]['NATURE'] === "Lateral Shift Out" ||
                            datacon[i]['NATURE'] === "Switch Out" || datacon[i]['NATURE'] === "IPOR" || datacon[i]['NATURE'] === "LTOF" ||
                            datacon[i]['NATURE'] === "FUL" || datacon[i]['NATURE'] === "STPO" || datacon[i]['NATURE'] === "CNO" ||
                            datacon[i]['NATURE'] === "FULR" || datacon[i]['NATURE'] === "Full Redemption" || datacon[i]['NATURE'] === "Partial Switch Out"
                            || datacon[i]['NATURE'] === "Full Switch Out" || datacon[i]['NATURE'] === "Partial Redemption"
                            || datacon[i]['NATURE'] === "SWD" || datacon[i]['NATURE'] === "SWOF" || datacon[i]['NATURE'] === "TOCOB") {
                            unit = "-" + datacon[i].UNITS;
                        } else {
                            unit = datacon[i].UNITS;
                        }

                        balance = parseFloat(unit) + parseFloat(balance);
                        cnav = datacon[i].cnav;
                        bank = datacon[i].BANK_NAME;
                        acno = datacon[i].AC_NO;
                        joint1 = datacon[i].JTNAME1;
                        joint2 = datacon[i].JTNAME2;
                        nominee = datacon[i].NOMINEE;
                        invname = datacon[i].INVNAME;
                        folio = datacon[i].FOLIO;
                        scheme = datacon[i].SCHEME;
                        nature = datacon[i].NATURE;
                        navdate = datacon[i].navdate;
                        isin = datacon[i].SCHEMEISIN;
                    }
                    if(Array.isArray(bank) === true ){
                    bank = bank.filter(function(e){return e});
                    if(bank.length === 0){
                        bank  = "";
                     }else{
                        bank  = bank[0];
                     }
                    }
                    if(Array.isArray(acno) === true ){
                    acno = acno.filter(function(e){return e});
                    if(acno.length === 0){
                        acno = "";
                     } else{
                        acno = acno[0];
                     }
                    }
                    if(Array.isArray(joint1) === true ){
                    joint1 = joint1.filter(function(e){return e});
                    if(joint1.length === 0  ){
                        joint1 = "" ;
                     }else{
                        joint1 = joint1[0] ;
                     }
                    }
                    if(Array.isArray(joint2) === true ){
                    joint2 = joint2.filter(function(e){return e});
                    if(joint2.length === 0){
                        joint2 = "";
                    }else{
                        joint2 = joint2[0];
                    }
                    }
                    if(Array.isArray(nominee) === true ){
                    nominee = nominee.filter(function(e){return e});
                    if(nominee.length === 0){
                        nominee ="";
                    }else{
                        nominee = nominee[0];
                    }
                    }
                    
                   if (balance.isNaN || cnav === "" || balance < 0 ) {
                       balance = 0;
                        cnav =0;
                    }
                        dataarr.push({UNITS:parseFloat(balance.toFixed(3)),AMOUNT:Math.round(parseFloat(cnav)*parseFloat(balance)),BANK_NAME:bank,AC_NO:acno,JTNAME1:joint1,JTNAME2:joint2,NOMINEE:nominee,INVNAME:invname,FOLIO:folio,SCHEME:scheme,NATURE:nature,navdate:navdate,SCHEMEISIN:isin,cnav:cnav})   
                    resdata.data = dataarr
                } else {
                    resdata = {
                        status: 400,
                        message: "Data not found"
                    };
                }
                res.json(resdata);
                return resdata;
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


// app.post("/api/gettaxsavinguserwise", function (req, res) {
//     try{
//     var yer = req.body.fromyear;
//     var secyer = req.body.toyear;
//     yer = yer + "-04-01";
//     secyer = secyer + "-03-31"
//     var pan = req.body.pan;

//     var transk = mongoose.model('trans_karvy', transkarvy, 'trans_karvy');
//     var transc = mongoose.model('trans_cams', transcams, 'trans_cams');
//     var transf = mongoose.model('trans_franklin', transfranklin, 'trans_franklin');
//      if(req.body.pan != "" && req.body.name != ""){
//         const pipeline = [  ///trans_cams
//         { $match: { $and: [{ SCHEME: /Tax/ }, { PAN: pan },{ INV_NAME: {$regex : `^${req.body.name}.*` , $options: 'i' } }, { TRXN_NATUR: { $not: /^Redemption.*/ } },{ TRXN_NATUR: { $not: /^Gross Dividend.*/ } },  { TRXN_NATUR: { $not: /^Dividend Paid.*/ } }, { TRXN_NATUR: { $not: /^Switchout.*/ } }, { TRXN_NATUR: { $not: /^Transfer-Out.*/ } }, { TRXN_NATUR: { $not: /^Lateral Shift Out.*/ } },{ TRADDATE: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }] } },
//         { $group: { _id: { INV_NAME: "$INV_NAME", PAN: "$PAN", SCHEME: "$SCHEME", TRXN_NATUR: "$TRXN_NATUR", FOLIO_NO: "$FOLIO_NO", AMOUNT: "$AMOUNT", TRADDATE: "$TRADDATE" } } },
//         { $project: { _id: 0, INVNAME: "$_id.INV_NAME", PAN: "$_id.PAN", SCHEME: "$_id.SCHEME", TRXN_NATUR: "$_id.TRXN_NATUR", FOLIO_NO: "$_id.FOLIO_NO", AMOUNT: "$_id.AMOUNT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRADDATE" } }} },
//         { $sort: { TRADDATE: -1 } }
//     ]
//     const pipeline1 = [  ///trans_karvy                                             
//         { $match: { $and: [{ FUNDDESC: /Tax/ }, { PAN1: pan },{ INVNAME: {$regex : `^${req.body.name}.*` , $options: 'i' } } ,{ TRDESC: { $not: /^Redemption.*/ } },{ TRDESC: { $not: /^Rejection.*/ } },{ TRDESC: { $not: /^Gross Dividend.*/ } },{ TRDESC: { $not: /^Switch Over Out.*/ } },  { TRDESC: { $not: /^Dividend Paid.*/ } }, { TRXN_NATURE: { $not: /^Switchout.*/ } }, { TRXN_NATURE: { $not: /^Transfer-Out.*/ } }, { TRXN_NATURE: { $not: /^Lateral Shift Out.*/ } },{ TD_TRDT: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } } ] } },
//         { $group: { _id: { INVNAME: "$INVNAME", PAN1: "$PAN1", FUNDDESC: "$FUNDDESC", TRDESC: "$TRDESC", TD_ACNO: "$TD_ACNO", TD_AMT: "$TD_AMT", TD_TRDT: "$TD_TRDT" } } },
//         { $project: { _id: 0, INVNAME: "$_id.INVNAME", PAN: "$_id.PAN1", SCHEME: "$_id.FUNDDESC", TRXN_NATUR: "$_id.TRDESC", FOLIO_NO: "$_id.TD_ACNO", AMOUNT: "$_id.TD_AMT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TD_TRDT" } } } },
//         { $sort: { TRADDATE: -1 } }
//     ]
//     const pipeline2 = [  ///trans_franklin
//         { $match: { $and: [{ SCHEME_NA1: /Tax/ }, { IT_PAN_NO1: pan }, { INVESTOR_2: {$regex : `^${req.body.name}.*` , $options: 'i' } },{ TRXN_TYPE: { $not: /^SIPR.*/ } },{ TRXN_TYPE: { $not: /^TO.*/ } },{ TRXN_TYPE: { $not: /^DP.*/ } },{ TRXN_TYPE: { $not: /^RED.*/ } },{ TRXN_TYPE: { $not: /^REDR.*/ } },{ TRXN_TYPE: { $not: /^Gross Dividend.*/ } }, { TRXN_TYPE: { $not: /^Dividend Paid.*/ } }, { TRXN_TYPE: { $not: /^SWOF.*/ } }, { TRXN_TYPE: { $not: /^Transfer-Out.*/ } }, { TRXN_TYPE: { $not: /^Lateral Shift Out.*/ } } , { TRXN_DATE: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }  ] } },
//         { $group: { _id: { INVESTOR_2: "$INVESTOR_2", IT_PAN_NO1: "$IT_PAN_NO1", SCHEME_NA1: "$SCHEME_NA1", TRXN_TYPE: "$TRXN_TYPE", FOLIO_NO: "$FOLIO_NO", AMOUNT: "$AMOUNT", TRXN_DATE: "$TRXN_DATE"  } } },
//         { $project: { _id: 0, INVNAME: "$INVESTOR_2", PAN: "$_id.IT_PAN_NO1", SCHEME: "$_id.SCHEME_NA1", TRXN_NATUR: "$_id.TRXN_TYPE", FOLIO_NO: "$_id.FOLIO_NO", AMOUNT: "$_id.AMOUNT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRXN_DATE" } } } },
//         { $sort: { TRADDATE: -1 } }
//     ]
//         transc.aggregate(pipeline, (err, camsdata) => {
//             transk.aggregate(pipeline1, (err, karvydata) => {
//                 transf.aggregate(pipeline2, (err, frankdata) => {
//                     if (frankdata.length != 0 || karvydata.length != 0 || camsdata.length != 0) {
//                         resdata = {
//                             status: 200,
//                             message: 'Successfull',
//                             data: frankdata
//                         }
//                     } else {
//                         resdata = {
//                             status: 400,
//                             message: 'Data not found',
//                         }
//                     }
//                     var datacon = frankdata.concat(karvydata.concat(camsdata))
//                     datacon = datacon.map(JSON.stringify).reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
//                         .filter(function (item, index, arr) { return arr.indexOf(item, index + 1) === -1; }) // check if there is any occurence of the item in whole array
//                         .reverse().map(JSON.parse);
//                      for (var i = 0; i < datacon.length; i++) {
//                             if (datacon[i]['TRXN_NATUR'].match(/Systematic Investment.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic Withdrawal.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic - Instalment.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic - To.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic-NSE.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic Physical.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic-Normal.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic (ECS).*/)) {
//                                 datacon[i]['TRXN_NATUR'] = "SIP";
//                             } if (Math.sign(datacon[i]['AMOUNT']) === -1) {
//                                 datacon[i]['TRXN_NATUR'] = "SIPR";
//                             } if (datacon[i]['TRXN_NATUR'].match(/Systematic - From.*/)) {
//                                 datacon[i]['TRXN_NATUR'] = "STP";
//                             }
//                         }
//                     resdata.data = datacon.sort((a, b) => new Date(b.TRADDATE.split("-").reverse().join("/")).getTime() - new Date(a.TRADDATE.split("-").reverse().join("/")).getTime())
//                     res.json(resdata)
//                     return resdata
//                 });
//             });
//         });
//     }else{
//         resdata = {
//             status: 400,
//             message: 'Data not found',
//         }
//     }
// } catch (err) {
//     console.log(err)
// }
// });

app.post("/api/gettaxsavinguserwise", function (req, res) {
    try{
        var member="";var guardpan1=[];var guardpan2=[];
        var arr1=[];var arr2=[];var arr3=[];var alldata=[];var arrFolio=[];var arrName=[];
        let regex = /^([a-zA-Z]){5}([0-9]){4}([a-zA-Z]){1}?$/;
        if(req.body.fromyear ===""){
            resdata = {
                status: 400,
                message: 'Please enter from year',
            }
            res.json(resdata);
            return resdata;
        }else if(req.body.toyear ===""){
            resdata = {
                status: 400,
                message: 'Please enter to year',
            }
            res.json(resdata);
            return resdata;
        }else if(req.body.pan ===""){
            resdata = {
                status: 400,
                message: 'Please enter pan',
            }
            res.json(resdata);
            return resdata;
        }else if(!regex.test(req.body.pan)) {
            resdata = {
                status: 400,
                message: 'Please enter valid pan',
            }
            res.json(resdata);
            return resdata;
        }else{
            var yer = req.body.fromyear;
            var secyer = req.body.toyear;
            yer = yer + "-04-01";
            secyer = secyer + "-03-31"
            family.find({ adminPan:  {$regex : `^${req.body.pan}.*` , $options: 'i' }  },{_id:0,memberPan:1}, function (err, member) {
                if(member!=""){
                    member  = [...new Set(member.map(({memberPan}) => memberPan.toUpperCase()))];
                    guardpan1.push({GUARD_PAN:req.body.pan.toUpperCase()});
                    guardpan2.push({GUARDPANNO:req.body.pan.toUpperCase()});
                    arr1.push({PAN:req.body.pan.toUpperCase()});
                    arr2.push({PAN1:req.body.pan.toUpperCase()});
                    arr3.push({IT_PAN_NO1:req.body.pan.toUpperCase()});
                    for(var j=0;j<member.length;j++){     
                    guardpan1.push({GUARD_PAN:member[j]}); 
                    guardpan2.push({GUARDPANNO:member[j]});
                    arr1.push({PAN:member[j]});
                    arr2.push({PAN1:req.body.pan.toUpperCase()});
                    arr3.push({IT_PAN_NO1:req.body.pan.toUpperCase()});
                    }
                    var strPan1 = {$or:guardpan1};
                    var strPan2 = {$or:guardpan2};
                    folioc.find(strPan1).distinct("FOLIOCHK", function (err, member1) {
                      foliok.find(strPan2).distinct("ACNO", function (err, member2) {
                      var alldata = member1.concat(member2);   
                            for(var j=0;j<alldata.length;j++){     
                                arr1.push({FOLIO_NO:alldata[j]});
                                arr2.push({TD_ACNO:alldata[j]});
                                arr3.push({FOLIO_NO:alldata[j]});
                                }
                             var strFolio = {$or:arr1};
                             var strFolio1 = {$or:arr2};
                             var strFolio2 = {$or:arr3};
       pipeline = [  ///trans_cams
            { $match: { $and:  [strFolio, {SCHEME: /Tax/ }, { TRXN_NATUR: { $not: /^Lateral Shift Out.*/ } } , { TRXN_NATUR: { $not: /^Redemption.*/ } } , { TRXN_NATUR: { $not: /^Gross Dividend.*/ } },  { TRXN_NATUR: { $not: /^Dividend Paid.*/ } }, { TRXN_NATUR: { $not: /^Switchout.*/ } }, { TRXN_NATUR: { $not: /^Transfer-Out.*/ } } , { AMOUNT: { $gte: 0 } },{ TRADDATE: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } } ] } },
            { $group: { _id: { TAX_STATUS:"$TAX_STATUS",TRXNNO:"$TRXNNO",INV_NAME: "$INV_NAME", PAN: "$PAN", SCHEME: "$SCHEME", TRXN_NATUR: "$TRXN_NATUR", FOLIO_NO: "$FOLIO_NO", AMOUNT: "$AMOUNT", TRADDATE: "$TRADDATE" } } },
            { $project: { _id: 0,PER_STATUS:"$_id.TAX_STATUS",TRXNNO:"$_id.TRXNNO", INVNAME: "$_id.INV_NAME", PAN: "$_id.PAN", SCHEME: "$_id.SCHEME", TRXN_NATUR: "$_id.TRXN_NATUR", FOLIO_NO: "$_id.FOLIO_NO", AMOUNT: "$_id.AMOUNT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRADDATE" } }} },
            { $lookup: { from: 'folio_cams', localField: 'FOLIO_NO', foreignField: 'FOLIOCHK', as: 'detail' } },
            { $replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$detail", 0 ] }, "$$ROOT" ] } } } ,
            { $project: {    detail: 0 ,_id:0,TAX_STATUS:0,FOLIOCHK:0,AC_NO:0,FOLIO_DATE:0,PRODUCT:0,SCH_NAME:0,AMC_CODE:0,BANK_NAME:0,HOLDING_NA:0,IFSC_CODE:0,JNT_NAME1:0,JNT_NAME2:0,JOINT1_PAN:0,NOM2_NAME:0,NOM3_NAME:0,NOM_NAME:0,PRCODE:0,HOLDING_NATURE:0,PAN_NO:0,INV_NAME:0,EMAIL:0} },
            { $sort: { TRADDATE: -1 } }
    ]
       pipeline1 = [  ///trans_karvy                                             
             { $match: { $and: [strFolio1, { $or: [ { FUNDDESC: /TAX/ }, { FUNDDESC: /Tax Saver/ }, { FUNDDESC: /Long Term Equity Fund/ }, { FUNDDESC: /IDBI Equity Advantage Fund/ }, { FUNDDESC: /Sundaram Diversified Equity Fund/ }] }, { TD_AMT: { $gte: 0 } }, { TRDESC: { $not: /^Consolidation Out.*/ } }, { TRDESC: { $not: /^Redemption.*/ } }, { TRDESC: { $not: /^Rejection.*/ } }, { TRDESC: { $not: /^Gross Dividend.*/ } }, { TRDESC: { $not: /^Switch Over Out.*/ } }, { TRDESC: { $not: /^Dividend Paid.*/ } }, { TRDESC: { $not: /^Switchout.*/ } }, { TRDESC: { $not: /^Transfer-Out.*/ } }, { TRDESC: { $not: /^Lateral Shift Out.*/ } }, { TD_TRDT: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }] } },
            { $group: { _id: { STATUS:"$STATUS",TD_TRNO:"$TD_TRNO",INVNAME: "$INVNAME", PAN1: "$PAN1", FUNDDESC: "$FUNDDESC", TRDESC: "$TRDESC", TD_ACNO: "$TD_ACNO", TD_AMT: "$TD_AMT", TD_TRDT: "$TD_TRDT" } } },
            { $project: { _id: 0,PER_STATUS:"$_id.STATUS", TRXNNO:"$_id.TD_TRNO", INVNAME: "$_id.INVNAME", PAN: "$_id.PAN1", SCHEME: "$_id.FUNDDESC", TRXN_NATUR: "$_id.TRDESC", FOLIO_NO: "$_id.TD_ACNO", AMOUNT: "$_id.TD_AMT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TD_TRDT" } } } },
            { $lookup: { from: 'folio_karvy', localField: 'FOLIO_NO', foreignField: 'ACNO', as: 'detail' } },
            { $replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$detail", 0 ] }, "$$ROOT" ] } } } ,
            { $project: { detail: 0 , _id:0,STATUS:0,PRCODE:0,STATUSDESC:0,ACNO:0,BNKACNO:0,BNKACTYPE:0,FUNDDESC:0,NOMINEE:0,MODEOFHOLD:0,JTNAME2:0,FUND:0,EMAIL:0,BNAME:0,PANGNO:0,JTNAME1:0,PAN2:0} },
            { $sort: { TRADDATE: -1 } }
    ]
       pipeline2 = [  ///trans_franklin
          { $match: { $and: [strFolio2, { SCHEME_NA1: /TAX/ }, { SCHEME_NA1: /Taxshield/ }, { AMOUNT: { $gte: 0 } }, { TRXN_TYPE: { $not: /^SIPR.*/ } }, { TRXN_TYPE: { $not: /^TO.*/ } }, { TRXN_TYPE: { $not: /^DP.*/ } }, { TRXN_TYPE: { $not: /^RED.*/ } }, { TRXN_TYPE: { $not: /^REDR.*/ } }, { TRXN_TYPE: { $not: /^Gross Dividend.*/ } }, { TRXN_TYPE: { $not: /^Dividend Paid.*/ } }, { TRXN_TYPE: { $not: /^SWOF.*/ } }, { TRXN_TYPE: { $not: /^Transfer-Out.*/ } }, { TRXN_TYPE: { $not: /^Lateral Shift Out.*/ } }, { TRXN_DATE: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } } ] } },
            { $group: { _id: {  SOCIAL_S18:"$SOCIAL_S18",TRXN_NO:"$TRXN_NO", INVESTOR_2: "$INVESTOR_2", IT_PAN_NO1: "$IT_PAN_NO1", SCHEME_NA1: "$SCHEME_NA1", TRXN_TYPE: "$TRXN_TYPE", FOLIO_NO: "$FOLIO_NO", AMOUNT: "$AMOUNT", TRXN_DATE: "$TRXN_DATE"  } } },
            { $project: { _id: 0,PER_STATUS:"$_id.SOCIAL_S18",TRXNNO:"$_id.TRXN_NO", INVNAME: "$_id.INVESTOR_2", PAN: "$_id.IT_PAN_NO1", SCHEME: "$_id.SCHEME_NA1", TRXN_NATUR: "$_id.TRXN_TYPE", FOLIO_NO: "$_id.FOLIO_NO", AMOUNT: "$_id.AMOUNT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRXN_DATE" } } } },
            { $lookup: { from: 'folio_franklin', localField: 'FOLIO_NO', foreignField: 'FOLIO_NO', as: 'detail' } },
            { $replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$detail", 0 ] }, "$$ROOT" ] } } } ,
            { $project: { detail: 0 ,_id:0,TAX_STATUS:0,FOLIO_NO:0,PERSONAL_9:0,ACCNT_NO:0,AC_TYPE:0,ADDRESS1:0,BANK_CODE:0,BANK_NAME:0,COMP_CODE:0,D_BIRTH:0,EMAIL:0,HOLDING_T6:0,F_NAME:0,IFSC_CODE:0,JOINT_NAM1:0,JOINT_NAM2:0,KYC_ID:0,NEFT_CODE:0,NOMINEE1:0,PBANK_NAME:0,PANNO2:0,PANNO1:0,PHONE_RES:0,SOCIAL_ST7:0} },
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
                   
                    var datacon = frankdata.concat(karvydata.concat(camsdata))
                    
                   datacon = datacon.map(JSON.stringify).reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
                        .filter(function (item, index, arr) { return arr.indexOf(item, index + 1) === -1; }) // check if there is any occurence of the item in whole array
                        .reverse().map(JSON.parse);
                        var newdata1 = datacon.map(item=>{
                            return [JSON.stringify(item),item]
                             }); // creates array of array
                        var maparr1 = new Map(newdata1); // create key value pair from array of array
                        datacon = [...maparr1.values()];//converting back to array from mapobject 
                     datacon = datacon.map(function(obj) {
                       if(obj['GUARDIANN0']){
                           obj['GUARD_NAME'] = obj['GUARDIANN0']; // Assign new key
                           obj['GUARD_PAN'] = obj['GUARDPANNO'];
                            // Delete old key
                                 delete obj['GUARDIANN0'];
                                 delete obj['GUARDPANNO'];
                       }else if((obj['GUARDIANN0']) === ""){
                               obj['GUARD_NAME'] = obj['GUARDIANN0']; // Assign new key
                               obj['GUARD_PAN'] = obj['GUARDPANNO'];
                               delete obj['GUARDIANN0'];
                               delete obj['GUARDPANNO'];
                           }
                       if(obj['GUARDIAN20'] === ""){
                           obj['GUARD_NAME'] = obj['GUARDIAN20']; // Assign new key
                            // Delete old key
                           delete obj['GUARDIAN20'];
                       }else if((obj['GUARDIAN20']) === ""){
                           obj['GUARD_NAME'] = obj['GUARDIAN20']; // Assign new key
                           delete obj['GUARDIAN20'];
                       }
                           return obj;
                       });

                        for (var i = 0; i < datacon.length; i++) {
                            if (datacon[i]['TRXN_NATUR'].match(/Systematic Investment.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic Withdrawal.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic - Instalment.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic - To.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic-NSE.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic Physical.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic-Normal.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic (ECS).*/)) {
                                datacon[i]['TRXN_NATUR'] = "SIP";
                            } if (Math.sign(datacon[i]['AMOUNT']) === -1) {
                                datacon[i]['TRXN_NATUR'] = "SIPR";
                            } if (datacon[i]['TRXN_NATUR'].match(/Systematic - From.*/)) {
                                datacon[i]['TRXN_NATUR'] = "STP";
                            } if (datacon[i]['TRXN_NATUR'] === "Additional Purchase" || datacon[i]['TRXN_NATUR'] === "Fresh Purchase") {
                                datacon[i]['TRXN_NATUR'] = "Purchase";
                            }if(datacon[i]['TRXN_NATUR'] === "Additional Purchase" || datacon[i]['TRXN_NATUR'] === "ADD" ||
                               datacon[i]['TRXN_NATUR'] === "ADDPUR") {
                               datacon[i]['TRXN_NATUR'] = "Add. Purchase";
                           }if(datacon[i]['TRXN_NATUR'] === "Purchase" || datacon[i]['TRXN_NATUR'] === "NEW" || datacon[i]['TRXN_NATUR'] === "NFO Purchase" || 
                               datacon[i]['TRXN_NATUR'] === "Initial Allotment" || datacon[i]['TRXN_NATUR'] === "NEWPUR" ) {
                               datacon[i]['TRXN_NATUR'] = "Purchase";
                           }if(datacon[i]['TRXN_NATUR'] === "Lateral Shift In" || datacon[i]['TRXN_NATUR'] === "Switch-In" 
                                || datacon[i]['TRXN_NATUR'] === "Transfer-In" || datacon[i]['TRXN_NATUR'] === "Switch Over In" 
                                || datacon[i]['TRXN_NATUR'] === "LTIN" || datacon[i]['TRXN_NATUR'] === "LTIA") {
                                datacon[i]['TRXN_NATUR'] = "Switch In";
                            }if(datacon[i]['PER_STATUS'] === "On Behalf Of Minor" || datacon[i]['PER_STATUS'] === "MINOR" || datacon[i]['PER_STATUS'] === "On Behalf of Minor" )  {
                                datacon[i]['PER_STATUS'] = "Minor"; 
				datacon[i]['PAN'] = ""; 
                            }if (datacon[i]['PER_STATUS'] === "INDIVIDUAL") {
                                 datacon[i]['PER_STATUS'] = "Individual";
                            }if (datacon[i]['PER_STATUS'] === "HINDU UNDIVIDED FAMI") {
                                datacon[i]['PER_STATUS'] = "HUF";
                           }
                        }
                        resdata.data = datacon.sort((a, b) => new Date(b.TRADDATE.split("-").reverse().join("/")).getTime() - new Date(a.TRADDATE.split("-").reverse().join("/")).getTime());
                        res.json(resdata);
                       return resdata;
                    } else{
                        resdata = {
                            status: 400,
                            message: 'Data not found',
                        }
                        res.json(resdata);
                       return resdata;
                       }
                       
                    });
                });
             });
           });
       });
           }else{
            pipeline = [  ///trans_cams
            { $match: { $and:  [ { PAN: req.body.pan }, { SCHEME: /Tax/ },{ AMOUNT: { $gte: 0 } },{ TRXN_NATUR: { $not: /^Lateral Shift Out.*/ } } ,{ TRXN_NATUR: { $not: /^Redemption.*/ } } , { TRXN_NATUR: { $not: /^Gross Dividend.*/ } },  { TRXN_NATUR: { $not: /^Dividend Paid.*/ } }, { TRXN_NATUR: { $not: /^Switchout.*/ } }, { TRXN_NATUR: { $not: /^Transfer-Out.*/ } },{ TRADDATE: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } } ] } },
            //{ $match: { $and:  [ { TRXN_NATUR: { $not: /^Lateral Shift Out.*/ } } ,{SCHEME: /Tax/ }, { TRXN_NATUR: { $not: /^Redemption.*/ } } , { TRXN_NATUR: { $not: /^Gross Dividend.*/ } },  { TRXN_NATUR: { $not: /^Dividend Paid.*/ } }, { TRXN_NATUR: { $not: /^Switchout.*/ } }, { TRXN_NATUR: { $not: /^Transfer-Out.*/ } },]},{ TRADDATE: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } } ] } },
            { $group: { _id: { TAX_STATUS:"$TAX_STATUS",TRXNNO:"$TRXNNO",INV_NAME: "$INV_NAME", PAN: "$PAN", SCHEME: "$SCHEME", TRXN_NATUR: "$TRXN_NATUR", FOLIO_NO: "$FOLIO_NO", AMOUNT: "$AMOUNT", TRADDATE: "$TRADDATE" } } },
            { $project: { _id: 0,PER_STATUS:"$_id.TAX_STATUS",TRXNNO:"$_id.TRXNNO", INVNAME: "$_id.INV_NAME", PAN: "$_id.PAN", SCHEME: "$_id.SCHEME", TRXN_NATUR: "$_id.TRXN_NATUR", FOLIO_NO: "$_id.FOLIO_NO", AMOUNT: "$_id.AMOUNT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRADDATE" } }} },
            { $lookup: { from: 'folio_cams', localField: 'FOLIO_NO', foreignField: 'FOLIOCHK', as: 'detail' } },
            { $replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$detail", 0 ] }, "$$ROOT" ] } } } ,
            { $project: {    detail: 0 ,_id:0,TAX_STATUS:0,FOLIOCHK:0,AC_NO:0,FOLIO_DATE:0,PRODUCT:0,SCH_NAME:0,AMC_CODE:0,BANK_NAME:0,HOLDING_NA:0,IFSC_CODE:0,JNT_NAME1:0,JNT_NAME2:0,JOINT1_PAN:0,NOM2_NAME:0,NOM3_NAME:0,NOM_NAME:0,PRCODE:0,HOLDING_NATURE:0,PAN_NO:0,INV_NAME:0,EMAIL:0} },
            { $sort: { TRADDATE: -1 } }
    ]
       pipeline1 = [  ///trans_karvy                                             
             { $match: { $and: [{ PAN1: req.body.pan }, { $or: [{ FUNDDESC: /TAX/ }, { FUNDDESC: /Tax/ }, { FUNDDESC: /Tax Saver/ }, { FUNDDESC: /Long Term Equity Fund/ }, { FUNDDESC: /IDBI Equity Advantage Fund/ }, { FUNDDESC: /Sundaram Diversified Equity Fund/ }] }, { TD_AMT: { $gte: 0 } }, { TRDESC: { $not: /^Consolidation Out.*/ } }, { TRDESC: { $not: /^Redemption.*/ } }, { TRDESC: { $not: /^Rejection.*/ } }, { TRDESC: { $not: /^Gross Dividend.*/ } }, { TRDESC: { $not: /^Switch Over Out.*/ } }, { TRDESC: { $not: /^Dividend Paid.*/ } }, { TRDESC: { $not: /^Switchout.*/ } }, { TRDESC: { $not: /^Transfer-Out.*/ } }, { TRDESC: { $not: /^Lateral Shift Out.*/ } }, { TD_TRDT: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }] } },
            { $group: { _id: { STATUS:"$STATUS",TD_TRNO:"$TD_TRNO",INVNAME: "$INVNAME", PAN1: "$PAN1", FUNDDESC: "$FUNDDESC", TRDESC: "$TRDESC", TD_ACNO: "$TD_ACNO", TD_AMT: "$TD_AMT", TD_TRDT: "$TD_TRDT" } } },
            { $project: { _id: 0,PER_STATUS:"$_id.STATUS", TRXNNO:"$_id.TD_TRNO", INVNAME: "$_id.INVNAME", PAN: "$_id.PAN1", SCHEME: "$_id.FUNDDESC", TRXN_NATUR: "$_id.TRDESC", FOLIO_NO: "$_id.TD_ACNO", AMOUNT: "$_id.TD_AMT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TD_TRDT" } } } },
            { $lookup: { from: 'folio_karvy', localField: 'FOLIO_NO', foreignField: 'ACNO', as: 'detail' } },
            { $replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$detail", 0 ] }, "$$ROOT" ] } } } ,
            { $project: { detail: 0 , _id:0,STATUS:0,PRCODE:0,STATUSDESC:0,ACNO:0,BNKACNO:0,BNKACTYPE:0,FUNDDESC:0,NOMINEE:0,MODEOFHOLD:0,JTNAME2:0,FUND:0,EMAIL:0,BNAME:0,PANGNO:0,JTNAME1:0,PAN2:0} },
            { $sort: { TRADDATE: -1 } }
    ]
       pipeline2 = [  ///trans_franklin
           { $match: { $and: [{ IT_PAN_NO1: req.body.pan }, { $or: [{ SCHEME_NA1: /TAX/ }, { SCHEME_NA1: /Taxshield/ }] },  { AMOUNT: { $gte: 0 } }, { TRXN_TYPE: { $not: /^SIPR.*/ } }, { TRXN_TYPE: { $not: /^TO.*/ } }, { TRXN_TYPE: { $not: /^DP.*/ } }, { TRXN_TYPE: { $not: /^RED.*/ } }, { TRXN_TYPE: { $not: /^REDR.*/ } }, { TRXN_TYPE: { $not: /^Gross Dividend.*/ } }, { TRXN_TYPE: { $not: /^Dividend Paid.*/ } }, { TRXN_TYPE: { $not: /^SWOF.*/ } }, { TRXN_TYPE: { $not: /^Transfer-Out.*/ } }, { TRXN_TYPE: { $not: /^Lateral Shift Out.*/ } }, { TRXN_DATE: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } } ] } },
            { $group: { _id: {  SOCIAL_S18:"$SOCIAL_S18",TRXN_NO:"$TRXN_NO", INVESTOR_2: "$INVESTOR_2", IT_PAN_NO1: "$IT_PAN_NO1", SCHEME_NA1: "$SCHEME_NA1", TRXN_TYPE: "$TRXN_TYPE", FOLIO_NO: "$FOLIO_NO", AMOUNT: "$AMOUNT", TRXN_DATE: "$TRXN_DATE"  } } },
            { $project: { _id: 0,PER_STATUS:"$_id.SOCIAL_S18",TRXNNO:"$_id.TRXN_NO", INVNAME: "$_id.INVESTOR_2", PAN: "$_id.IT_PAN_NO1", SCHEME: "$_id.SCHEME_NA1", TRXN_NATUR: "$_id.TRXN_TYPE", FOLIO_NO: "$_id.FOLIO_NO", AMOUNT: "$_id.AMOUNT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRXN_DATE" } } } },
            { $lookup: { from: 'folio_franklin', localField: 'FOLIO_NO', foreignField: 'FOLIO_NO', as: 'detail' } },
            { $replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$detail", 0 ] }, "$$ROOT" ] } } } ,
            { $project: { detail: 0 ,_id:0,TAX_STATUS:0,FOLIO_NO:0,PERSONAL_9:0,ACCNT_NO:0,AC_TYPE:0,ADDRESS1:0,BANK_CODE:0,BANK_NAME:0,COMP_CODE:0,D_BIRTH:0,EMAIL:0,HOLDING_T6:0,F_NAME:0,IFSC_CODE:0,JOINT_NAM1:0,JOINT_NAM2:0,KYC_ID:0,NEFT_CODE:0,NOMINEE1:0,PBANK_NAME:0,PANNO2:0,PANNO1:0,PHONE_RES:0,SOCIAL_ST7:0} },
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
              
               var datacon = frankdata.concat(karvydata.concat(camsdata));
               
              datacon = datacon.map(JSON.stringify).reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
                   .filter(function (item, index, arr) { return arr.indexOf(item, index + 1) === -1; }) // check if there is any occurence of the item in whole array
                   .reverse().map(JSON.parse);
                   var newdata1 = datacon.map(item=>{
                       return [JSON.stringify(item),item]
                        }); // creates array of array
                   var maparr1 = new Map(newdata1); // create key value pair from array of array
                   datacon = [...maparr1.values()];//converting back to array from mapobject 
                datacon = datacon.map(function(obj) {
                  if(obj['GUARDIANN0']){
                      obj['GUARD_NAME'] = obj['GUARDIANN0']; // Assign new key
                      obj['GUARD_PAN'] = obj['GUARDPANNO'];
                       // Delete old key
                            delete obj['GUARDIANN0'];
                            delete obj['GUARDPANNO'];
                  }else if((obj['GUARDIANN0']) === ""){
                          obj['GUARD_NAME'] = obj['GUARDIANN0']; // Assign new key
                          obj['GUARD_PAN'] = obj['GUARDPANNO'];
                          delete obj['GUARDIANN0'];
                          delete obj['GUARDPANNO'];
                      }
                  if(obj['GUARDIAN20']){
                      obj['GUARD_NAME'] = obj['GUARDIAN20']; // Assign new key
                       // Delete old key
                      delete obj['GUARDIAN20'];
                  }else if((obj['GUARDIAN20']) === ""){
                      obj['GUARD_NAME'] = obj['GUARDIAN20']; // Assign new key
                      delete obj['GUARDIAN20'];
                  }
                      return obj;
                  });

                   for (var i = 0; i < datacon.length; i++) {
                       if (datacon[i]['TRXN_NATUR'].match(/Systematic Investment.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic Withdrawal.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic - Instalment.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic - To.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic-NSE.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic Physical.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic-Normal.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic (ECS).*/)) {
                           datacon[i]['TRXN_NATUR'] = "SIP";
                       } if (Math.sign(datacon[i]['AMOUNT']) === -1) {
                           datacon[i]['TRXN_NATUR'] = "SIPR";
                       } if (datacon[i]['TRXN_NATUR'].match(/Systematic - From.*/)) {
                           datacon[i]['TRXN_NATUR'] = "STP";
                       } if (datacon[i]['TRXN_NATUR'] === "Additional Purchase" || datacon[i]['TRXN_NATUR'] === "Fresh Purchase") {
                           datacon[i]['TRXN_NATUR'] = "Purchase";
                       }if(datacon[i]['TRXN_NATUR'] === "Additional Purchase" || datacon[i]['TRXN_NATUR'] === "ADD" ||
                          datacon[i]['TRXN_NATUR'] === "ADDPUR") {
                          datacon[i]['TRXN_NATUR'] = "Add. Purchase";
                      }if(datacon[i]['TRXN_NATUR'] === "Purchase" || datacon[i]['TRXN_NATUR'] === "NEW" || datacon[i]['TRXN_NATUR'] === "NFO Purchase" || 
                          datacon[i]['TRXN_NATUR'] === "Initial Allotment" || datacon[i]['TRXN_NATUR'] === "NEWPUR" ) {
                          datacon[i]['TRXN_NATUR'] = "Purchase";
                      }if(datacon[i]['TRXN_NATUR'] === "Lateral Shift In" || datacon[i]['TRXN_NATUR'] === "Switch-In" 
                           || datacon[i]['TRXN_NATUR'] === "Transfer-In" || datacon[i]['TRXN_NATUR'] === "Switch Over In" 
                           || datacon[i]['TRXN_NATUR'] === "LTIN" || datacon[i]['TRXN_NATUR'] === "LTIA") {
                           datacon[i]['TRXN_NATUR'] = "Switch In";
                       }if(datacon[i]['PER_STATUS'] === "On Behalf Of Minor" || datacon[i]['PER_STATUS'] === "MINOR" || datacon[i]['PER_STATUS'] === "On Behalf of Minor" )  {
                           datacon[i]['PER_STATUS'] = "Minor";   
			   datacon[i]['PAN'] = "";   
                       }if (datacon[i]['PER_STATUS'] === "INDIVIDUAL") {
                            datacon[i]['PER_STATUS'] = "Individual";
                       }if (datacon[i]['PER_STATUS'] === "HINDU UNDIVIDED FAMI") {
                           datacon[i]['PER_STATUS'] = "HUF";
                      }
                   }
                   resdata.data = datacon.sort((a, b) => new Date(b.TRADDATE.split("-").reverse().join("/")).getTime() - new Date(a.TRADDATE.split("-").reverse().join("/")).getTime());
                   res.json(resdata);
                  return resdata;
                } else{
                    resdata = {
                        status: 400,
                        message: 'Data not found',
                    }
                    res.json(resdata);
                   return resdata;
                   }
                  
               });
           });
        });
              }       
           });
       }
   } catch (err) {
       console.log(err)
   }
   })

// app.post("/api/gettaxsavinguserwise", function (req, res) {
//     try{
//     var yer = req.body.fromyear;
//     var secyer = req.body.toyear;
//     yer = yer + "-04-01";
//     secyer = secyer + "-03-31"
//     var pan = req.body.pan;
//    if(req.body.pan != "" && req.body.name != ""){
//     pipeline = [  ///trans_cams
//         { $match: { $and: [{ SCHEME: /Tax/ }, { PAN: pan },{ INV_NAME: {$regex : `^${req.body.name}.*` , $options: 'i' } }, { AMOUNT: { $gte: 0 } },{ TRXN_NATUR: { $not: /^Redemption.*/ } } , { TRXN_NATUR: { $not: /^Gross Dividend.*/ } },  { TRXN_NATUR: { $not: /^Dividend Paid.*/ } }, { TRXN_NATUR: { $not: /^Switchout.*/ } }, { TRXN_NATUR: { $not: /^Transfer-Out.*/ } }, { TRXN_NATUR: { $not: /^Lateral Shift Out.*/ } },{ TRADDATE: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }] } },
//         { $group: { _id: { INV_NAME: "$INV_NAME", PAN: "$PAN", SCHEME: "$SCHEME", TRXN_NATUR: "$TRXN_NATUR", FOLIO_NO: "$FOLIO_NO", AMOUNT: "$AMOUNT", TRADDATE: "$TRADDATE" } } },
//         { $project: { _id: 0, INVNAME: "$_id.INV_NAME", PAN: "$_id.PAN", SCHEME: "$_id.SCHEME", TRXN_NATUR: "$_id.TRXN_NATUR", FOLIO_NO: "$_id.FOLIO_NO", AMOUNT: "$_id.AMOUNT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRADDATE" } }} },
//         { $sort: { TRADDATE: -1 } }
//     ]
//        pipeline1 = [  ///trans_karvy                                             
//         { $match: { $and: [{ FUNDDESC: /TAX/ }, { PAN1: pan },{ INVNAME: {$regex : `^${req.body.name}.*` , $options: 'i' } } , { TD_AMT: { $gte: 0 } } ,{ TRDESC: { $not: /^Consolidation Out.*/ } },{ TRDESC: { $not: /^Redemption.*/ } },{ TRDESC: { $not: /^Rejection.*/ } },{ TRDESC: { $not: /^Gross Dividend.*/ } },{ TRDESC: { $not: /^Switch Over Out.*/ } },  { TRDESC: { $not: /^Dividend Paid.*/ } }, { TRDESC: { $not: /^Switchout.*/ } }, { TRDESC: { $not: /^Transfer-Out.*/ } }, { TRDESC: { $not: /^Lateral Shift Out.*/ } },{ TD_TRDT: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } } ] } },
//         { $group: { _id: { INVNAME: "$INVNAME", PAN1: "$PAN1", FUNDDESC: "$FUNDDESC", TRDESC: "$TRDESC", TD_ACNO: "$TD_ACNO", TD_AMT: "$TD_AMT", TD_TRDT: "$TD_TRDT" } } },
//         { $project: { _id: 0, INVNAME: "$_id.INVNAME", PAN: "$_id.PAN1", SCHEME: "$_id.FUNDDESC", TRXN_NATUR: "$_id.TRDESC", FOLIO_NO: "$_id.TD_ACNO", AMOUNT: "$_id.TD_AMT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TD_TRDT" } } } },
//         { $sort: { TRADDATE: -1 } }
//     ]
//        pipeline2 = [  ///trans_franklin
//         { $match: { $and: [{ SCHEME_NA1: /TAX/ }, { IT_PAN_NO1: pan }, { INVESTOR_2: {$regex : `^${req.body.name}.*` , $options: 'i' } }, { AMOUNT: { $gte: 0 } } , { TRXN_TYPE: { $not: /^SIPR.*/ } },{ TRXN_TYPE: { $not: /^TO.*/ } },{ TRXN_TYPE: { $not: /^DP.*/ } },{ TRXN_TYPE: { $not: /^RED.*/ } },{ TRXN_TYPE: { $not: /^REDR.*/ } },{ TRXN_TYPE: { $not: /^Gross Dividend.*/ } }, { TRXN_TYPE: { $not: /^Dividend Paid.*/ } }, { TRXN_TYPE: { $not: /^SWOF.*/ } }, { TRXN_TYPE: { $not: /^Transfer-Out.*/ } }, { TRXN_TYPE: { $not: /^Lateral Shift Out.*/ } } , { TRXN_DATE: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }  ] } },
//         { $group: { _id: { INVESTOR_2: "$INVESTOR_2", IT_PAN_NO1: "$IT_PAN_NO1", SCHEME_NA1: "$SCHEME_NA1", TRXN_TYPE: "$TRXN_TYPE", FOLIO_NO: "$FOLIO_NO", AMOUNT: "$AMOUNT", TRXN_DATE: "$TRXN_DATE"  } } },
//         { $project: { _id: 0, INVNAME: "$_id.INVESTOR_2", PAN: "$_id.IT_PAN_NO1", SCHEME: "$_id.SCHEME_NA1", TRXN_NATUR: "$_id.TRXN_TYPE", FOLIO_NO: "$_id.FOLIO_NO", AMOUNT: "$_id.AMOUNT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRXN_DATE" } } } },
//         { $sort: { TRADDATE: -1 } }
//     ]
//   }
//         transc.aggregate(pipeline, (err, camsdata) => {
//              transk.aggregate(pipeline1, (err, karvydata) => {
//                 transf.aggregate(pipeline2, (err, frankdata) => {
//                     if (frankdata.length != 0 || karvydata.length != 0 || camsdata.length != 0) {
//                       resdata = {
//                             status: 200,
//                             message: 'Successfull',
//                             data: frankdata
//                         }
//                     } else {
//                         resdata = {
//                             status: 400,
//                             message: 'Data not found',
//                         }
//                     }
//                     var datacon = frankdata.concat(karvydata.concat(camsdata))
//                    datacon = datacon.map(JSON.stringify).reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
//                         .filter(function (item, index, arr) { return arr.indexOf(item, index + 1) === -1; }) // check if there is any occurence of the item in whole array
//                         .reverse().map(JSON.parse);
//                         for (var i = 0; i < datacon.length; i++) {
//                             if (datacon[i]['TRXN_NATUR'].match(/Systematic Investment.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic Withdrawal.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic - Instalment.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic - To.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic-NSE.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic Physical.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic-Normal.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic (ECS).*/)) {
//                                 datacon[i]['TRXN_NATUR'] = "SIP";
//                             } if (Math.sign(datacon[i]['AMOUNT']) === -1) {
//                                 datacon[i]['TRXN_NATUR'] = "SIPR";
//                             } if (datacon[i]['TRXN_NATUR'].match(/Systematic - From.*/)) {
//                                 datacon[i]['TRXN_NATUR'] = "STP";
//                             } if (datacon[i]['TRXN_NATUR'] === "Additional Purchase" || datacon[i]['TRXN_NATUR'] === "Fresh Purchase") {
//                                 datacon[i]['TRXN_NATUR'] = "Purchase";
//                             }if(datacon[i]['TRXN_NATUR'] === "Additional Purchase" || datacon[i]['TRXN_NATUR'] === "ADD" ||
//                             datacon[i]['TRXN_NATUR'] === "ADDPUR") {
//                                datacon[i]['TRXN_NATUR'] = "Add. Purchase";
//                            }if (datacon[i]['TRXN_NATUR'] === "Purchase" || datacon[i]['TRXN_NATUR'] === "NEW" || datacon[i]['TRXN_NATUR'] === "NFO Purchase" || 
//                            datacon[i]['TRXN_NATUR'] === "Initial Allotment" || datacon[i]['TRXN_NATUR'] === "NEWPUR" ) {
//                                datacon[i]['TRXN_NATUR'] = "Purchase";
//                            }if (datacon[i]['TRXN_NATUR'] === "Lateral Shift In" || datacon[i]['TRXN_NATUR'] === "Switch-In" 
//                            || datacon[i]['TRXN_NATUR'] === "Transfer-In" || datacon[i]['TRXN_NATUR'] === "Switch Over In" 
//                            || datacon[i]['TRXN_NATUR'] === "LTIN" || datacon[i]['TRXN_NATUR'] === "LTIA") {
//                                datacon[i]['TRXN_NATUR'] = "Switch In";
//                            }
//                         }
//                     resdata.data = datacon.sort((a, b) => new Date(b.TRADDATE.split("-").reverse().join("/")).getTime() - new Date(a.TRADDATE.split("-").reverse().join("/")).getTime())
//                     res.json(resdata)
//                     return resdata
//                 });
//              });
//          });
// } catch (err) {
//     console.log(err)
// }
// });

// app.post("/api/getsipstpuserwise", function (req, res) {
//     try{
//     var mon = parseInt(req.body.month);
//     var yer = parseInt(req.body.year);

//     var pan = req.body.pan;
//     if(req.body.pan != "" && req.body.name != ""){
//         const pipeline = [  ///trans_cams
//             { $group: { _id: { INV_NAME: "$INV_NAME", PAN: "$PAN", TRXN_NATUR: "$TRXN_NATUR", FOLIO_NO: "$FOLIO_NO", SCHEME: "$SCHEME", AMOUNT: "$AMOUNT",TAX_STATUS:"$TAX_STATUS" ,TRADDATE: "$TRADDATE" } } },
//             { $project: { _id: 0, INVNAME: "$_id.INV_NAME", PAN: "$_id.PAN", TRXN_NATUR: "$_id.TRXN_NATUR",  FOLIO_NO: "$_id.FOLIO_NO", SCHEME: "$_id.SCHEME", AMOUNT: "$_id.AMOUNT" ,TAX_STATUS:"$_id.TAX_STATUS", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRADDATE" } }, month: { $month: ('$_id.TRADDATE') }, year: { $year: ('$_id.TRADDATE') } } },
//             { $match: { $and: [{ month: mon }, { year: yer }, { PAN: pan }, { INVNAME: {$regex : `^${req.body.name}.*` , $options: 'si' } } , { TRXN_NATUR: /Systematic/ } ] } },
//             { $sort: { TRADDATE: -1 } }
//         ]
//         const pipeline1 = [  ///trans_karvy
//             { $group: { _id: { INVNAME: "$INVNAME", PAN1: "$PAN1", TRDESC: "$TRDESC",  TD_ACNO: "$TD_ACNO", FUNDDESC: "$FUNDDESC", TD_AMT: "$TD_AMT",STATUS:"$STATUS", TD_TRDT: "$TD_TRDT" } } },
//             { $project: { _id: 0, INVNAME: "$_id.INVNAME", PAN: "$_id.PAN1", TRXN_NATUR: "$_id.TRDESC", FOLIO_NO: "$_id.TD_ACNO", SCHEME: "$_id.FUNDDESC",STATUS:"$_id.STATUS" ,AMOUNT: "$_id.TD_AMT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TD_TRDT" } }, month: { $month: ('$_id.TD_TRDT') }, year: { $year: ('$_id.TD_TRDT') } } },
//             { $match: { $and: [{ month: mon }, { year: yer }, { PAN: pan } ,{ INVNAME: {$regex : `^${req.body.name}.*` , $options: 'si' } } , { TRXN_NATUR: /Systematic/ }  ] } },
//             { $sort: { TRADDATE: -1 } }
//         ]
//         const pipeline2 = [  ///trans_franklin
//             { $group: { _id: { INVESTOR_2: "$INVESTOR_2", IT_PAN_NO1: "$IT_PAN_NO1", TRXN_TYPE: "$TRXN_TYPE", FOLIO_NO: "$FOLIO_NO", SCHEME_NA1: "$SCHEME_NA1",SOCIAL_S18:"$SOCIAL_S18", AMOUNT: "$AMOUNT", TRXN_DATE: "$TRXN_DATE" } } },
//             { $project: { _id: 0, INVNAME: "$INVESTOR_2", PAN: "$_id.IT_PAN_NO1", TRXN_NATUR: "$_id.TRXN_TYPE", FOLIO_NO: "$_id.FOLIO_NO", SCHEME: "$_id.SCHEME_NA1",SOCIAL_S18:"$_id.SOCIAL_S18", AMOUNT: "$_id.AMOUNT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRXN_DATE" } }, month: { $month: ('$_id.TRXN_DATE') }, year: { $year: ('$_id.TRXN_DATE') } } },
//             { $match: { $and: [{ month: mon }, { year: yer }, { PAN: pan } , { INVNAME: {$regex : `^${req.body.name}.*` , $options: 'si' } }    ] } },
//             { $sort: { TRADDATE: -1 } }
//         ]

//         transc.aggregate(pipeline, (err, camsdata) => {
//             transk.aggregate(pipeline1, (err, karvydata) => {
//                 transf.aggregate(pipeline2, (err, frankdata) => {
//                     if (frankdata.length != 0 || karvydata.length != 0 || camsdata.length != 0) {
//                         resdata = {
//                             status: 200,
//                             message: 'Successfull',
//                             data: frankdata
//                         }
//                     } else {
//                         resdata = {
//                             status: 400,
//                             message: 'Data not found',
//                         }
//                     }
//                     datacon = frankdata.concat(karvydata.concat(camsdata))
//                     datacon = datacon.map(JSON.stringify).reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
//                         .filter(function (item, index, arr) { return arr.indexOf(item, index + 1) === -1; }) // check if there is any occurence of the item in whole array
//                         .reverse().map(JSON.parse);
//                     for (var i = 0; i < datacon.length; i++) {
//                         if (datacon[i]['TRXN_NATUR'].match(/Systematic.*/)) {
//                             datacon[i]['TRXN_NATUR'] = "SIP";
//                         }
//                         if ((Math.sign(datacon[i]['AMOUNT']) === -1)) {
//                             datacon[i]['TRXN_NATUR'] = "SIPR";
//                         }
//                         if (datacon[i]['TRXN_NATUR'].match(/Systematic - From.*/)) {
//                             datacon[i]['TRXN_NATUR'] = "STP";
//                         }
//                     }
//                     resdata.data = datacon.sort((a, b) => new Date(b.TRADDATE.split("-").reverse().join("/")).getTime() - new Date(a.TRADDATE.split("-").reverse().join("/")).getTime())
//                     res.json(resdata)
//                     return resdata
//                 });
//             });
//         });
//     }else{
//         resdata = {
//             status: 400,
//             message: 'Data not found',
//         }
//         res.json(resdata)
//          return resdata
//     }
// } catch (err) {
//     console.log(err)
// }
// })

// app.post("/api/getsipstpuserwise", function (req, res) {
//     try{
//         var member="";
//         var guardpan1=[];var guardpan2=[];
//         var arr1=[];var arr2=[];var arr3=[];var alldata=[];var arrFolio=[];var arrName=[];
//         let regex = /^([a-zA-Z]){5}([0-9]){4}([a-zA-Z]){1}?$/;
//         if(req.body.month ===""){
//             resdata = {
//                 status: 400,
//                 message: 'Please enter month',
//             }
//             res.json(resdata);
//             return resdata;
//         }else if(req.body.year ===""){
//             resdata = {
//                 status: 400,
//                 message: 'Please enter year',
//             }
//             res.json(resdata);
//             return resdata;
//         }else if(req.body.pan ===""){
//             resdata = {
//                 status: 400,
//                 message: 'Please enter pan',
//             }
//             res.json(resdata);
//             return resdata;
//         }else if(!regex.test(req.body.pan)) {
//             resdata = {
//                 status: 400,
//                 message: 'Please enter valid pan',
//             }
//             res.json(resdata);
//             return resdata;
//         }else{
//             var mon = parseInt(req.body.month);
//             var yer = parseInt(req.body.year);
//             family.find({ adminPan:  {$regex : `^${req.body.pan}.*` , $options: 'i' }  },{_id:0,memberPan:1}, function (err, member) {
//                 if(member!=""){
//                     member  = [...new Set(member.map(({memberPan}) => memberPan.toUpperCase()))];
//                     guardpan1.push({GUARD_PAN:req.body.pan.toUpperCase()});
//                     guardpan2.push({GUARDPANNO:req.body.pan.toUpperCase()});
//                     arr1.push({PAN:req.body.pan.toUpperCase()});
//                     for(var j=0;j<member.length;j++){     
//                     guardpan1.push({GUARD_PAN:member[j]}); 
//                     guardpan2.push({GUARDPANNO:member[j]});
//                     arr1.push({PAN:member[j]});
//                     }
//                     var strPan1 = {$or:guardpan1};
//                     var strPan2 = {$or:guardpan2};
//                     folioc.find(strPan1).distinct("FOLIOCHK", function (err, member1) {
//                       foliok.find(strPan2).distinct("ACNO", function (err, member2) {
//                       var alldata = member1.concat(member2);   
//                             for(var j=0;j<alldata.length;j++){     
//                                 arr1.push({FOLIO_NO:alldata[j]});
//                                 }
//                                 var strFolio = {$or:arr1};

//                         pipeline = [  ///trans_cams
//                             { $group: { _id: {  TAX_STATUS:"$TAX_STATUS",TRXNNO:"$TRXNNO",INV_NAME: "$INV_NAME", PAN: "$PAN", TRXN_NATUR: "$TRXN_NATUR", FOLIO_NO: "$FOLIO_NO", SCHEME: "$SCHEME", AMOUNT: "$AMOUNT" ,TRADDATE: "$TRADDATE" } } },
//                             { $project: { _id: 0,PER_STATUS:"$_id.TAX_STATUS", TRXNNO:"$_id.TRXNNO", INVNAME: "$_id.INV_NAME", PAN: "$_id.PAN", TRXN_NATUR: "$_id.TRXN_NATUR",  FOLIO_NO: "$_id.FOLIO_NO", SCHEME: "$_id.SCHEME", AMOUNT: "$_id.AMOUNT" , TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRADDATE" } }, month: { $month: ('$_id.TRADDATE') }, year: { $year: ('$_id.TRADDATE') } } },
//                             { $match: { $and: [{ month: mon }, { year: yer }, strFolio , { TRXN_NATUR: /Systematic/ } ] } },
//                             { $lookup: { from: 'folio_cams', localField: 'FOLIO_NO', foreignField: 'FOLIOCHK', as: 'detail' } },
//                             { $replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$detail", 0 ] }, "$$ROOT" ] } } } ,
//                             { $project: {    detail: 0 ,_id:0,TAX_STATUS:0,FOLIOCHK:0,AC_NO:0,FOLIO_DATE:0,PRODUCT:0,SCH_NAME:0,AMC_CODE:0,BANK_NAME:0,HOLDING_NA:0,IFSC_CODE:0,JNT_NAME1:0,JNT_NAME2:0,JOINT1_PAN:0,NOM2_NAME:0,NOM3_NAME:0,NOM_NAME:0,PRCODE:0,HOLDING_NATURE:0,PAN_NO:0,INV_NAME:0,EMAIL:0} },
//                             { $sort: { TRADDATE: -1 } }                      
//                         ]
//                          pipeline1 = [  ///trans_karvy
//                             { $group: { _id: { STATUS:"$STATUS",TD_TRNO:"$TD_TRNO",INVNAME: "$INVNAME", PAN1: "$PAN1", TRDESC: "$TRDESC",  TD_ACNO: "$TD_ACNO", FUNDDESC: "$FUNDDESC", TD_AMT: "$TD_AMT", TD_TRDT: "$TD_TRDT" } } },
//                             { $project: { _id: 0, PER_STATUS:"$_id.STATUS", TRXNNO:"$_id.TD_TRNO", INVNAME: "$_id.INVNAME", PAN: "$_id.PAN1", TRXN_NATUR: "$_id.TRDESC", FOLIO_NO: "$_id.TD_ACNO", SCHEME: "$_id.FUNDDESC" ,AMOUNT: "$_id.TD_AMT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TD_TRDT" } }, month: { $month: ('$_id.TD_TRDT') }, year: { $year: ('$_id.TD_TRDT') } } },
//                             { $match: { $and: [{ month: mon }, { year: yer }, strFolio , { TRXN_NATUR: /Systematic/ }  ] } },
//                             { $lookup: { from: 'folio_karvy', localField: 'FOLIO_NO', foreignField: 'ACNO', as: 'detail' } },
//                             { $replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$detail", 0 ] }, "$$ROOT" ] } } } ,
//                             { $project: { detail: 0 , _id:0,STATUS:0,PRCODE:0,STATUSDESC:0,ACNO:0,BNKACNO:0,BNKACTYPE:0,FUNDDESC:0,NOMINEE:0,MODEOFHOLD:0,JTNAME2:0,FUND:0,EMAIL:0,BNAME:0,PANGNO:0,JTNAME1:0,PAN2:0} },
//                             { $sort: { TRADDATE: -1 } }
//                         ]
//                          pipeline2 = [  ///trans_franklin
//                             { $group: { _id: {SOCIAL_S18:"$SOCIAL_S18",TRXN_NO:"$TRXN_NO", INVESTOR_2: "$INVESTOR_2", IT_PAN_NO1: "$IT_PAN_NO1", TRXN_TYPE: "$TRXN_TYPE", FOLIO_NO: "$FOLIO_NO", SCHEME_NA1: "$SCHEME_NA1", AMOUNT: "$AMOUNT", TRXN_DATE: "$TRXN_DATE" } } },
//                             { $project: { _id: 0,PER_STATUS:"$_id.SOCIAL_S18",TRXNNO:"$_id.TRXN_NO", INVNAME: "$_id.INVESTOR_2", PAN: "$_id.IT_PAN_NO1", TRXN_NATUR: "$_id.TRXN_TYPE", FOLIO_NO: "$_id.FOLIO_NO", SCHEME: "$_id.SCHEME_NA1", AMOUNT: "$_id.AMOUNT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRXN_DATE" } }, month: { $month: ('$_id.TRXN_DATE') }, year: { $year: ('$_id.TRXN_DATE') } } },
//                             { $match: { $and: [{ month: mon }, { year: yer }, strFolio , { TRXN_NATUR: /SIP/ } ] } },
//                             { $lookup: { from: 'folio_franklin', localField: 'FOLIO_NO', foreignField: 'FOLIO_NO', as: 'detail' } },
//                             { $replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$detail", 0 ] }, "$$ROOT" ] } } } ,
//                             { $project: { detail: 0 ,_id:0,TAX_STATUS:0,PERSONAL_9:0,ACCNT_NO:0,AC_TYPE:0,ADDRESS1:0,BANK_CODE:0,BANK_NAME:0,COMP_CODE:0,D_BIRTH:0,EMAIL:0,HOLDING_T6:0,F_NAME:0,IFSC_CODE:0,JOINT_NAM1:0,JOINT_NAM2:0,KYC_ID:0,NEFT_CODE:0,NOMINEE1:0,PBANK_NAME:0,PANNO2:0,PANNO1:0,PHONE_RES:0,SOCIAL_ST7:0} },
//                             { $sort: { TRADDATE: -1 } }
//                         ]
//                          transc.aggregate(pipeline, (err, camsdata) => {
//                             transk.aggregate(pipeline1, (err, karvydata) => {
//                                 transf.aggregate(pipeline2, (err, frankdata) => {
//                                     if (frankdata != 0 || karvydata != 0 || camsdata != 0) {
//                                         resdata = {
//                                             status: 200,
//                                             message: 'Successfull',
//                                             data: frankdata
//                                         }
//                                         datacon = frankdata.concat(karvydata.concat(camsdata));
                                       
//                                         datacon = datacon.map(JSON.stringify).reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
//                                             .filter(function (item, index, arr) { return arr.indexOf(item, index + 1) === -1; }) // check if there is any occurence of the item in whole array
//                                             .reverse().map(JSON.parse);
//                                             datacon = Array.from(new Set(datacon));
//                                             var newdata1 = datacon.map(item=>{
//                                                 return [JSON.stringify(item),item]
//                                                  }); // creates array of array
//                                                  var maparr1 = new Map(newdata1); // create key value pair from array of array
//                                                  datacon = [...maparr1.values()];//converting back to array from mapobject 
                                                 
//                                                  datacon = datacon.map(function(obj) {
//                                                     if(obj['GUARDIANN0']){
//                                                         obj['GUARD_NAME'] = obj['GUARDIANN0']; // Assign new key
//                                                         obj['GUARD_PAN'] = obj['GUARDPANNO'];
//                                                          // Delete old key
//                                                               delete obj['GUARDIANN0'];
//                                                               delete obj['GUARDPANNO'];
//                                                     }else if((obj['GUARDIANN0']) === ""){
//                                                             obj['GUARD_NAME'] = obj['GUARDIANN0']; // Assign new key
//                                                             obj['GUARD_PAN'] = obj['GUARDPANNO'];
//                                                             delete obj['GUARDIANN0'];
//                                                             delete obj['GUARDPANNO'];
//                                                         }
//                                                     if(obj['GUARDIAN20']){
//                                                         obj['GUARD_NAME'] = obj['GUARDIAN20']; // Assign new key
//                                                          // Delete old key
//                                                         delete obj['GUARDIAN20'];
//                                                     }else if((obj['GUARDIAN20']) === ""){
//                                                         obj['GUARD_NAME'] = obj['GUARDIAN20']; // Assign new key
//                                                         delete obj['GUARDIAN20'];
//                                                     }
//                                                         return obj;
                                                    
//                                                     });
//                                         for (var i = 0; i < datacon.length; i++) {
//                                                 if (datacon[i]['TRXN_NATUR'].match(/^Systematic/)) {
//                                                     datacon[i]['TRXN_NATUR'] = "SIP";
//                                                 }
//                                                 if(Math.sign(datacon[i]['AMOUNT']) === -1){
//                                                     datacon[i]['TRXN_NATUR'] = "SIPR";
//                                                 }
//                                                 if (datacon[i]['TRXN_NATUR'].match(/Systematic - From.*/)) {
//                                                     datacon[i]['TRXN_NATUR'] = "STP";
//                                                 }if (datacon[i]['PER_STATUS'] === "On Behalf Of Minor" || datacon[i]['PER_STATUS'] === "MINOR" || datacon[i]['PER_STATUS'] === "On Behalf of Minor" )  {
//                                                     datacon[i]['PER_STATUS'] = "Minor";   
// 						    datacon[i]['PAN'] = "";  
//                                                  }if (datacon[i]['PER_STATUS'] === "INDIVIDUAL" || datacon[i]['PER_STATUS'] === "Resident Individual") {
//                                                          datacon[i]['PER_STATUS'] = "Individual";
//                                                }if (datacon[i]['PER_STATUS'] === "HINDU UNDIVIDED FAMI") {
//                                                          datacon[i]['PER_STATUS'] = "HUF";
//                                                }
//                                             }                 
//                                             resdata.data = datacon.sort((a, b) => new Date(b.TRADDATE.split("-").reverse().join("/")).getTime() - new Date(a.TRADDATE.split("-").reverse().join("/")).getTime());
//                                             res.json(resdata);
//                                            return resdata;
//                                            } else{
//                                             resdata = {
//                                                 status: 400,
//                                                 message: 'Data not found',
//                                             }
//                                             res.json(resdata);
//                                            return resdata;
//                                            }
                                           
//                                         });
//                                     });
//                                  });
//                                });
//                            });
//                                }else{
//                                 //console.log("fff",allPan);
//                                 pipeline = [  ///trans_cams
//                                     { $group: { _id: {  TAX_STATUS:"$TAX_STATUS",TRXNNO:"$TRXNNO",INV_NAME: "$INV_NAME", PAN: "$PAN", TRXN_NATUR: "$TRXN_NATUR", FOLIO_NO: "$FOLIO_NO", SCHEME: "$SCHEME", AMOUNT: "$AMOUNT" ,TRADDATE: "$TRADDATE" } } },
//                                     { $project: { _id: 0,PER_STATUS:"$_id.TAX_STATUS", TRXNNO:"$_id.TRXNNO", INVNAME: "$_id.INV_NAME", PAN: "$_id.PAN", TRXN_NATUR: "$_id.TRXN_NATUR",  FOLIO_NO: "$_id.FOLIO_NO", SCHEME: "$_id.SCHEME", AMOUNT: "$_id.AMOUNT" , TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRADDATE" } }, month: { $month: ('$_id.TRADDATE') }, year: { $year: ('$_id.TRADDATE') } } },
//                                     { $match: { $and: [{ month: mon }, { year: yer }, {PAN:req.body.pan}, { TRXN_NATUR: /Systematic/ } ] } },
//                                     { $lookup: { from: 'folio_cams', localField: 'FOLIO_NO', foreignField: 'FOLIOCHK', as: 'detail' } },
//                                     { $replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$detail", 0 ] }, "$$ROOT" ] } } } ,
//                                     { $project: {    detail: 0 ,_id:0,TAX_STATUS:0,FOLIOCHK:0,AC_NO:0,FOLIO_DATE:0,PRODUCT:0,SCH_NAME:0,AMC_CODE:0,BANK_NAME:0,HOLDING_NA:0,IFSC_CODE:0,JNT_NAME1:0,JNT_NAME2:0,JOINT1_PAN:0,NOM2_NAME:0,NOM3_NAME:0,NOM_NAME:0,PRCODE:0,HOLDING_NATURE:0,PAN_NO:0,INV_NAME:0,EMAIL:0} },
//                                     { $sort: { TRADDATE: -1 } }                      
//                                 ]
//                                  pipeline1 = [  ///trans_karvy
//                                     { $group: { _id: { STATUS:"$STATUS",TD_TRNO:"$TD_TRNO",INVNAME: "$INVNAME", PAN1: "$PAN1", TRDESC: "$TRDESC",  TD_ACNO: "$TD_ACNO", FUNDDESC: "$FUNDDESC", TD_AMT: "$TD_AMT", TD_TRDT: "$TD_TRDT" } } },
//                                     { $project: { _id: 0, PER_STATUS:"$_id.STATUS", TRXNNO:"$_id.TD_TRNO", INVNAME: "$_id.INVNAME", PAN: "$_id.PAN1", TRXN_NATUR: "$_id.TRDESC", FOLIO_NO: "$_id.TD_ACNO", SCHEME: "$_id.FUNDDESC" ,AMOUNT: "$_id.TD_AMT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TD_TRDT" } }, month: { $month: ('$_id.TD_TRDT') }, year: { $year: ('$_id.TD_TRDT') } } },
//                                     { $match: { $and: [{ month: mon }, { year: yer },{PAN:req.body.pan}, { TRXN_NATUR: /Systematic/ }  ] } },
//                                     { $lookup: { from: 'folio_karvy', localField: 'FOLIO_NO', foreignField: 'ACNO', as: 'detail' } },
//                                     { $replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$detail", 0 ] }, "$$ROOT" ] } } } ,
//                                     { $project: { detail: 0 , _id:0,STATUS:0,PRCODE:0,STATUSDESC:0,ACNO:0,BNKACNO:0,BNKACTYPE:0,FUNDDESC:0,NOMINEE:0,MODEOFHOLD:0,JTNAME2:0,FUND:0,EMAIL:0,BNAME:0,PANGNO:0,JTNAME1:0,PAN2:0} },
//                                     { $sort: { TRADDATE: -1 } }
//                                 ]
//                                  pipeline2 = [  ///trans_franklin
//                                     { $group: { _id: {SOCIAL_S18:"$SOCIAL_S18",TRXN_NO:"$TRXN_NO", INVESTOR_2: "$INVESTOR_2", IT_PAN_NO1: "$IT_PAN_NO1", TRXN_TYPE: "$TRXN_TYPE", FOLIO_NO: "$FOLIO_NO", SCHEME_NA1: "$SCHEME_NA1", AMOUNT: "$AMOUNT", TRXN_DATE: "$TRXN_DATE" } } },
//                                     { $project: { _id: 0,PER_STATUS:"$_id.SOCIAL_S18",TRXNNO:"$_id.TRXN_NO", INVNAME: "$_id.INVESTOR_2", PAN: "$_id.IT_PAN_NO1", TRXN_NATUR: "$_id.TRXN_TYPE", FOLIO_NO: "$_id.FOLIO_NO", SCHEME: "$_id.SCHEME_NA1", AMOUNT: "$_id.AMOUNT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRXN_DATE" } }, month: { $month: ('$_id.TRXN_DATE') }, year: { $year: ('$_id.TRXN_DATE') } } },
//                                     { $match: { $and: [{ month: mon }, { year: yer },{PAN:req.body.pan} , { TRXN_NATUR: /SIP/ } ] } },
//                                     { $lookup: { from: 'folio_franklin', localField: 'FOLIO_NO', foreignField: 'FOLIO_NO', as: 'detail' } },
//                                     { $replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$detail", 0 ] }, "$$ROOT" ] } } } ,
//                                     { $project: { detail: 0 ,_id:0,TAX_STATUS:0,PERSONAL_9:0,ACCNT_NO:0,AC_TYPE:0,ADDRESS1:0,BANK_CODE:0,BANK_NAME:0,COMP_CODE:0,D_BIRTH:0,EMAIL:0,HOLDING_T6:0,F_NAME:0,IFSC_CODE:0,JOINT_NAM1:0,JOINT_NAM2:0,KYC_ID:0,NEFT_CODE:0,NOMINEE1:0,PBANK_NAME:0,PANNO2:0,PANNO1:0,PHONE_RES:0,SOCIAL_ST7:0} },
//                                     { $sort: { TRADDATE: -1 } }
//                                 ]
                        
//                                 transc.aggregate(pipeline, (err, camsdata) => {
//                                     transk.aggregate(pipeline1, (err, karvydata) => {
//                                         transf.aggregate(pipeline2, (err, frankdata) => {
//                                             if (frankdata.length != 0 || karvydata.length != 0 || camsdata.length != 0) {
//                                                 resdata = {
//                                                     status: 200,
//                                                     message: 'Successfull',
//                                                     data: frankdata
//                                                 }
                                           
//                                             datacon = frankdata.concat(karvydata.concat(camsdata))
// //                                             datacon = datacon.map(JSON.stringify).reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
// //                                                 .filter(function (item, index, arr) { return arr.indexOf(item, index + 1) === -1; }) // check if there is any occurence of the item in whole array
// //                                                 .reverse().map(JSON.parse);
//                                             for (var i = 0; i < datacon.length; i++) {
//                                                 if (datacon[i]['TRXN_NATUR'].match(/Systematic.*/)) {
//                                                     datacon[i]['TRXN_NATUR'] = "SIP";
//                                                 }
//                                                 if ((Math.sign(datacon[i]['AMOUNT']) === -1)) {
//                                                     datacon[i]['TRXN_NATUR'] = "SIPR";
//                                                 }
//                                                 if (datacon[i]['TRXN_NATUR'].match(/Systematic - From.*/)) {
//                                                     datacon[i]['TRXN_NATUR'] = "STP";
//                                                 }
//                                             }
//                                             resdata.data = datacon.sort((a, b) => new Date(b.TRADDATE.split("-").reverse().join("/")).getTime() - new Date(a.TRADDATE.split("-").reverse().join("/")).getTime())
//                                             res.json(resdata)
//                                             return resdata;
//                                         } else {
//                                             resdata = {
//                                                 status: 400,
//                                                 message: 'Data not found',
//                                             }
//                                         }
//                                         });
//                                     });
//                                 });
//                                   }       
//                                });
//                            }
//                        } catch (err) {
//                            console.log(err)
//                        }
//                        })

app.post("/api/getsipstpuserwise", function (req, res) {
    try{
        var member="";
        var guardpan1=[];var guardpan2=[];
        var arr1=[];var arr2=[];var arr3=[];var alldata=[];var arrFolio=[];var arrName=[];
        let regex = /^([a-zA-Z]){5}([0-9]){4}([a-zA-Z]){1}?$/;
            if(req.body.month ===""){
                resdata = {
                    status: 400,
                    message: 'Please enter month',
                }
                res.json(resdata);
                return resdata;
            }else if(req.body.year ===""){
                resdata = {
                    status: 400,
                    message: 'Please enter year',
                }
                res.json(resdata);
                return resdata;
            }else if(req.body.pan ===""){
                resdata = {
                    status: 400,
                    message: 'Please enter pan',
                }
                res.json(resdata);
                return resdata;
            }else if(!regex.test(req.body.pan)) {
                resdata = {
                    status: 400,
                    message: 'Please enter valid pan',
                }
                res.json(resdata);
                return resdata;
            }else{
                 var lastdate =  new Date(req.body.year, req.body.month, 0).getDate();
                var firstdate = req.body.year+"-"+req.body.month+"-"+"01";
                var seconddate = req.body.year+"-"+req.body.month+"-"+lastdate;
                family.find({ adminPan:  {$regex : `^${req.body.pan}.*` , $options: 'i' }  },{_id:0,memberPan:1}, function (err, member) {
                    if(member!=""){
                        member  = [...new Set(member.map(({memberPan}) => memberPan.toUpperCase()))];
                        guardpan1.push({GUARD_PAN:req.body.pan.toUpperCase()});
                        guardpan2.push({GUARDPANNO:req.body.pan.toUpperCase()});
                        arr1.push({PAN:req.body.pan.toUpperCase()});
                        arr2.push({PAN1:req.body.pan.toUpperCase()});
                        for(var j=0;j<member.length;j++){     
                        guardpan1.push({GUARD_PAN:member[j]}); 
                        guardpan2.push({GUARDPANNO:member[j]});
                        arr1.push({PAN:member[j]});
                        arr2.push({PAN1:member[j]});
                        }
                        var strPan1 = {$or:guardpan1};
                        var strPan2 = {$or:guardpan2};
                        folioc.find(strPan1).distinct("FOLIOCHK", function (err, member1) {
                          foliok.find(strPan2).distinct("ACNO", function (err, member2) {
                          var alldata = member1.concat(member2);   
                          for(var j=0;j<alldata.length;j++){     
                            arr1.push({FOLIO_NO:alldata[j]});
                            arr2.push({TD_ACNO:alldata[j]});
                            }
                            var strFolio = {$or:arr1};
                            var strFolio1 = {$or:arr2};
                            pipeline = [  ///trans_cams
                                { $match: { $and: [{TRADDATE: { $gte: new Date(moment(firstdate).format("YYYY-MM-DD")), $lt: new Date(moment(seconddate).format("YYYY-MM-DD")) } }, strFolio,{ TRXN_NATUR: /Systematic/ }  ] } },

                                { $group: { _id: {  TAX_STATUS:"$TAX_STATUS",TRXNNO:"$TRXNNO",INV_NAME: "$INV_NAME", PAN: "$PAN", TRXN_NATUR: "$TRXN_NATUR", FOLIO_NO: "$FOLIO_NO", SCHEME: "$SCHEME", AMOUNT: "$AMOUNT" ,TRADDATE: "$TRADDATE" } } },

                                { $lookup: { from: 'folio_cams', localField: '_id.FOLIO_NO', foreignField: 'FOLIOCHK', as: 'detail' } },
                                //{ $unwind: "$detail" },
                                { $project: {_id:0,GUARD_NAME:"$detail.GUARD_NAME",GUARD_PAN:"$detail.GUARD_PAN",PER_STATUS:"$_id.TAX_STATUS", TRXNNO:"$_id.TRXNNO", INVNAME: "$_id.INV_NAME", PAN: "$_id.PAN", TRXN_NATUR: "$_id.TRXN_NATUR",  FOLIO_NO: "$_id.FOLIO_NO",SCHEME: "$_id.SCHEME", AMOUNT: "$_id.AMOUNT" , TRADDATE:{ $dateToString:{ format: "%d-%m-%Y", date: "$_id.TRADDATE"} } } },
                               
                                { $sort: { TRADDATE: -1 } }                      
                            ]
                             pipeline1 = [  ///trans_karvy
                                { $match: { $and: [strFolio1, {TD_TRDT: { $gte: new Date(moment(firstdate).format("YYYY-MM-DD")), $lt: new Date(moment(seconddate).format("YYYY-MM-DD")) } } ,{ TRDESC: /Systematic/ }  ] } },

                                { $group: { _id: { STATUS:"$STATUS",TD_TRNO:"$TD_TRNO",INVNAME: "$INVNAME", PAN1: "$PAN1", TRDESC: "$TRDESC",  TD_ACNO: "$TD_ACNO", FUNDDESC: "$FUNDDESC", TD_AMT: "$TD_AMT", TD_TRDT: "$TD_TRDT" } } },

                                { $lookup: { from: 'folio_karvy', localField: '_id.TD_ACNO', foreignField: 'ACNO', as: 'detail' } },
                                //{ $unwind: "$detail" },
                                { $project: { _id: 0,GUARD_NAME:"$detail.GUARDIANN0",GUARD_PAN:"$detail.GUARDPANNO", PER_STATUS:"$_id.STATUS", TRXNNO:"$_id.TD_TRNO", INVNAME: "$_id.INVNAME", PAN: "$_id.PAN1", TRXN_NATUR: "$_id.TRDESC", FOLIO_NO: "$_id.TD_ACNO", SCHEME: "$_id.FUNDDESC" ,AMOUNT: "$_id.TD_AMT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TD_TRDT" } } } },
                               
                                { $sort: { TRADDATE: -1 } }
                            ]
                            transc.aggregate(pipeline, (err, camsdata) => {
                                transk.aggregate(pipeline1, (err, karvydata) => {
                                    if (karvydata != 0 || camsdata != 0) {
                                        resdata = {
                                            status: 200,
                                            message: 'Successfull',
                                            data: frankdata
                                        }
                                        datacon = karvydata.concat(camsdata);
                                        datacon = datacon.map(JSON.stringify).reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
                                            .filter(function (item, index, arr) { return arr.indexOf(item, index + 1) === -1; }) // check if there is any occurence of the item in whole array
                                            .reverse().map(JSON.parse);
                                            datacon = Array.from(new Set(datacon));
                                            var newdata1 = datacon.map(item=>{
                                                return [JSON.stringify(item),item]
                                                 }); // creates array of array
                                                 var maparr1 = new Map(newdata1); // create key value pair from array of array
                                                 datacon = [...maparr1.values()];//converting back to array from mapobject 
                                                 
                                                    for (var i = 0; i < datacon.length; i++) {
                                                        if (datacon[i]['GUARD_NAME']) {
                                                            datacon[i]['GUARD_NAME'] = datacon[i]['GUARD_NAME'][0] ;
                                                        } if (datacon[i]['GUARD_PAN']) {
                                                            datacon[i]['GUARD_PAN'] = datacon[i]['GUARD_PAN'][0] ;
                                                        }
                                                        if (datacon[i]['TRXN_NATUR'].match(/^Systematic/)) {
                                                            datacon[i]['TRXN_NATUR'] = "SIP";
                                                        }
                                                        if(Math.sign(datacon[i]['AMOUNT']) === -1){
                                                            datacon[i]['TRXN_NATUR'] = "SIPR";
                                                        }
                                                        if (datacon[i]['TRXN_NATUR'].match(/Systematic - From.*/)) {
                                                            datacon[i]['TRXN_NATUR'] = "STP";
                                                        }if (datacon[i]['PER_STATUS'] === "On Behalf Of Minor" || datacon[i]['PER_STATUS'] === "MINOR" || datacon[i]['PER_STATUS'] === "On Behalf of Minor" )  {
                                                            datacon[i]['PER_STATUS'] = "Minor";   
                                                             datacon[i]['PAN'] = "";  
                                                         }if (datacon[i]['PER_STATUS'] === "INDIVIDUAL" || datacon[i]['PER_STATUS'] === "Resident Individual") {
                                                                 datacon[i]['PER_STATUS'] = "Individual";
                                                       }if (datacon[i]['PER_STATUS'] === "HINDU UNDIVIDED FAMI") {
                                                                 datacon[i]['PER_STATUS'] = "HUF";
                                                       }
                                                       if (datacon[i]['GUARD_NAME'] === null && datacon[i]['GUARD_PAN'] === null) {
                                                        datacon[i]['GUARD_NAME'] = "";
                                                        datacon[i]['GUARD_PAN'] = "";
                                                      }
                                                    }                 
                                                    resdata.data = datacon.sort((a, b) => new Date(b.TRADDATE.split("-").reverse().join("/")).getTime() - new Date(a.TRADDATE.split("-").reverse().join("/")).getTime());
                                                    res.json(resdata);
                                                   return resdata;
                                    } else{
                                        resdata = {
                                            status: 400,
                                            message: 'Data not found',
                                        }
                                        res.json(resdata);
                                       return resdata;
                                    }
                                });
                              })
                            });
                        })
                    }else{
                        pipeline = [  ///trans_cams
                            { $match: { $and: [{TRADDATE: { $gte: new Date(moment(firstdate).format("YYYY-MM-DD")), $lt: new Date(moment(seconddate).format("YYYY-MM-DD")) } }, {PAN:req.body.pan},{ TRXN_NATUR: /Systematic/ }  ] } },

                            { $group: { _id: {  TAX_STATUS:"$TAX_STATUS",TRXNNO:"$TRXNNO",INV_NAME: "$INV_NAME", PAN: "$PAN", TRXN_NATUR: "$TRXN_NATUR", FOLIO_NO: "$FOLIO_NO", SCHEME: "$SCHEME", AMOUNT: "$AMOUNT" ,TRADDATE: "$TRADDATE" } } },
                            { $lookup: { from: 'folio_cams', localField: '_id.FOLIO_NO', foreignField: 'FOLIOCHK', as: 'detail' } },
                            //{ $unwind: "$detail" },
                            { $project: { _id: 0,GUARD_NAME:"$detail.GUARD_NAME",GUARD_PAN:"$detail.GUARD_PAN",PER_STATUS:"$_id.TAX_STATUS", TRXNNO:"$_id.TRXNNO", INVNAME: "$_id.INV_NAME", PAN: "$_id.PAN", TRXN_NATUR: "$_id.TRXN_NATUR",  FOLIO_NO: "$_id.FOLIO_NO", SCHEME: "$_id.SCHEME", AMOUNT: "$_id.AMOUNT" , TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRADDATE" } }} },
                            
                            { $sort: { TRADDATE: -1 } }                      
                        ]
                         pipeline1 = [  ///trans_karvy
                            { $match: { $and: [{PAN1:req.body.pan}, {TD_TRDT: { $gte: new Date(moment(firstdate).format("YYYY-MM-DD")), $lt: new Date(moment(seconddate).format("YYYY-MM-DD")) } },{ TRDESC: /Systematic/ }   ] } },

                            { $group: { _id: { STATUS:"$STATUS",TD_TRNO:"$TD_TRNO",INVNAME: "$INVNAME", PAN1: "$PAN1", TRDESC: "$TRDESC",  TD_ACNO: "$TD_ACNO", FUNDDESC: "$FUNDDESC", TD_AMT: "$TD_AMT", TD_TRDT: "$TD_TRDT" } } },
                            { $lookup: { from: 'folio_karvy', localField: '_id.TD_ACNO', foreignField: 'ACNO', as: 'detail' } },
                            //{ $unwind: "$detail" },
                            { $project: { _id: 0,GUARD_NAME:"$detail.GUARDIANN0",GUARD_PAN:"$detail.GUARDPANNO", PER_STATUS:"$_id.STATUS", TRXNNO:"$_id.TD_TRNO", INVNAME: "$_id.INVNAME", PAN: "$_id.PAN1", TRXN_NATUR: "$_id.TRDESC", FOLIO_NO: "$_id.TD_ACNO", SCHEME: "$_id.FUNDDESC" ,AMOUNT: "$_id.TD_AMT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TD_TRDT" } } } },
                           
                            
                            { $sort: { TRADDATE: -1 } }
                        ]
                        transc.aggregate(pipeline, (err, camsdata) => {
                           transk.aggregate(pipeline1, (err, karvydata) => {

                            if ( karvydata.length != 0 || camsdata.length != 0) {
                                resdata = {
                                    status: 200,
                                    message: 'Successfull',
                                    data: frankdata
                                }
                                datacon = karvydata.concat(camsdata);
                                datacon = datacon.map(JSON.stringify).reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
                                            .filter(function (item, index, arr) { return arr.indexOf(item, index + 1) === -1; }) // check if there is any occurence of the item in whole array
                                            .reverse().map(JSON.parse);
                                            datacon = Array.from(new Set(datacon));
                            for (var i = 0; i < datacon.length; i++) {
                                if (datacon[i]['TRXN_NATUR'].match(/Systematic.*/)) {
                                    datacon[i]['TRXN_NATUR'] = "SIP";
                                }
                                if ((Math.sign(datacon[i]['AMOUNT']) === -1)) {
                                    datacon[i]['TRXN_NATUR'] = "SIPR";
                                }
                                if (datacon[i]['TRXN_NATUR'].match(/Systematic - From.*/)) {
                                    datacon[i]['TRXN_NATUR'] = "STP";
                                }if (datacon[i]['GUARD_NAME'] === null && datacon[i]['GUARD_PAN'] === null) {
                                    datacon[i]['GUARD_NAME'] = "";
                                    datacon[i]['GUARD_PAN'] = "";
                                  }
                                  if (datacon[i]['GUARD_NAME']) {
                                    datacon[i]['GUARD_NAME'] = datacon[i]['GUARD_NAME'][0] ;
                                } if (datacon[i]['GUARD_PAN']) {
                                    datacon[i]['GUARD_PAN'] = datacon[i]['GUARD_PAN'][0] ;
                                }
                            }
                            resdata.data = datacon.sort((a, b) => new Date(b.TRADDATE.split("-").reverse().join("/")).getTime() - new Date(a.TRADDATE.split("-").reverse().join("/")).getTime());
                            res.json(resdata)
                            return resdata;
                           } else{
                            resdata = {
                                status: 400,
                                message: 'Data not found',
                            }
                            res.json(resdata);
                           return resdata;
                        }
                        });
                      });
                    }
                });   
            }
    } catch (err) {
        console.log(err)
    }
})

// app.post("/api/getdividendscheme", function (req, res) {
//     try{
//     var yer = req.body.fromyear;
//     var secyer = req.body.toyear;
//     yer = yer + "-04-01";
//     secyer = secyer + "-03-31"
//     if(req.body.pan != "" && req.body.name != ""){
//     const pipeline = [  ///trans_cams                                                     
//         { $match: { $and: [{ TRXN_NATUR: /Div/ }, { SCHEME: req.body.scheme },{ INV_NAME: {$regex : `^${req.body.name}.*` , $options: 'i' } }, { PAN: req.body.pan }, { TRADDATE: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }] } },
//         { $group: { _id: { INV_NAME: "$INV_NAME", SCHEME: "$SCHEME", TRXN_NATUR: "$TRXN_NATUR", FOLIO_NO: "$FOLIO_NO", AMOUNT: "$AMOUNT", TRADDATE: "$TRADDATE" } } },
//         { $project: { _id: 0, INVNAME: "$_id.INV_NAME", SCHEME: "$_id.SCHEME", TRXN_NATUR: "$_id.TRXN_NATUR", FOLIO_NO: "$_id.FOLIO_NO", AMOUNT: "$_id.AMOUNT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRADDATE" } } } },
//     ]
//     const pipeline1 = [  ///trans_karvy
//         { $match: { $and: [{ TRDESC: /Div/ }, { FUNDDESC: req.body.scheme },{ INVNAME: {$regex : `^${req.body.name}.*` , $options: 'i' } } , { PAN1: req.body.pan }, { TD_TRDT: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }] } },
//         { $group: { _id: { INVNAME: "$INVNAME", FUNDDESC: "$FUNDDESC", TRDESC: "$TRDESC", TD_ACNO: "$TD_ACNO", TD_AMT: "$TD_AMT", TD_TRDT: "$TD_TRDT" } } },
//         { $project: { _id: 0, INVNAME: "$_id.INVNAME", SCHEME: "$_id.FUNDDESC", TRXN_NATUR: "$_id.TRDESC", FOLIO_NO: "$_id.TD_ACNO", AMOUNT: "$_id.TD_AMT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TD_TRDT" } } } },
//     ]
//     const pipeline2 = [  ///trans_franklin
//         { $match: { $and: [{ TRXN_TYPE: /Div/ }, { SCHEME_NA1: req.body.scheme },{ INVESTOR_2: {$regex : `^${req.body.name}.*` , $options: 'i' } }, { IT_PAN_NO1: req.body.pan }, { TRXN_DATE: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }] } },
//         { $group: { _id: { INVESTOR_2: "$INVESTOR_2", SCHEME_NA1: "$SCHEME_NA1", TRXN_TYPE: "$TRXN_TYPE", FOLIO_NO: "$FOLIO_NO", AMOUNT: "$AMOUNT", TRXN_DATE: "$TRXN_DATE" } } },
//         { $project: { _id: 0, INVNAME: "$_id.INVESTOR_2", SCHEME: "$_id.SCHEME_NA1", TRXN_NATUR: "$_id.TRXN_TYPE", FOLIO_NO: "$_id.FOLIO_NO", AMOUNT: "$_id.AMOUNT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRXN_DATE" } } } },
//     ]

//     transc.aggregate(pipeline, (err, camsdata) => {
//         transk.aggregate(pipeline1, (err, karvydata) => {
//             transf.aggregate(pipeline2, (err, frankdata) => {
//                 if (camsdata != 0 || karvydata != 0 || frankdata != 0) {
//                     resdata = {
//                         status: 200,
//                         message: 'Successfull',
//                         data: frankdata
//                     }
//                 } else {
//                     resdata = {
//                         status: 400,
//                         message: 'Data not found',
//                     }
//                 }
//                 var datacon = frankdata.concat(karvydata.concat(camsdata))
//                 datacon = datacon.map(JSON.stringify).reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
//                     .filter(function (item, index, arr) { return arr.indexOf(item, index + 1) === -1; }) // check if there is any occurence of the item in whole array
//                     .reverse().map(JSON.parse);
//                 for (var i = 0; i < datacon.length; i++) {
//                     if (datacon[i]['TRXN_NATUR'] === "Gross Dividend") {
//                         datacon[i]['TRXN_NATUR'] = "Dividend Payout";
//                     } if (datacon[i]['TRXN_NATUR'].match(/Div. Rei.*/) || datacon[i]['TRXN_NATUR'].match(/Dividend Reinvest*/)) {
//                         datacon[i]['TRXN_NATUR'] = "Div. Reinv.";
//                     } if (datacon[i]['TRXN_NATUR'].match(/Dividend Paid*/)) {
//                         datacon[i]['TRXN_NATUR'] = "Div. Paid";
//                     }
//                 }
//                 resdata.data = datacon;
//                 resdata.data = datacon.sort((a, b) => new Date(b.TRADDATE.split("-").reverse().join("/")).getTime() - new Date(a.TRADDATE.split("-").reverse().join("/")).getTime())
//                 res.json(resdata)
//                 return resdata
//             });
//         });
//     });
// }else{
//     resdata = {
//         status: 400,
//         message: 'Data not found',
//     }
// 	 res.json(resdata)
//          return resdata
// }
// } catch (err) {
//     console.log(err)
// }
// });

app.post("/api/getdividendscheme", function (req, res) {
    try{
        let regex = /^([a-zA-Z]){5}([0-9]){4}([a-zA-Z]){1}?$/;
        if(req.body.fromyear ===""){
            resdata = {
                status: 400,
                message: 'Please enter from year',
            }           
        }else if(req.body.toyear ===""){
            resdata = {
                status: 400,
                message: 'Please enter to year',
            }
        }else if(req.body.pan ===""){
            resdata = {
                status: 400,
                message: 'Please enter pan',
            }
        }else if(!regex.test(req.body.pan)) {
            resdata = {
                status: 400,
                message: 'Please enter valid pan',
            }
        }else if(req.body.scheme ===""){
                resdata = {
                    status: 400,
                    message: 'Please enter scheme',
                }
        } else{
            
    var yer = req.body.fromyear;
    var secyer = req.body.toyear;
    yer = yer + "-04-01";
    secyer = secyer + "-03-31"
//    if(req.body.pan != "" ){
    const pipeline = [  ///trans_cams                                                     
        { $match: { $and: [{ TRXN_NATUR: /Div/ }, { SCHEME: req.body.scheme },{ PAN: req.body.pan }, { TRADDATE: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }] } },
        { $group: { _id: { INV_NAME: "$INV_NAME", SCHEME: "$SCHEME", TRXN_NATUR: "$TRXN_NATUR", FOLIO_NO: "$FOLIO_NO", AMOUNT: "$AMOUNT", TRADDATE: "$TRADDATE" } } },
        { $project: { _id: 0, INVNAME: "$_id.INV_NAME", SCHEME: "$_id.SCHEME", TRXN_NATUR: "$_id.TRXN_NATUR", FOLIO_NO: "$_id.FOLIO_NO", AMOUNT: "$_id.AMOUNT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRADDATE" } } } },
    ]
    const pipeline1 = [  ///trans_karvy
        { $match: { $and: [{ TRDESC: /Div/ }, { FUNDDESC: req.body.scheme }, { PAN1: req.body.pan }, { TD_TRDT: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }] } },
        { $group: { _id: { INVNAME: "$INVNAME", FUNDDESC: "$FUNDDESC", TRDESC: "$TRDESC", TD_ACNO: "$TD_ACNO", TD_AMT: "$TD_AMT", TD_TRDT: "$TD_TRDT" } } },
        { $project: { _id: 0, INVNAME: "$_id.INVNAME", SCHEME: "$_id.FUNDDESC", TRXN_NATUR: "$_id.TRDESC", FOLIO_NO: "$_id.TD_ACNO", AMOUNT: "$_id.TD_AMT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TD_TRDT" } } } },
    ]
    const pipeline2 = [  ///trans_franklin
        { $match: { $and: [{ $or: [{ TRXN_TYPE: /DIR/ }, { TRXN_TYPE: /DP/ }] }, { SCHEME_NA1: req.body.scheme }, { IT_PAN_NO1: req.body.pan }, { TRXN_DATE: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }] } },
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
                    var datacon = frankdata.concat(karvydata.concat(camsdata));

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
                //resdata.data = datacon;
                resdata.data = datacon.sort((a, b) => new Date(b.TRADDATE.split("-").reverse().join("/")).getTime() - new Date(a.TRADDATE.split("-").reverse().join("/")).getTime())
                res.json(resdata)
                return resdata
                } else {
                    resdata = {
                        status: 400,
                        message: 'Data not found',
                    }
                }
                
            });
        });
    });
        }
} catch (err) {
    console.log(err)
}
});
// app.post("/api/getdividend1", function (req, res) {
//     try{
//     var yer = req.body.fromyear;
//     var secyer = req.body.toyear;
//     yer = yer + "-04-01";
//     secyer = secyer + "-03-31"
//    if(req.body.pan != "" && req.body.name != ""){
//     const pipeline = [  ///trans_cams                                                     
//         { $match: { $and: [{ TRXN_NATUR: /Div/ }, { PAN: req.body.pan }, { INV_NAME: {$regex : `^${req.body.name}.*` , $options: 'i' } },{ TRADDATE: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }] } },
//         { $group: { _id: { SCHEME: "$SCHEME", INV_NAME: "$INV_NAME" }, AMOUNT: { $sum: "$AMOUNT" } } },
//         { $project: { _id: 0, SCHEME: "$_id.SCHEME", INVNAME: "$_id.INV_NAME", AMOUNT: { $sum: "$AMOUNT" } } },
//     ]
//     const pipeline1 = [  ///trans_karvy
//         { $match: { $and: [{ TRDESC: /Div/ }, { PAN1: req.body.pan },{ INVNAME: {$regex : `^${req.body.name}.*` , $options: 'i' } }, { TD_TRDT: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }] } },
//         { $group: { _id: { FUNDDESC: "$FUNDDESC", INVNAME: "$INVNAME" }, TD_AMT: { $sum: "$TD_AMT" } } },
//         { $project: { _id: 0, SCHEME: "$_id.FUNDDESC", INVNAME: "$_id.INVNAME", AMOUNT: { $sum: "$TD_AMT" } } },
//     ]
//     const pipeline2 = [  ///trans_franklin
//         { $match: { $and: [{ TRXN_TYPE: /Div/ }, { IT_PAN_NO1: req.body.pan },{ INVESTOR_2: {$regex : `^${req.body.name}.*` , $options: 'i' } }, { TRXN_DATE: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }] } },
//         { $group: { _id: { SCHEME_NA1: "$SCHEME_NA1", INVESTOR_2: "$INVESTOR_2" }, AMOUNT: { $sum: "$AMOUNT" } } },
//         { $project: { _id: 0, SCHEME: "$_id.SCHEME_NA1", INVNAME: "$_id.INVESTOR_2", AMOUNT: { $sum: "$AMOUNT" } } },

//     ]

//     transc.aggregate(pipeline, (err, newdata) => {
//         transk.aggregate(pipeline1, (err, newdata1) => {
//             transf.aggregate(pipeline2, (err, newdata2) => {
//                 if (newdata != 0 || newdata1 != 0 || newdata2 != 0) {
//                     resdata = {
//                         status: 200,
//                         message: 'Successfull',
//                         data: newdata2
//                     }
//                 } else {
//                     resdata = {
//                         status: 400,
//                         message: 'Data not found',
//                     }
//                 }
//                 var datacon = newdata2.concat(newdata1.concat(newdata))
//                 datacon = datacon.map(JSON.stringify).reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
//                     .filter(function (item, index, arr) { return arr.indexOf(item, index + 1) === -1; }) // check if there is any occurence of the item in whole array
//                     .reverse().map(JSON.parse);
//                 resdata.data = datacon;
//                 res.json(resdata)
//                 return resdata
//             });
//         });
//     })
// }else{
//     resdata = {
//         status: 400,
//         message: 'Data not found',
//     }
// 	 res.json(resdata)
//          return resdata
// }
// } catch (err) {
//     console.log(err)
// }
// });

// app.post("/api/getdividend2", function (req, res) {
//     try{
//         var member="";
//         var arr1=[];var arr2=[];var arr3=[];var alldata=[];var arrFolio=[];var arrName=[];
//         let regex = /^([a-zA-Z]){5}([0-9]){4}([a-zA-Z]){1}?$/;
//         if(req.body.fromyear ===""){
//             resdata = {
//                 status: 400,
//                 message: 'Please enter from year',
//             }
            
//         }else if(req.body.toyear ===""){
//             resdata = {
//                 status: 400,
//                 message: 'Please enter to year',
//             }
//         }else if(req.body.pan ===""){
//             resdata = {
//                 status: 400,
//                 message: 'Please enter pan',
//             }
//         }else if(!regex.test(req.body.pan)) {
//             resdata = {
//                 status: 400,
//                 message: 'Please enter valid pan',
//             }
//         }else{
            
//             var yer = req.body.fromyear;
//             var secyer = req.body.toyear;
//             yer = yer + "-04-01";
//             secyer = secyer + "-03-31"

//             family.find({ adminPan:  {$regex : `^${req.body.pan}.*` , $options: 'i' }  },{_id:0,memberPan:1}, function (err, member) {
//                 if(member!=""){
//                     member  = [...new Set(member.map(({memberPan}) => memberPan.toUpperCase()))];
//                     arr1.push({PAN:req.body.pan.toUpperCase()});
//                     arr2.push({GUARD_PAN:req.body.pan.toUpperCase()});
//                     arr3.push({GUARDPANNO:req.body.pan.toUpperCase()});
//                     for(var j=0;j<member.length;j++){     
//                     arr1.push({PAN:member[j]}); 
//                     arr2.push({GUARD_PAN:member[j]});
//                     arr3.push({GUARDPANNO:member[j]});
//                     }
//                     var strPan2 = {$or:arr2};
//                     var strPan3 = {$or:arr3};

//                     folioc.find(strPan2).distinct("FOLIOCHK", function (err, member1) {
//                       foliok.find(strPan3).distinct("ACNO", function (err, member2) {
//                       var alldata = member1.concat(member2);   
//                             for(var j=0;j<alldata.length;j++){     
//                                 arr1.push({FOLIO_NO:alldata[j]});
//                                 arr2.push({TD_ACNO:alldata[j]});
//                                 arr3.push({FOLIO_NO:alldata[j]});
//                                 }
//                              var strFolio = {$or:arr1};
//                              var strFolio1 = {$or:arr2};
//                              var strFolio2 = {$or:arr3};
                            
//     const pipeline = [  ///trans_cams                                                     
//             { $match: { $and: [strFolio,{ TRXN_NATUR: /Div/ },{ TRADDATE: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }] } },
//             { $group: { _id: { PAN:"$PAN", TAX_STATUS:"$TAX_STATUS",SCHEME: "$SCHEME",FOLIO_NO:"$FOLIO_NO", INV_NAME: "$INV_NAME" }, AMOUNT: { $sum: "$AMOUNT" } } },
//             { $project: { _id: 0,PAN:"$_id.PAN", PER_STATUS:"$_id.TAX_STATUS",SCHEME: "$_id.SCHEME",FOLIO_NO:"$_id.FOLIO_NO", INVNAME: "$_id.INV_NAME", AMOUNT: { $sum: "$AMOUNT" } } },
//             { $lookup: { from: 'folio_cams', localField: 'FOLIO_NO', foreignField: 'FOLIOCHK', as: 'detail' } },
//             { $replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$detail", 0 ] }, "$$ROOT" ] } } } ,
//             { $project: {    detail: 0 ,_id:0,TAX_STATUS:0,FOLIOCHK:0,AC_NO:0,FOLIO_DATE:0,PRODUCT:0,SCH_NAME:0,AMC_CODE:0,BANK_NAME:0,HOLDING_NA:0,IFSC_CODE:0,JNT_NAME1:0,JNT_NAME2:0,JOINT1_PAN:0,NOM2_NAME:0,NOM3_NAME:0,NOM_NAME:0,PRCODE:0,HOLDING_NATURE:0,PAN_NO:0,INV_NAME:0,EMAIL:0} },
//             { $sort: { TRADDATE: -1 } }
//     ]
//     const pipeline1 = [  ///trans_karvy
//             { $match: { $and: [ strFolio1, { TRDESC: /Div/ }, { TD_TRDT: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }] } },
//             { $group: { _id: { PAN1:"$PAN1",STATUS:"$STATUS",FUNDDESC: "$FUNDDESC", TD_ACNO:"$TD_ACNO",INVNAME: "$INVNAME" }, TD_AMT: { $sum: "$TD_AMT" } } },
//             { $project: { _id: 0,PAN:"$_id.PAN1",PER_STATUS:"$_id.STATUS", SCHEME: "$_id.FUNDDESC",FOLIO_NO:"$_id.TD_ACNO", INVNAME: "$_id.INVNAME", AMOUNT: { $sum: "$TD_AMT" } } },
//             { $lookup: { from: 'folio_karvy', localField: 'FOLIO_NO', foreignField: 'ACNO', as: 'detail' } },
//             { $replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$detail", 0 ] }, "$$ROOT" ] } } } ,
//             { $project: { detail: 0 , _id:0,STATUS:0,PRCODE:0,STATUSDESC:0,ACNO:0,BNKACNO:0,BNKACTYPE:0,FUNDDESC:0,NOMINEE:0,MODEOFHOLD:0,JTNAME2:0,FUND:0,EMAIL:0,BNAME:0,PANGNO:0,JTNAME1:0,PAN2:0} },
//             { $sort: { TRADDATE: -1 } }
//     ]
//     const pipeline2 = [  ///trans_franklin
//             { $match: { $and: [strFolio2, { $or: [{ TRXN_TYPE: /DIR/ }, { TRXN_TYPE: /DP/ }] }, { TRXN_DATE: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }] } },
//             { $group: { _id: { IT_PAN_NO1:"$IT_PAN_NO1",SOCIAL_S18:"$SOCIAL_S18",SCHEME_NA1: "$SCHEME_NA1",FOLIO_NO:"$FOLIO_NO", INVESTOR_2: "$INVESTOR_2" }, AMOUNT: { $sum: "$AMOUNT" } } },
//             { $project: { _id: 0,PAN:"$_id.IT_PAN_NO1",PER_STATUS:"$_id.SOCIAL_S18", SCHEME: "$_id.SCHEME_NA1", FOLIO_NO:"$_id.FOLIO_NO",INVNAME: "$_id.INVESTOR_2", AMOUNT: { $sum: "$AMOUNT" } } },
//             { $replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$detail", 0 ] }, "$$ROOT" ] } } } ,
//             { $lookup: { from: 'folio_franklin', localField: 'FOLIO_NO', foreignField: 'FOLIO_NO', as: 'detail' } },
//             { $project: { detail: 0 ,_id:0,TAX_STATUS:0,FOLIO_NO:0,PERSONAL_9:0,ACCNT_NO:0,AC_TYPE:0,ADDRESS1:0,BANK_CODE:0,BANK_NAME:0,COMP_CODE:0,D_BIRTH:0,EMAIL:0,HOLDING_T6:0,F_NAME:0,IFSC_CODE:0,JOINT_NAM1:0,JOINT_NAM2:0,KYC_ID:0,NEFT_CODE:0,NOMINEE1:0,PBANK_NAME:0,PANNO2:0,PANNO1:0,PHONE_RES:0,SOCIAL_ST7:0} },
//             { $sort: { TRADDATE: -1 } }
//     ]

//     transc.aggregate(pipeline, (err, camsdata) => {
//         transk.aggregate(pipeline1, (err, karvydata) => {
//             transf.aggregate(pipeline2, (err, frankdata) => {
//                 if (camsdata != 0 || karvydata != 0 || frankdata != 0) {
//                     resdata = {
//                         status: 200,
//                         message: 'Successfull',
//                         data: frankdata
//                     }
               
//                 var datacon = frankdata.concat(karvydata.concat(camsdata))
//                 datacon = datacon.map(JSON.stringify).reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
//                         .filter(function (item, index, arr) { return arr.indexOf(item, index + 1) === -1; }) // check if there is any occurence of the item in whole array
//                         .reverse().map(JSON.parse);
//                         var newdata1 = datacon.map(item=>{
//                             return [JSON.stringify(item),item]
//                              }); // creates array of array
//                         var maparr1 = new Map(newdata1); // create key value pair from array of array
//                         datacon = [...maparr1.values()];//converting back to array from mapobject 
//                      datacon = datacon.map(function(obj) {
//                        if(obj['GUARDIANN0']){
//                            obj['GUARD_NAME'] = obj['GUARDIANN0']; // Assign new key
//                            obj['GUARD_PAN'] = obj['GUARDPANNO'];
//                             // Delete old key
//                                  delete obj['GUARDIANN0'];
//                                  delete obj['GUARDPANNO'];
//                        }else if((obj['GUARDIANN0']) === ""){
//                                obj['GUARD_NAME'] = obj['GUARDIANN0']; // Assign new key
//                                obj['GUARD_PAN'] = obj['GUARDPANNO'];
//                                delete obj['GUARDIANN0'];
//                                delete obj['GUARDPANNO'];
//                            }
//                        if(obj['GUARDIAN20'] === ""){
//                            obj['GUARD_NAME'] = obj['GUARDIAN20']; // Assign new key
//                             // Delete old key
//                            delete obj['GUARDIAN20'];
//                        }else if((obj['GUARDIAN20']) === ""){
//                            obj['GUARD_NAME'] = obj['GUARDIAN20']; // Assign new key
//                            delete obj['GUARDIAN20'];
//                        }
//                            return obj;
//                        });
//                        for (var i = 0; i < datacon.length; i++) {
//                        if(datacon[i]['PER_STATUS'] === "On Behalf Of Minor" || datacon[i]['PER_STATUS'] === "MINOR" || datacon[i]['PER_STATUS'] === "On Behalf of Minor" )  {
//                         datacon[i]['PER_STATUS'] = "Minor";      
//                     }if (datacon[i]['PER_STATUS'] === "INDIVIDUAL") {
//                          datacon[i]['PER_STATUS'] = "Individual";
//                     }if (datacon[i]['PER_STATUS'] === "HINDU UNDIVIDED FAMI") {
//                         datacon[i]['PER_STATUS'] = "HUF";
//                    }
//                 }
//                 resdata.data = datacon;
//                 res.json(resdata);
//                 return resdata;
//                 } else{
//                     resdata = {
//                         status: 400,
//                         message: 'Data Not Found',
//                     }
//                     res.json(resdata);
//                     return resdata;
//                 }
                
//              });
//          });
//       });
//     });
// });
//     }else{     
//              pipeline = [  ///trans_cams                                                                    
//                 { $match: { $and: [{ TRXN_NATUR: /Div/ },{ PAN: req.body.pan }, { TRADDATE: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }] } },
//                 { $group: { _id: { PAN:"$PAN", TAX_STATUS:"$TAX_STATUS",SCHEME: "$SCHEME",FOLIO_NO:"$FOLIO_NO", INV_NAME: "$INV_NAME" }, AMOUNT: { $sum: "$AMOUNT" } } },
//                 { $project: { _id: 0,PAN:"$_id.PAN", PER_STATUS:"$_id.TAX_STATUS",SCHEME: "$_id.SCHEME",FOLIO_NO:"$_id.FOLIO_NO", INVNAME: "$_id.INV_NAME", AMOUNT: { $sum: "$AMOUNT" } } },
//                 { $lookup: { from: 'folio_cams', localField: 'FOLIO_NO', foreignField: 'FOLIOCHK', as: 'detail' } },
//                 { $replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$detail", 0 ] }, "$$ROOT" ] } } } ,
//                 { $project: {    detail: 0 ,_id:0,TAX_STATUS:0,FOLIOCHK:0,AC_NO:0,FOLIO_DATE:0,PRODUCT:0,SCH_NAME:0,AMC_CODE:0,BANK_NAME:0,HOLDING_NA:0,IFSC_CODE:0,JNT_NAME1:0,JNT_NAME2:0,JOINT1_PAN:0,NOM2_NAME:0,NOM3_NAME:0,NOM_NAME:0,PRCODE:0,HOLDING_NATURE:0,PAN_NO:0,INV_NAME:0,EMAIL:0} },
//                 { $sort: { TRADDATE: -1 } }
//             ]
//              pipeline1 = [  ///trans_karvy
//                 { $match: { $and: [{ TRDESC: /Div/ }, { PAN1: req.body.pan }, { TD_TRDT: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }] } },
//                 { $group: { _id: { PAN1:"$PAN1",STATUS:"$STATUS",FUNDDESC: "$FUNDDESC", TD_ACNO:"$TD_ACNO",INVNAME: "$INVNAME" }, TD_AMT: { $sum: "$TD_AMT" } } },
//                 { $project: { _id: 0,PAN:"$_id.PAN1",PER_STATUS:"$_id.STATUS", SCHEME: "$_id.FUNDDESC",FOLIO_NO:"$_id.TD_ACNO", INVNAME: "$_id.INVNAME", AMOUNT: { $sum: "$TD_AMT" } } },
//                 { $lookup: { from: 'folio_karvy', localField: 'FOLIO_NO', foreignField: 'ACNO', as: 'detail' } },
//                 { $replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$detail", 0 ] }, "$$ROOT" ] } } } ,
//                 { $project: { detail: 0 , _id:0,STATUS:0,PRCODE:0,STATUSDESC:0,ACNO:0,BNKACNO:0,BNKACTYPE:0,FUNDDESC:0,NOMINEE:0,MODEOFHOLD:0,JTNAME2:0,FUND:0,EMAIL:0,BNAME:0,PANGNO:0,JTNAME1:0,PAN2:0} },
//                 { $sort: { TRADDATE: -1 } }
//             ]
//              pipeline2 = [  ///trans_franklin
//                 { $match: { $and: [ { $or: [{ TRXN_TYPE: /DIR/ }, { TRXN_TYPE: /DP/ }] },{ IT_PAN_NO1: req.body.pan }, { TRXN_DATE: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }] } },
//                 { $group: { _id: { IT_PAN_NO1:"$IT_PAN_NO1",SOCIAL_S18:"$SOCIAL_S18",SCHEME_NA1: "$SCHEME_NA1",FOLIO_NO:"$FOLIO_NO", INVESTOR_2: "$INVESTOR_2" }, AMOUNT: { $sum: "$AMOUNT" } } },
//                 { $project: { _id: 0,PAN:"$_id.IT_PAN_NO1",PER_STATUS:"$_id.SOCIAL_S18", SCHEME: "$_id.SCHEME_NA1", FOLIO_NO:"$_id.FOLIO_NO",INVNAME: "$_id.INVESTOR_2", AMOUNT: { $sum: "$AMOUNT" } } },
//                 { $replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$detail", 0 ] }, "$$ROOT" ] } } } ,
//                 { $lookup: { from: 'folio_franklin', localField: 'FOLIO_NO', foreignField: 'FOLIO_NO', as: 'detail' } },
//                 { $project: { detail: 0 ,_id:0,TAX_STATUS:0,FOLIO_NO:0,PERSONAL_9:0,ACCNT_NO:0,AC_TYPE:0,ADDRESS1:0,BANK_CODE:0,BANK_NAME:0,COMP_CODE:0,D_BIRTH:0,EMAIL:0,HOLDING_T6:0,F_NAME:0,IFSC_CODE:0,JOINT_NAM1:0,JOINT_NAM2:0,KYC_ID:0,NEFT_CODE:0,NOMINEE1:0,PBANK_NAME:0,PANNO2:0,PANNO1:0,PHONE_RES:0,SOCIAL_ST7:0} },
//                 { $sort: { TRADDATE: -1 } }
//             ]       
//             transc.aggregate(pipeline, (err, camsdata) => {
//                 transk.aggregate(pipeline1, (err, karvydata) => {
//                     transf.aggregate(pipeline2, (err, frankdata) => {
//                         if (camsdata != 0 || karvydata != 0 || frankdata != 0) {
//                             resdata = {
//                                 status: 200,
//                                 message: 'Successfull',
//                                 data: frankdata
//                             } 
//                         var datacon = frankdata.concat(karvydata.concat(camsdata))
//                         datacon = datacon.map(JSON.stringify).reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
//                             .filter(function (item, index, arr) { return arr.indexOf(item, index + 1) === -1; }) // check if there is any occurence of the item in whole array
//                             .reverse().map(JSON.parse);
//                         resdata.data = datacon;
//                         res.json(resdata);
//                         return resdata;
//                     }else {
//                         resdata = {
//                             status: 400,
//                             message: 'Data not found',
//                         }
//                         res.json(resdata);
//                         return resdata;
//                     }
//                 });
//             });
//         });
//      }
//  });       
//     }
// } catch (err) {
// console.log(err)
// }
// });

app.post("/api/getdividend", function (req, res) {
    try{
        var member="";
        var guardpan1=[];var guardpan2=[];var alldata=[];var arrFolio=[];var arrName=[];
        let regex = /^([a-zA-Z]){5}([0-9]){4}([a-zA-Z]){1}?$/;
        if(req.body.fromyear ===""){
            resdata = {
                status: 400,
                message: 'Please enter from year',
            }
            
        }else if(req.body.toyear ===""){
            resdata = {
                status: 400,
                message: 'Please enter to year',
            }
        }else if(req.body.pan ===""){
            resdata = {
                status: 400,
                message: 'Please enter pan',
            }
        }else if(!regex.test(req.body.pan)) {
            resdata = {
                status: 400,
                message: 'Please enter valid pan',
            }
        }else{
            
            var yer = req.body.fromyear;
            var secyer = req.body.toyear;
            yer = yer + "-04-01";
            secyer = secyer + "-03-31"
            var arr1=[];var arr2=[];var arr3=[];
            family.find({ adminPan:  {$regex : `^${req.body.pan}.*` , $options: 'i' }  },{_id:0,memberPan:1}, function (err, member) {
                if(member!=""){
                    member  = [...new Set(member.map(({memberPan}) => memberPan.toUpperCase()))];
                    guardpan1.push({GUARD_PAN:req.body.pan.toUpperCase()});
                    guardpan2.push({GUARDPANNO:req.body.pan.toUpperCase()});
                    arr1.push({PAN:req.body.pan.toUpperCase()});
                    arr2.push({PAN1:req.body.pan.toUpperCase()});
                    arr3.push({IT_PAN_NO1:req.body.pan.toUpperCase()});
                    for(var j=0;j<member.length;j++){     
                        guardpan1.push({GUARD_PAN:member[j]}); 
                        guardpan2.push({GUARDPANNO:member[j]});
                        arr1.push({PAN:member[j]});
                        arr2.push({PAN1:member[j]});
                        arr3.push({IT_PAN_NO1:member[j]});
                    }
                    var strPan1 = {$or:guardpan1};
                    var strPan2 = {$or:guardpan2};
                    
                    //arr1.push({PAN:arr1});
                  
                    folioc.find(strPan1).distinct("FOLIOCHK", function (err, member1) {
                      foliok.find(strPan2).distinct("ACNO", function (err, member2) {
                      var alldata = member1.concat(member2);   
                     
                     
                            for(var j=0;j<alldata.length;j++){     
                                arr1.push({FOLIO_NO:alldata[j]});
                                arr2.push({TD_ACNO:alldata[j]});
                                arr3.push({FOLIO_NO:alldata[j]});
                                }
                               
                             var strFolio = {$or:arr1};
                             var strFolio1 = {$or:arr2};
                             var strFolio2 = {$or:arr3};
                            
     pipeline = [  ///trans_cams                                                     
            { $match: { $and: [strFolio,{ TRXN_NATUR: /Div/ },{ TRADDATE: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }] } },
            { $group: { _id: { PAN:"$PAN", TAX_STATUS:"$TAX_STATUS",SCHEME: "$SCHEME",FOLIO_NO:"$FOLIO_NO", INV_NAME: "$INV_NAME"  }, AMOUNT: { $sum: "$AMOUNT" } } },
            { $project: { _id: 0,PAN:"$_id.PAN", PER_STATUS:"$_id.TAX_STATUS",SCHEME: "$_id.SCHEME",FOLIO_NO:"$_id.FOLIO_NO", INVNAME: "$_id.INV_NAME", AMOUNT: { $sum: "$AMOUNT"  } } },
            { $lookup: { from: 'folio_cams', localField: 'FOLIO_NO', foreignField: 'FOLIOCHK', as: 'detail' } },
            { $replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$detail", 0 ] }, "$$ROOT" ] } } } ,
            { $project: {    detail: 0 ,_id:0,TAX_STATUS:0,FOLIOCHK:0,AC_NO:0,FOLIO_DATE:0,PRODUCT:0,SCH_NAME:0,AMC_CODE:0,BANK_NAME:0,HOLDING_NA:0,IFSC_CODE:0,JNT_NAME1:0,JNT_NAME2:0,JOINT1_PAN:0,NOM2_NAME:0,NOM3_NAME:0,NOM_NAME:0,PRCODE:0,HOLDING_NATURE:0,PAN_NO:0,INV_NAME:0,EMAIL:0} },
            { $sort: { TRADDATE: -1 } }
    ]
     pipeline1 = [  ///trans_karvy
            { $match: { $and: [ strFolio1, { $or: [{TD_TRTYPE:/DIV/},{TRDESC: /.*Div.*/ } ]}, { TD_TRDT: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }] } },
            { $group: { _id: { PAN1:"$PAN1",STATUS:"$STATUS",FUNDDESC: "$FUNDDESC", TD_ACNO:"$TD_ACNO",INVNAME: "$INVNAME" }, TD_AMT: { $sum: "$TD_AMT" } } },
            { $project: { _id: 0,PAN:"$_id.PAN1",PER_STATUS:"$_id.STATUS", SCHEME: "$_id.FUNDDESC",FOLIO_NO:"$_id.TD_ACNO", INVNAME: "$_id.INVNAME", AMOUNT: { $sum: "$TD_AMT"  } } },
            { $lookup: { from: 'folio_karvy', localField: 'FOLIO_NO', foreignField: 'ACNO', as: 'detail' } },
            { $replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$detail", 0 ] }, "$$ROOT" ] } } } ,
            { $project: { detail: 0 , _id:0,STATUS:0,PRCODE:0,STATUSDESC:0,ACNO:0,BNKACNO:0,BNKACTYPE:0,FUNDDESC:0,NOMINEE:0,MODEOFHOLD:0,JTNAME2:0,FUND:0,EMAIL:0,BNAME:0,PANGNO:0,JTNAME1:0,PAN2:0} },
            { $sort: { TRADDATE: -1 } }
    ]
     pipeline2 = [  ///trans_franklin
            { $match: { $and: [strFolio2, { $or: [{ TRXN_TYPE: /DIR/ }, { TRXN_TYPE: /DP/ }] }, { TRXN_DATE: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }] } },
            { $group: { _id: { IT_PAN_NO1:"$IT_PAN_NO1",SOCIAL_S18:"$SOCIAL_S18",SCHEME_NA1: "$SCHEME_NA1",FOLIO_NO:"$FOLIO_NO", INVESTOR_2: "$INVESTOR_2" }, AMOUNT: { $sum: "$AMOUNT" } } },
            { $project: { _id: 0,PAN:"$_id.IT_PAN_NO1",PER_STATUS:"$_id.SOCIAL_S18", SCHEME: "$_id.SCHEME_NA1", FOLIO_NO:"$_id.FOLIO_NO",INVNAME: "$_id.INVESTOR_2", AMOUNT: { $sum: "$AMOUNT" } } },
            { $replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$detail", 0 ] }, "$$ROOT" ] } } } ,
            { $lookup: { from: 'folio_franklin', localField: 'FOLIO_NO', foreignField: 'FOLIO_NO', as: 'detail' } },
            { $project: { detail: 0 ,_id:0,TAX_STATUS:0,FOLIO_NO:0,PERSONAL_9:0,ACCNT_NO:0,AC_TYPE:0,ADDRESS1:0,BANK_CODE:0,BANK_NAME:0,COMP_CODE:0,D_BIRTH:0,EMAIL:0,HOLDING_T6:0,F_NAME:0,IFSC_CODE:0,JOINT_NAM1:0,JOINT_NAM2:0,KYC_ID:0,NEFT_CODE:0,NOMINEE1:0,PBANK_NAME:0,PANNO2:0,PANNO1:0,PHONE_RES:0,SOCIAL_ST7:0} },
            { $sort: { TRADDATE: -1 } }
    ]

    transc.aggregate(pipeline, (err, camsdata) => {
        transk.aggregate(pipeline1, (err, karvydata) => {
            transf.aggregate(pipeline2, (err, frankdata) => {
                if ( karvydata != 0 ) {
                    resdata = {
                        status: 200,
                        message: 'Successfull',
                        data: frankdata
                    }
               
                var datacon = frankdata.concat(karvydata.concat(camsdata));
                //console.log(datacon)
                datacon = datacon.map(JSON.stringify).reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
                        .filter(function (item, index, arr) { return arr.indexOf(item, index + 1) === -1; }) // check if there is any occurence of the item in whole array
                        .reverse().map(JSON.parse);
                        var newdata1 = datacon.map(item=>{
                            return [JSON.stringify(item),item]
                             }); // creates array of array
                        var maparr1 = new Map(newdata1); // create key value pair from array of array
                        datacon = [...maparr1.values()];//converting back to array from mapobject 
                     datacon = datacon.map(function(obj) {
                       if(obj['GUARDIANN0']){
                           obj['GUARD_NAME'] = obj['GUARDIANN0']; // Assign new key
                           obj['GUARD_PAN'] = obj['GUARDPANNO'];
                            // Delete old key
                                 delete obj['GUARDIANN0'];
                                 delete obj['GUARDPANNO'];
                       }else if((obj['GUARDIANN0']) === ""){
                               obj['GUARD_NAME'] = obj['GUARDIANN0']; // Assign new key
                               obj['GUARD_PAN'] = obj['GUARDPANNO'];
                               delete obj['GUARDIANN0'];
                               delete obj['GUARDPANNO'];
                           }
                       if(obj['GUARDIAN20'] === ""){
                           obj['GUARD_NAME'] = obj['GUARDIAN20']; // Assign new key
                            // Delete old key
                           delete obj['GUARDIAN20'];
                       }else if((obj['GUARDIAN20']) === ""){
                           obj['GUARD_NAME'] = obj['GUARDIAN20']; // Assign new key
                           delete obj['GUARDIAN20'];
                       }
                           return obj;
                       });
                       for (var i = 0; i < datacon.length; i++) {
                       if(datacon[i]['PER_STATUS'] === "On Behalf Of Minor" || datacon[i]['PER_STATUS'] === "MINOR" || datacon[i]['PER_STATUS'] === "On Behalf of Minor" )  {
                        datacon[i]['PER_STATUS'] = "Minor";      
			datacon[i]['PAN'] = "";      
                    }if (datacon[i]['PER_STATUS'] === "INDIVIDUAL") {
                         datacon[i]['PER_STATUS'] = "Individual";
                    }if (datacon[i]['PER_STATUS'] === "HINDU UNDIVIDED FAMI") {
                        datacon[i]['PER_STATUS'] = "HUF";
                   }
                }
                resdata.data = datacon;
                res.json(resdata);
                return resdata;
                } else{
                    resdata = {
                        status: 400,
                        message: 'Data Not Found',
                    }
                    res.json(resdata);
                    return resdata;
                }
                
              });
          });
      });
    });
});
    }else{     
             pipeline = [  ///trans_cams                                                                    
                { $match: { $and: [{ TRXN_NATUR: /Div/ },{ PAN: req.body.pan }, { TRADDATE: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }] } },
                { $group: { _id: { PAN:"$PAN", TAX_STATUS:"$TAX_STATUS",SCHEME: "$SCHEME",FOLIO_NO:"$FOLIO_NO", INV_NAME: "$INV_NAME" }, AMOUNT: { $sum: "$AMOUNT" } } },
                { $project: { _id: 0,PAN:"$_id.PAN", PER_STATUS:"$_id.TAX_STATUS",SCHEME: "$_id.SCHEME",FOLIO_NO:"$_id.FOLIO_NO", INVNAME: "$_id.INV_NAME", AMOUNT: { $sum: "$AMOUNT"  } } },
                { $lookup: { from: 'folio_cams', localField: 'FOLIO_NO', foreignField: 'FOLIOCHK', as: 'detail' } },
                { $replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$detail", 0 ] }, "$$ROOT" ] } } } ,
                { $project: {    detail: 0 ,_id:0,TAX_STATUS:0,FOLIOCHK:0,AC_NO:0,FOLIO_DATE:0,PRODUCT:0,SCH_NAME:0,AMC_CODE:0,BANK_NAME:0,HOLDING_NA:0,IFSC_CODE:0,JNT_NAME1:0,JNT_NAME2:0,JOINT1_PAN:0,NOM2_NAME:0,NOM3_NAME:0,NOM_NAME:0,PRCODE:0,HOLDING_NATURE:0,PAN_NO:0,INV_NAME:0,EMAIL:0} },
                { $sort: { TRADDATE: -1 } }
            ]
             pipeline1 = [  ///trans_karvy
                { $match: { $and: [{ $or: [{TD_TRTYPE:/DIV/},{TRDESC: /Div/ } ]}, { PAN1: req.body.pan }, { TD_TRDT: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }] } },
                { $group: { _id: { PAN1:"$PAN1",STATUS:"$STATUS",FUNDDESC: "$FUNDDESC", TD_ACNO:"$TD_ACNO",INVNAME: "$INVNAME" }, TD_AMT: { $sum: "$TD_AMT" } } },
                { $project: { _id: 0,PAN:"$_id.PAN1",PER_STATUS:"$_id.STATUS", SCHEME: "$_id.FUNDDESC",FOLIO_NO:"$_id.TD_ACNO", INVNAME: "$_id.INVNAME", AMOUNT: { $sum: "$TD_AMT"  } } },
                { $lookup: { from: 'folio_karvy', localField: 'FOLIO_NO', foreignField: 'ACNO', as: 'detail' } },
                { $replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$detail", 0 ] }, "$$ROOT" ] } } } ,
                { $project: { detail: 0 , _id:0,STATUS:0,PRCODE:0,STATUSDESC:0,ACNO:0,BNKACNO:0,BNKACTYPE:0,FUNDDESC:0,NOMINEE:0,MODEOFHOLD:0,JTNAME2:0,FUND:0,EMAIL:0,BNAME:0,PANGNO:0,JTNAME1:0,PAN2:0} },
                { $sort: { TRADDATE: -1 } }
            ]
             pipeline2 = [  ///trans_franklin
                { $match: { $and: [ { $or: [{ TRXN_TYPE: /DIR/ }, { TRXN_TYPE: /DP/ }] },{ IT_PAN_NO1: req.body.pan }, { TRXN_DATE: { $gte: new Date(moment(yer).format("YYYY-MM-DD")), $lt: new Date(moment(secyer).format("YYYY-MM-DD")) } }] } },
                { $group: { _id: { IT_PAN_NO1:"$IT_PAN_NO1",SOCIAL_S18:"$SOCIAL_S18",SCHEME_NA1: "$SCHEME_NA1",FOLIO_NO:"$FOLIO_NO", INVESTOR_2: "$INVESTOR_2" }, AMOUNT: { $sum: "$AMOUNT" } } },
                { $project: { _id: 0,PAN:"$_id.IT_PAN_NO1",PER_STATUS:"$_id.SOCIAL_S18", SCHEME: "$_id.SCHEME_NA1", FOLIO_NO:"$_id.FOLIO_NO",INVNAME: "$_id.INVESTOR_2", AMOUNT: { $sum: "$AMOUNT"  } } },
                { $replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$detail", 0 ] }, "$$ROOT" ] } } } ,
                { $lookup: { from: 'folio_franklin', localField: 'FOLIO_NO', foreignField: 'FOLIO_NO', as: 'detail' } },
                { $project: { detail: 0 ,_id:0,TAX_STATUS:0,FOLIO_NO:0,PERSONAL_9:0,ACCNT_NO:0,AC_TYPE:0,ADDRESS1:0,BANK_CODE:0,BANK_NAME:0,COMP_CODE:0,D_BIRTH:0,EMAIL:0,HOLDING_T6:0,F_NAME:0,IFSC_CODE:0,JOINT_NAM1:0,JOINT_NAM2:0,KYC_ID:0,NEFT_CODE:0,NOMINEE1:0,PBANK_NAME:0,PANNO2:0,PANNO1:0,PHONE_RES:0,SOCIAL_ST7:0} },
                { $sort: { TRADDATE: -1 } }
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
                        var datacon = frankdata.concat(karvydata.concat(camsdata))
                        datacon = datacon.map(JSON.stringify).reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
                            .filter(function (item, index, arr) { return arr.indexOf(item, index + 1) === -1; }) // check if there is any occurence of the item in whole array
                            .reverse().map(JSON.parse);
                        resdata.data = datacon;
                        res.json(resdata);
                        return resdata;
                    }else {
                        resdata = {
                            status: 400,
                            message: 'Data not found',
                        }
                        res.json(resdata);
                        return resdata;
                    }
                });
            });
        });
     }
 });       
    }
} catch (err) {
console.log(err)
}
});


// app.post("/api/gettransactionuserwise1", function (req, res) {
//     try{
//     var mon = parseInt(req.body.month);
//     var yer = parseInt(req.body.year);
//     if(req.body.pan != "" && req.body.name != ""){
//         const pipeline = [  ///trans_cams
//             { $group: { _id: { INV_NAME: "$INV_NAME", PAN: "$PAN", TRXN_NATUR: "$TRXN_NATUR", FOLIO_NO: "$FOLIO_NO", SCHEME: "$SCHEME", AMOUNT: "$AMOUNT", TRADDATE: "$TRADDATE" } } },
//             { $project: { _id: 0, INVNAME: "$_id.INV_NAME", PAN: "$_id.PAN", TRXN_NATUR: "$_id.TRXN_NATUR", FOLIO_NO: "$_id.FOLIO_NO", SCHEME: "$_id.SCHEME", AMOUNT: "$_id.AMOUNT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRADDATE" } }, month: { $month: ('$_id.TRADDATE') }, year: { $year: ('$_id.TRADDATE') } } },
//             { $match: { $and: [{ month: mon }, { year: yer }, { PAN: req.body.pan } , { INVNAME: {$regex : `^${req.body.name}.*` , $options: 'i' } } ] } },
//             { $sort: { TRADDATE: -1 } }
//         ]
//         const pipeline1 = [  ///trans_karvy
//             { $group: { _id: { INVNAME: "$INVNAME", PAN1: "$PAN1", TRDESC: "$TRDESC", TD_ACNO: "$TD_ACNO", FUNDDESC: "$FUNDDESC", TD_AMT: "$TD_AMT", TD_TRDT: "$TD_TRDT" } } },
//             { $project: { _id: 0, INVNAME: "$_id.INVNAME", PAN: "$_id.PAN1", TRXN_NATUR: "$_id.TRDESC", FOLIO_NO: "$_id.TD_ACNO", SCHEME: "$_id.FUNDDESC", AMOUNT: "$_id.TD_AMT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TD_TRDT" } }, month: { $month: ('$_id.TD_TRDT') }, year: { $year: ('$_id.TD_TRDT') } } },
//             { $match: { $and: [{ month: mon }, { year: yer }, { PAN: req.body.pan }, { INVNAME: {$regex : `^${req.body.name}.*` , $options: 'i' } } ] } },
//             { $sort: { TRADDATE: -1 } }
//         ]
//         const pipeline2 = [  ///trans_franklin
//             { $group: { _id: { INVESTOR_2: "$INVESTOR_2", IT_PAN_NO1: "$IT_PAN_NO1", TRXN_TYPE: "$TRXN_TYPE", FOLIO_NO: "$FOLIO_NO", SCHEME_NA1: "$SCHEME_NA1", AMOUNT: "$AMOUNT", TRXN_DATE: "$TRXN_DATE" } } },
//             { $project: { _id: 0, INVNAME: "$INVESTOR_2", PAN: "$_id.IT_PAN_NO1", TRXN_NATUR: "$_id.TRXN_TYPE", FOLIO_NO: "$_id.FOLIO_NO", SCHEME: "$_id.SCHEME_NA1", AMOUNT: "$_id.AMOUNT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRXN_DATE" } }, month: { $month: ('$_id.TRXN_DATE') }, year: { $year: ('$_id.TRXN_DATE') } } },
//             { $match: { $and: [{ month: mon }, { year: yer }, { PAN: req.body.pan }, { INVNAME: {$regex : `^${req.body.name}.*` , $options: 'i' } } ] } },
//             { $sort: { TRADDATE: -1 } }
//         ]

//         transc.aggregate(pipeline, (err, camsdata) => {
//             transk.aggregate(pipeline1, (err, karvydata) => {
//                 transf.aggregate(pipeline2, (err, frankdata) => {
//                     if (frankdata.length != 0 || karvydata.length != 0 || camsdata.length != 0) {
//                         //   if( newdata != 0){
//                         resdata = {
//                             status: 200,
//                             message: 'Successfull',
//                             data: frankdata
//                         }
//                     } else {
//                         resdata = {
//                             status: 400,
//                             message: 'Data not found',
//                         }
//                     }
//                     var datacon = frankdata.concat(karvydata.concat(camsdata))
//                     datacon = datacon.map(JSON.stringify).reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
//                         .filter(function (item, index, arr) { return arr.indexOf(item, index + 1) === -1; }) // check if there is any occurence of the item in whole array
//                         .reverse().map(JSON.parse);
//                     for (var i = 0; i < datacon.length; i++) {
//                         if (datacon[i]['TRXN_NATUR'] === "Redemption") {
//                             datacon[i]['TRXN_NATUR'] = "RED";
//                         } if (datacon[i]['TRXN_NATUR'].match(/Systematic Investment.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic Withdrawal.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic - Instalment.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic - To.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic-NSE.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic Physical.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic-Normal.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic (ECS).*/)) {
//                             datacon[i]['TRXN_NATUR'] = "SIP";
//                         } if (Math.sign(datacon[i]['AMOUNT']) === -1) {
//                             datacon[i]['TRXN_NATUR'] = "SIPR";
//                         } if (datacon[i]['TRXN_NATUR'].match(/Systematic - From.*/)) {
//                             datacon[i]['TRXN_NATUR'] = "STP";
//                         }
//                     }
//                     resdata.data = datacon.sort((a, b) => new Date(b.TRADDATE.split("-").reverse().join("/")).getTime() - new Date(a.TRADDATE.split("-").reverse().join("/")).getTime())
//                     res.json(resdata)
//                     return resdata
//                 });
//             });
//         });
//     }else{
//         resdata = {
//             status: 400,
//             message: 'Data not found',
//         }
// 	     res.json(resdata)
//              return resdata
//     }
// } catch (err) {
//     console.log(err)
// }
// })

// app.post("/api/gettransactionuserwise2", function (req, res) {
//     try{
//         var member="";
//         var arr=[];var alldata=[];
//         let regex = /^([a-zA-Z]){5}([0-9]){4}([a-zA-Z]){1}?$/;
//         if(req.body.month ===""){
//             resdata = {
//                 status: 400,
//                 message: 'Please enter month',
//             }
            
//         }else if(req.body.year ===""){
//             resdata = {
//                 status: 400,
//                 message: 'Please enter year',
//             }
//         }else if(req.body.pan ===""){
//             resdata = {
//                 status: 400,
//                 message: 'Please enter pan',
//             }
//         }else if(!regex.test(req.body.pan)) {
//             resdata = {
//                 status: 400,
//                 message: 'Please enter valid pan',
//             }
//         }else{
//             var mon = parseInt(req.body.month);
//             var yer = parseInt(req.body.year);
//              family.find({ adminPan:  {$regex : `^${req.body.pan}.*` , $options: 'i' }  },{_id:0,memberPan:1}, function (err, member) {
//                 if(member!=""){
//                     member  = [...new Set(member.map(({memberPan}) => memberPan.toUpperCase()))];
//                     arr.push({PAN:req.body.pan.toUpperCase()});
//                     for(var j=0;j<member.length;j++){     
//                     arr.push({PAN:member[j]}); 
//                     }
//                     var str = {$or:arr};
//          pipeline = [  ///trans_cams
//             { $group: { _id: { TRXNNO:"$TRXNNO",INV_NAME: "$INV_NAME", PAN: "$PAN", TRXN_NATUR: "$TRXN_NATUR", FOLIO_NO: "$FOLIO_NO", SCHEME: "$SCHEME", AMOUNT: "$AMOUNT", TRADDATE: "$TRADDATE" } } },
//             { $project: { _id: 0,TRXNNO:"$_id.TRXNNO", INVNAME: "$_id.INV_NAME", PAN: "$_id.PAN", TRXN_NATUR: "$_id.TRXN_NATUR", FOLIO_NO: "$_id.FOLIO_NO", SCHEME: "$_id.SCHEME", AMOUNT: "$_id.AMOUNT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRADDATE" } }, month: { $month: ('$_id.TRADDATE') }, year: { $year: ('$_id.TRADDATE') } } },
//             { $match: { $and: [{ month: mon }, { year: yer }, str  ] } },
//             { $sort: { TRADDATE: -1 } }
//         ]
//          pipeline1 = [  ///trans_karvy
//             { $group: { _id: { TD_TRNO:"$TD_TRNO", INVNAME: "$INVNAME", PAN1: "$PAN1", TRDESC: "$TRDESC", TD_ACNO: "$TD_ACNO", FUNDDESC: "$FUNDDESC", TD_AMT: "$TD_AMT", TD_TRDT: "$TD_TRDT" } } },
//             { $project: { _id: 0, TD_TRNO:"$_id.TD_TRNO",INVNAME: "$_id.INVNAME", PAN: "$_id.PAN1", TRXN_NATUR: "$_id.TRDESC", FOLIO_NO: "$_id.TD_ACNO", SCHEME: "$_id.FUNDDESC", AMOUNT: "$_id.TD_AMT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TD_TRDT" } }, month: { $month: ('$_id.TD_TRDT') }, year: { $year: ('$_id.TD_TRDT') } } },
//             { $match: { $and: [{ month: mon }, { year: yer }, str ] } },
//             { $sort: { TRADDATE: -1 } }
//         ]
//          pipeline2 = [  ///trans_franklin
//             { $group: { _id: { TRXN_NO:"$TRXN_NO", INVESTOR_2: "$INVESTOR_2", IT_PAN_NO1: "$IT_PAN_NO1", TRXN_TYPE: "$TRXN_TYPE", FOLIO_NO: "$FOLIO_NO", SCHEME_NA1: "$SCHEME_NA1", AMOUNT: "$AMOUNT", TRXN_DATE: "$TRXN_DATE" } } },
//             { $project: { _id: 0, TRXN_NO:"$_id.TRXN_NO", INVNAME: "$_id.INVESTOR_2", PAN: "$_id.IT_PAN_NO1", TRXN_NATUR: "$_id.TRXN_TYPE", FOLIO_NO: "$_id.FOLIO_NO", SCHEME: "$_id.SCHEME_NA1", AMOUNT: "$_id.AMOUNT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRXN_DATE" } }, month: { $month: ('$_id.TRXN_DATE') }, year: { $year: ('$_id.TRXN_DATE') } } },
//             { $match: { $and: [{ month: mon }, { year: yer }, str ] } },
//             { $sort: { TRADDATE: -1 } }
//         ]
//         transc.aggregate(pipeline, (err, camsdata) => {
//             transk.aggregate(pipeline1, (err, karvydata) => {
//                 transf.aggregate(pipeline2, (err, frankdata) => {
//                     if (frankdata != 0 || karvydata != 0 || camsdata != 0) {
//                         resdata = {
//                             status: 200,
//                             message: 'Successfull',
//                             data: frankdata
//                         }
//                         var datacon = frankdata.concat(karvydata.concat(camsdata));
//                         datacon = datacon.map(JSON.stringify).reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
//                             .filter(function (item, index, arr) { return arr.indexOf(item, index + 1) === -1; }) // check if there is any occurence of the item in whole array
//                             .reverse().map(JSON.parse);
//                         for (var i = 0; i < datacon.length; i++) {
//                             if (datacon[i]['TRXN_NATUR'] === "Redemption" || datacon[i]['TRXN_NATUR'] === "FUL" || datacon[i]['TRXN_NATUR'] === "SIPR" || 
//                         datacon[i]['TRXN_NATUR'] === "Full Redemption" || datacon[i]['TRXN_NATUR'] === "Partial Redemption") {
//                             datacon[i]['TRXN_NATUR'] = "RED";
//                         }  if (datacon[i]['TRXN_NATUR'].match(/Systematic Investment.*/) || 
//                          datacon[i]['TRXN_NATUR'].match(/Systematic - Instalment.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic - To.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic-NSE.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic Physical.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic-Normal.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic (ECS).*/)) {
//                             datacon[i]['TRXN_NATUR'] = "SIP";
//                         } if (datacon[i]['TRXN_NATUR'] === "Systematic Withdrawal") {
//                             datacon[i]['TRXN_NATUR'] = "SWP";
//                         }if (Math.sign(datacon[i]['AMOUNT']) === -1) {
//                             datacon[i]['TRXN_NATUR'] = "SIPR";
//                         } if (datacon[i]['TRXN_NATUR'].match(/Systematic - From.*/) || datacon[i]['TRXN_NATUR'] === "S T P" || datacon[i]['TRXN_NATUR'] === "S T P In") {
//                             datacon[i]['TRXN_NATUR'] = "STP";
//                         }if (datacon[i]['TRXN_NATUR'] === "Lateral Shift Out" || datacon[i]['TRXN_NATUR'] === "Switchout"
//                          || datacon[i]['TRXN_NATUR'] === "Transfer-Out" || datacon[i]['TRXN_NATUR'] === "Transmission Out"
//                           || datacon[i]['TRXN_NATUR'] === "Switch Over Out" || datacon[i]['TRXN_NATUR'] === "LTOP"
//                           || datacon[i]['TRXN_NATUR'] === "LTOF" || datacon[i]['TRXN_NATUR'] === "Partial Switch Out" || 
//                           datacon[i]['TRXN_NATUR'] === "Full Switch Out") {
//                             datacon[i]['TRXN_NATUR'] = "Switch Out";
//                         }if (datacon[i]['TRXN_NATUR'] === "Lateral Shift In" || datacon[i]['TRXN_NATUR'] === "Switch-In" 
//                         || datacon[i]['TRXN_NATUR'] === "Transfer-In" || datacon[i]['TRXN_NATUR'] === "Switch Over In" 
//                         || datacon[i]['TRXN_NATUR'] === "LTIN" || datacon[i]['TRXN_NATUR'] === "LTIA") {
//                             datacon[i]['TRXN_NATUR'] = "Switch In";
//                         }if (datacon[i]['TRXN_NATUR'] === "Dividend Reinvest" || 
//                         datacon[i]['TRXN_NATUR'] === "Dividend Paid"
//                          || datacon[i]['TRXN_NATUR'] === "Div. Reinvestment") {
//                             datacon[i]['TRXN_NATUR'] = "Dividend";
//                         }if (datacon[i]['TRXN_NATUR'] === "Gross Dividend") {
//                             datacon[i]['TRXN_NATUR'] = "Dividend Payout";
//                         }if (datacon[i]['TRXN_NATUR'] === "Consolidation In") {
//                             datacon[i]['TRXN_NATUR'] = "Con In";
//                         }if (datacon[i]['TRXN_NATUR'] === "Consolidation Out") {
//                             datacon[i]['TRXN_NATUR'] = "Con Out";
//                         }if (datacon[i]['TRXN_NATUR'] === "Consolidation Out") {
//                             datacon[i]['TRXN_NATUR'] = "Con Out";
//                         }if (datacon[i]['TRXN_NATUR'] === "Purchase" || datacon[i]['TRXN_NATUR'] === "NEW" || 
//                         datacon[i]['TRXN_NATUR'] === "Initial Allotment"
//                         || datacon[i]['TRXN_NATUR'] === "NEWPUR") {
//                             datacon[i]['TRXN_NATUR'] = "Purchase";
//                         }if(datacon[i]['TRXN_NATUR'] === "Additional Purchase" || datacon[i]['TRXN_NATUR'] === "ADD" ||
//                          datacon[i]['TRXN_NATUR'] === "ADDPUR") {
//                             datacon[i]['TRXN_NATUR'] = "Add. Purchase";
//                         }
//                      }
//                      resdata.data = datacon.sort((a, b) => new Date(b.TRADDATE.split("-").reverse().join("/")).getTime() - new Date(a.TRADDATE.split("-").reverse().join("/")).getTime());
//                      res.json(resdata);
//                     return resdata;
//                     } 
                    
//                  });
//              });
//           });
//         }else{
//             resdata = {
//                 status: 400,
//                 message: 'Data not found',
//             }
//            }       
//         });
//     }
// } catch (err) {
//     console.log(err)
// }
// })

// app.post("/api/gettransactionuserwise", function (req, res) {
//     try{
//         var member="";var guardpan1=[];var guardpan2=[];
//         var arr1=[];var arr2=[];var arr3=[];var alldata=[];var arrFolio=[];var arrName=[];
//         let regex = /^([a-zA-Z]){5}([0-9]){4}([a-zA-Z]){1}?$/;
//         if(req.body.month ===""){
//             resdata = {
//                 status: 400,
//                 message: 'Please enter month',
//             }
//             res.json(resdata);
//             return resdata;
//         }else if(req.body.year ===""){
//             resdata = {
//                 status: 400,
//                 message: 'Please enter year',
//             }
//             res.json(resdata);
//             return resdata;
//         }else if(req.body.pan ===""){
//             resdata = {
//                 status: 400,
//                 message: 'Please enter pan',
//             }
//             res.json(resdata);
//             return resdata;
//         }else if(!regex.test(req.body.pan)) {
//             resdata = {
//                 status: 400,
//                 message: 'Please enter valid pan',
//             }
//             res.json(resdata);
//             return resdata;
//         }else{
//             var mon = parseInt(req.body.month);
//             var yer = parseInt(req.body.year);
//              family.find({ adminPan:  {$regex : `^${req.body.pan}.*` , $options: 'i' }  },{_id:0,memberPan:1}, function (err, member) {
//                 if(member!=""){
//                     member  = [...new Set(member.map(({memberPan}) => memberPan.toUpperCase()))];
//                     guardpan1.push({GUARD_PAN:req.body.pan.toUpperCase()});
//                     guardpan2.push({GUARDPANNO:req.body.pan.toUpperCase()});
//                     arr1.push({PAN:req.body.pan.toUpperCase()});
//                     // arr2.push({PAN1:req.body.pan.toUpperCase()});
//                     // arr3.push({IT_PAN_NO1:req.body.pan.toUpperCase()});
//                     for(var j=0;j<member.length;j++){     
//                     guardpan1.push({GUARD_PAN:member[j]}); 
//                     guardpan2.push({GUARDPANNO:member[j]});
//                     arr1.push({PAN:member[j]});
//                     // arr2.push({PAN1:member[j]});
//                     // arr3.push({IT_PAN_NO1:member[j]});
//                     }
//                     var strPan1 = {$or:guardpan1};
//                     var strPan2 = {$or:guardpan2};
//                     folioc.find(strPan1).distinct("FOLIOCHK", function (err, member1) {
//                       foliok.find(strPan2).distinct("ACNO", function (err, member2) {
//                       var alldata = member1.concat(member2);   
//                       for(var j=0;j<alldata.length;j++){     
//                         arr1.push({FOLIO_NO:alldata[j]});
//                         // arr2.push({TD_ACNO:alldata[j]});
//                         // arr3.push({FOLIO_NO:alldata[j]});
//                         }
//                         var strFolio = {$or:arr1};
//                         // var strFolio1 = {$or:arr2};
//                         // var strFolio2 = {$or:arr3};
//         pipeline = [  ///trans_cams
//             { $group: { _id: { TAX_STATUS:"$TAX_STATUS",TRXNNO:"$TRXNNO",INV_NAME: "$INV_NAME", PAN: "$PAN", TRXN_NATUR: "$TRXN_NATUR", FOLIO_NO: "$FOLIO_NO", SCHEME: "$SCHEME", AMOUNT: "$AMOUNT", TRADDATE: "$TRADDATE" } } },
//             { $project: { _id: 0,PER_STATUS:"$_id.TAX_STATUS",TRXNNO:"$_id.TRXNNO", INVNAME: "$_id.INV_NAME", PAN: "$_id.PAN", TRXN_NATUR: "$_id.TRXN_NATUR", FOLIO_NO: "$_id.FOLIO_NO", SCHEME: "$_id.SCHEME", AMOUNT: "$_id.AMOUNT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRADDATE" } }, month: { $month: ('$_id.TRADDATE') }, year: { $year: ('$_id.TRADDATE') } } },
//             { $match: { $and: [{ month: mon }, { year: yer } ,strFolio  ] } },
//             { $lookup: { from: 'folio_cams', localField: 'FOLIO_NO', foreignField: 'FOLIOCHK', as: 'detail' } },
//             { $replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$detail", 0 ] }, "$$ROOT" ] } } } ,
//             { $project: {    detail: 0 ,_id:0,TAX_STATUS:0,FOLIOCHK:0,AC_NO:0,FOLIO_DATE:0,PRODUCT:0,SCH_NAME:0,AMC_CODE:0,BANK_NAME:0,HOLDING_NA:0,IFSC_CODE:0,JNT_NAME1:0,JNT_NAME2:0,JOINT1_PAN:0,NOM2_NAME:0,NOM3_NAME:0,NOM_NAME:0,PRCODE:0,HOLDING_NATURE:0,PAN_NO:0,INV_NAME:0,EMAIL:0} },
//             { $sort: { TRADDATE: -1 } }
//         ]
//          pipeline1 = [  ///trans_karvy
//             { $group: { _id: { STATUS:"$STATUS",TD_TRNO:"$TD_TRNO", INVNAME: "$INVNAME", PAN1: "$PAN1", TRDESC: "$TRDESC", TD_ACNO: "$TD_ACNO", FUNDDESC: "$FUNDDESC", TD_AMT: "$TD_AMT", TD_TRDT: "$TD_TRDT" } } },
//             { $project: { _id: 0,PER_STATUS:"$_id.STATUS", TRXNNO:"$_id.TD_TRNO",INVNAME: "$_id.INVNAME", PAN: "$_id.PAN1", TRXN_NATUR: "$_id.TRDESC", FOLIO_NO: "$_id.TD_ACNO", SCHEME: "$_id.FUNDDESC", AMOUNT: "$_id.TD_AMT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TD_TRDT" } }, month: { $month: ('$_id.TD_TRDT') }, year: { $year: ('$_id.TD_TRDT') } } },
//             { $match: { $and: [{ month: mon }, { year: yer }  ,strFolio ] } },
//             { $lookup: { from: 'folio_karvy', localField: 'FOLIO_NO', foreignField: 'ACNO', as: 'detail' } },
//             { $replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$detail", 0 ] }, "$$ROOT" ] } } } ,
//             { $project: { detail: 0 , _id:0,STATUS:0,PRCODE:0,STATUSDESC:0,ACNO:0,BNKACNO:0,BNKACTYPE:0,FUNDDESC:0,NOMINEE:0,MODEOFHOLD:0,JTNAME2:0,FUND:0,EMAIL:0,BNAME:0,PANGNO:0,JTNAME1:0,PAN2:0} },
//             { $sort: { TRADDATE: -1 } }
//         ]
//          pipeline2 = [  ///trans_franklin
//             { $group: { _id: { SOCIAL_S18:"$SOCIAL_S18",TRXN_NO:"$TRXN_NO", INVESTOR_2: "$INVESTOR_2", IT_PAN_NO1: "$IT_PAN_NO1", TRXN_TYPE: "$TRXN_TYPE", FOLIO_NO: "$FOLIO_NO", SCHEME_NA1: "$SCHEME_NA1", AMOUNT: "$AMOUNT", TRXN_DATE: "$TRXN_DATE" } } },
//             { $project: { _id: 0, PER_STATUS:"$_id.SOCIAL_S18",TRXNNO:"$_id.TRXN_NO", INVNAME: "$_id.INVESTOR_2", PAN: "$_id.IT_PAN_NO1", TRXN_NATUR: "$_id.TRXN_TYPE", FOLIO_NO: "$_id.FOLIO_NO", SCHEME: "$_id.SCHEME_NA1", AMOUNT: "$_id.AMOUNT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRXN_DATE" } }, month: { $month: ('$_id.TRXN_DATE') }, year: { $year: ('$_id.TRXN_DATE') } } },
//             { $match: { $and: [{ month: mon }, { year: yer }  ,strFolio] } },
//             { $lookup: { from: 'folio_franklin', localField: 'FOLIO_NO', foreignField: 'FOLIO_NO', as: 'detail' } },
//             { $replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$detail", 0 ] }, "$$ROOT" ] } } } ,
//             { $project: { detail: 0 ,_id:0,TAX_STATUS:0,PERSONAL_9:0,ACCNT_NO:0,AC_TYPE:0,ADDRESS1:0,BANK_CODE:0,BANK_NAME:0,COMP_CODE:0,D_BIRTH:0,EMAIL:0,HOLDING_T6:0,F_NAME:0,IFSC_CODE:0,JOINT_NAM1:0,JOINT_NAM2:0,KYC_ID:0,NEFT_CODE:0,NOMINEE1:0,PBANK_NAME:0,PANNO2:0,PANNO1:0,PHONE_RES:0,SOCIAL_ST7:0} },
//             { $sort: { TRADDATE: -1 } }
//         ]
//         transc.aggregate(pipeline, (err, camsdata) => {
//             transk.aggregate(pipeline1, (err, karvydata) => {
//                 transf.aggregate(pipeline2, (err, frankdata) => {
//                     if (frankdata != 0 || karvydata != 0 || camsdata != 0) {
//                         resdata = {
//                             status: 200,
//                             message: 'Successfull',
//                             data: frankdata
//                         }
//                         var datacon = frankdata.concat(karvydata.concat(camsdata));
//                         datacon = datacon.map(JSON.stringify).reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
//                             .filter(function (item, index, arr) { return arr.indexOf(item, index + 1) === -1; }) // check if there is any occurence of the item in whole array
//                             .reverse().map(JSON.parse);
//                             var newdata1 = datacon.map(item=>{
//                                 return [JSON.stringify(item),item]
//                                  }); // creates array of array
//                                  var maparr1 = new Map(newdata1); // create key value pair from array of array
//                                  datacon = [...maparr1.values()];//converting back to array from mapobject 

//                               datacon = datacon.map(function(obj) {
//                                 if(obj['GUARDIANN0']){
//                                     obj['GUARD_NAME'] = obj['GUARDIANN0']; // Assign new key
//                                     obj['GUARD_PAN'] = obj['GUARDPANNO'];
//                                      // Delete old key
//                                           delete obj['GUARDIANN0'];
//                                           delete obj['GUARDPANNO'];
//                                 }else if((obj['GUARDIANN0']) === ""){
//                                         obj['GUARD_NAME'] = obj['GUARDIANN0']; // Assign new key
//                                         obj['GUARD_PAN'] = obj['GUARDPANNO'];
//                                         delete obj['GUARDIANN0'];
//                                         delete obj['GUARDPANNO'];
//                                     }
//                                 if(obj['GUARDIAN20']){
//                                     obj['GUARD_NAME'] = obj['GUARDIAN20']; // Assign new key
//                                      // Delete old key
//                                     delete obj['GUARDIAN20'];
//                                 }else if((obj['GUARDIAN20']) === ""){
//                                     obj['GUARD_NAME'] = obj['GUARDIAN20']; // Assign new key
//                                     delete obj['GUARDIAN20'];
//                                 }
//                                     return obj;
                                
//                                 });

//                         for (var i = 0; i < datacon.length; i++) {
//                             if (datacon[i]['TRXN_NATUR'] === "Redemption" || datacon[i]['TRXN_NATUR'] === "FUL" || datacon[i]['TRXN_NATUR'] === "SIPR" || 
//                         datacon[i]['TRXN_NATUR'] === "Full Redemption" || datacon[i]['TRXN_NATUR'] === "Partial Redemption") {
//                             datacon[i]['TRXN_NATUR'] = "RED";
//                         }  if (datacon[i]['TRXN_NATUR'].match(/Systematic Investment.*/) || 
//                          datacon[i]['TRXN_NATUR'].match(/Systematic - Instalment.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic - To.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic-NSE.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic Physical.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic-Normal.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic (ECS).*/)) {
//                             datacon[i]['TRXN_NATUR'] = "SIP";
//                         } if (datacon[i]['TRXN_NATUR'] === "Systematic Withdrawal") {
//                             datacon[i]['TRXN_NATUR'] = "SWP";
//                         }if (Math.sign(datacon[i]['AMOUNT']) === -1) {
//                             datacon[i]['TRXN_NATUR'] = "SIPR";
//                         } if (datacon[i]['TRXN_NATUR'].match(/Systematic - From.*/) || datacon[i]['TRXN_NATUR'] === "S T P" || datacon[i]['TRXN_NATUR'] === "S T P In") {
//                             datacon[i]['TRXN_NATUR'] = "STP";
//                         }if (datacon[i]['TRXN_NATUR'] === "Lateral Shift Out" || datacon[i]['TRXN_NATUR'] === "Switchout"
//                          || datacon[i]['TRXN_NATUR'] === "Transfer-Out" || datacon[i]['TRXN_NATUR'] === "Transmission Out"
//                           || datacon[i]['TRXN_NATUR'] === "Switch Over Out" || datacon[i]['TRXN_NATUR'] === "LTOP"
//                           || datacon[i]['TRXN_NATUR'] === "LTOF" || datacon[i]['TRXN_NATUR'] === "Partial Switch Out" || 
//                           datacon[i]['TRXN_NATUR'] === "Full Switch Out") {
//                             datacon[i]['TRXN_NATUR'] = "Switch Out";
//                         }if (datacon[i]['TRXN_NATUR'] === "Lateral Shift In" || datacon[i]['TRXN_NATUR'] === "Switch-In" 
//                         || datacon[i]['TRXN_NATUR'] === "Transfer-In" || datacon[i]['TRXN_NATUR'] === "Switch Over In" 
//                         || datacon[i]['TRXN_NATUR'] === "LTIN" || datacon[i]['TRXN_NATUR'] === "LTIA") {
//                             datacon[i]['TRXN_NATUR'] = "Switch In";
//                         }if (datacon[i]['TRXN_NATUR'] === "Dividend Reinvest" || 
//                         datacon[i]['TRXN_NATUR'] === "Dividend Paid"
//                          || datacon[i]['TRXN_NATUR'] === "Div. Reinvestment") {
//                             datacon[i]['TRXN_NATUR'] = "Dividend";
//                         }if (datacon[i]['TRXN_NATUR'] === "Gross Dividend") {
//                             datacon[i]['TRXN_NATUR'] = "Dividend Payout";
//                         }if (datacon[i]['TRXN_NATUR'] === "Consolidation In") {
//                             datacon[i]['TRXN_NATUR'] = "Con In";
//                         }if (datacon[i]['TRXN_NATUR'] === "Consolidation Out") {
//                             datacon[i]['TRXN_NATUR'] = "Con Out";
//                         }if (datacon[i]['TRXN_NATUR'] === "Consolidation Out") {
//                             datacon[i]['TRXN_NATUR'] = "Con Out";
//                         }if (datacon[i]['TRXN_NATUR'] === "Purchase" || datacon[i]['TRXN_NATUR'] === "NEW" || 
//                             datacon[i]['TRXN_NATUR'] === "Initial Allotment"
//                         || datacon[i]['TRXN_NATUR'] === "NEWPUR") {
//                             datacon[i]['TRXN_NATUR'] = "Purchase";
//                         }if(datacon[i]['TRXN_NATUR'] === "Additional Purchase" || datacon[i]['TRXN_NATUR'] === "ADD" ||
//                          datacon[i]['TRXN_NATUR'] === "ADDPUR") {
//                             datacon[i]['TRXN_NATUR'] = "Add. Purchase";
//                         }if (datacon[i]['PER_STATUS'] === "On Behalf Of Minor" || datacon[i]['PER_STATUS'] === "MINOR" || datacon[i]['PER_STATUS'] === "On Behalf of Minor" )  {
//                             datacon[i]['PER_STATUS'] = "Minor";     
// 		            datacon[i]['PAN'] = "";    
//                         }if (datacon[i]['PER_STATUS'] === "INDIVIDUAL" || datacon[i]['PER_STATUS'] === "Resident Individual") {
//                            datacon[i]['PER_STATUS'] = "Individual";
//                        }if (datacon[i]['PER_STATUS'] === "HINDU UNDIVIDED FAMI") {
//                          datacon[i]['PER_STATUS'] = "HUF";
//                       }
//                      }

//                      resdata.data = datacon.sort((a, b) => new Date(b.TRADDATE.split("-").reverse().join("/")).getTime() - new Date(a.TRADDATE.split("-").reverse().join("/")).getTime());
//                      res.json(resdata);
//                     return resdata;
//                     }else{
//                         resdata = {
//                             status: 400,
//                             message: 'Data not found',
                            
//                         }
//                         res.json(resdata);
//                        return resdata;
//                     }
                    
//                  });
//              });
//           });
//         });
//     });
//         }else{
//             pipeline = [  ///trans_cams
//                 { $group: { _id: { TAX_STATUS:"$TAX_STATUS",TRXNNO:"$TRXNNO",INV_NAME: "$INV_NAME", PAN: "$PAN", TRXN_NATUR: "$TRXN_NATUR", FOLIO_NO: "$FOLIO_NO", SCHEME: "$SCHEME", AMOUNT: "$AMOUNT", TRADDATE: "$TRADDATE" } } },
//                 { $project: { _id: 0,PER_STATUS:"$_id.TAX_STATUS",TRXNNO:"$_id.TRXNNO", INVNAME: "$_id.INV_NAME", PAN: "$_id.PAN", TRXN_NATUR: "$_id.TRXN_NATUR", FOLIO_NO: "$_id.FOLIO_NO", SCHEME: "$_id.SCHEME", AMOUNT: "$_id.AMOUNT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRADDATE" } }, month: { $month: ('$_id.TRADDATE') }, year: { $year: ('$_id.TRADDATE') } } },
//                 { $match: { $and: [{ month: mon }, { year: yer } ,{PAN:req.body.pan}  ] } },
//                 { $lookup: { from: 'folio_cams', localField: 'FOLIO_NO', foreignField: 'FOLIOCHK', as: 'detail' } },
//                 { $replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$detail", 0 ] }, "$$ROOT" ] } } } ,
//                 { $project: {    detail: 0 ,_id:0,TAX_STATUS:0,FOLIOCHK:0,AC_NO:0,FOLIO_DATE:0,PRODUCT:0,SCH_NAME:0,AMC_CODE:0,BANK_NAME:0,HOLDING_NA:0,IFSC_CODE:0,JNT_NAME1:0,JNT_NAME2:0,JOINT1_PAN:0,NOM2_NAME:0,NOM3_NAME:0,NOM_NAME:0,PRCODE:0,HOLDING_NATURE:0,PAN_NO:0,INV_NAME:0,EMAIL:0} },
//                 { $sort: { TRADDATE: -1 } }
//             ]
//              pipeline1 = [  ///trans_karvy
//                 { $group: { _id: { STATUS:"$STATUS",TD_TRNO:"$TD_TRNO", INVNAME: "$INVNAME", PAN1: "$PAN1", TRDESC: "$TRDESC", TD_ACNO: "$TD_ACNO", FUNDDESC: "$FUNDDESC", TD_AMT: "$TD_AMT", TD_TRDT: "$TD_TRDT" } } },
//                 { $project: { _id: 0,PER_STATUS:"$_id.STATUS", TRXNNO:"$_id.TD_TRNO",INVNAME: "$_id.INVNAME", PAN: "$_id.PAN1", TRXN_NATUR: "$_id.TRDESC", FOLIO_NO: "$_id.TD_ACNO", SCHEME: "$_id.FUNDDESC", AMOUNT: "$_id.TD_AMT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TD_TRDT" } }, month: { $month: ('$_id.TD_TRDT') }, year: { $year: ('$_id.TD_TRDT') } } },
//                 { $match: { $and: [{ month: mon }, { year: yer }  ,{PAN:req.body.pan} ] } },
//                 { $lookup: { from: 'folio_karvy', localField: 'FOLIO_NO', foreignField: 'ACNO', as: 'detail' } },
//                 { $replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$detail", 0 ] }, "$$ROOT" ] } } } ,
//                 { $project: { detail: 0 , _id:0,STATUS:0,PRCODE:0,STATUSDESC:0,ACNO:0,BNKACNO:0,BNKACTYPE:0,FUNDDESC:0,NOMINEE:0,MODEOFHOLD:0,JTNAME2:0,FUND:0,EMAIL:0,BNAME:0,PANGNO:0,JTNAME1:0,PAN2:0} },
//                 { $sort: { TRADDATE: -1 } }
//             ]
//              pipeline2 = [  ///trans_franklin
//                 { $group: { _id: { SOCIAL_S18:"$SOCIAL_S18",TRXN_NO:"$TRXN_NO", INVESTOR_2: "$INVESTOR_2", IT_PAN_NO1: "$IT_PAN_NO1", TRXN_TYPE: "$TRXN_TYPE", FOLIO_NO: "$FOLIO_NO", SCHEME_NA1: "$SCHEME_NA1", AMOUNT: "$AMOUNT", TRXN_DATE: "$TRXN_DATE" } } },
//                 { $project: { _id: 0, PER_STATUS:"$_id.SOCIAL_S18",TRXNNO:"$_id.TRXN_NO", INVNAME: "$_id.INVESTOR_2", PAN: "$_id.IT_PAN_NO1", TRXN_NATUR: "$_id.TRXN_TYPE", FOLIO_NO: "$_id.FOLIO_NO", SCHEME: "$_id.SCHEME_NA1", AMOUNT: "$_id.AMOUNT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRXN_DATE" } }, month: { $month: ('$_id.TRXN_DATE') }, year: { $year: ('$_id.TRXN_DATE') } } },
//                 { $match: { $and: [{ month: mon }, { year: yer }  ,{PAN:req.body.pan}] } },
//                 { $lookup: { from: 'folio_franklin', localField: 'FOLIO_NO', foreignField: 'FOLIO_NO', as: 'detail' } },
//                 { $replaceRoot: { newRoot: { $mergeObjects: [ { $arrayElemAt: [ "$detail", 0 ] }, "$$ROOT" ] } } } ,
//                 { $project: { detail: 0 ,_id:0,TAX_STATUS:0,PERSONAL_9:0,ACCNT_NO:0,AC_TYPE:0,ADDRESS1:0,BANK_CODE:0,BANK_NAME:0,COMP_CODE:0,D_BIRTH:0,EMAIL:0,HOLDING_T6:0,F_NAME:0,IFSC_CODE:0,JOINT_NAM1:0,JOINT_NAM2:0,KYC_ID:0,NEFT_CODE:0,NOMINEE1:0,PBANK_NAME:0,PANNO2:0,PANNO1:0,PHONE_RES:0,SOCIAL_ST7:0} },
//                 { $sort: { TRADDATE: -1 } }
//             ]
    
//             transc.aggregate(pipeline, (err, camsdata) => {
//                 transk.aggregate(pipeline1, (err, karvydata) => {
//                     transf.aggregate(pipeline2, (err, frankdata) => {
//                         if (frankdata.length != 0 || karvydata.length != 0 || camsdata.length != 0) {
//                             resdata = {
//                                 status: 200,
//                                 message: 'Successfull',
//                                 data: frankdata
//                             }
                        
//                         datacon = frankdata.concat(karvydata.concat(camsdata))
//                         datacon = datacon.map(JSON.stringify).reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
//                             .filter(function (item, index, arr) { return arr.indexOf(item, index + 1) === -1; }) // check if there is any occurence of the item in whole array
//                             .reverse().map(JSON.parse);

//                             datacon = datacon.map(function(obj) {
//                             if(obj['GUARDIANN0']){
//                                 obj['GUARD_NAME'] = obj['GUARDIANN0']; // Assign new key
//                                 obj['GUARD_PAN'] = obj['GUARDPANNO'];
//                                  // Delete old key
//                                       delete obj['GUARDIANN0'];
//                                       delete obj['GUARDPANNO'];
//                             }else if((obj['GUARDIANN0']) === ""){
//                                     obj['GUARD_NAME'] = obj['GUARDIANN0']; // Assign new key
//                                     obj['GUARD_PAN'] = obj['GUARDPANNO'];
//                                     delete obj['GUARDIANN0'];
//                                     delete obj['GUARDPANNO'];
//                                 }
//                             if(obj['GUARDIAN20']){
//                                 obj['GUARD_NAME'] = obj['GUARDIAN20']; // Assign new key
//                                  // Delete old key
//                                 delete obj['GUARDIAN20'];
//                             }else if((obj['GUARDIAN20']) === ""){
//                                 obj['GUARD_NAME'] = obj['GUARDIAN20']; // Assign new key
//                                 delete obj['GUARDIAN20'];
//                             }
//                                 return obj;
//                             });
//                         for (var i = 0; i < datacon.length; i++) {
//                             if (datacon[i]['TRXN_NATUR'] === "Redemption" || datacon[i]['TRXN_NATUR'] === "FUL" || datacon[i]['TRXN_NATUR'] === "SIPR" || 
//                             datacon[i]['TRXN_NATUR'] === "Full Redemption" || datacon[i]['TRXN_NATUR'] === "Partial Redemption") {
//                                 datacon[i]['TRXN_NATUR'] = "RED";
//                             }  if (datacon[i]['TRXN_NATUR'].match(/Systematic Investment.*/) || 
//                              datacon[i]['TRXN_NATUR'].match(/Systematic - Instalment.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic - To.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic-NSE.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic Physical.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic-Normal.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic (ECS).*/)) {
//                                 datacon[i]['TRXN_NATUR'] = "SIP";
//                             } if (datacon[i]['TRXN_NATUR'] === "Systematic Withdrawal") {
//                                 datacon[i]['TRXN_NATUR'] = "SWP";
//                             }if (Math.sign(datacon[i]['AMOUNT']) === -1) {
//                                 datacon[i]['TRXN_NATUR'] = "SIPR";
//                             } if (datacon[i]['TRXN_NATUR'].match(/Systematic - From.*/) || datacon[i]['TRXN_NATUR'] === "S T P" || datacon[i]['TRXN_NATUR'] === "S T P In") {
//                                 datacon[i]['TRXN_NATUR'] = "STP";
//                             }if (datacon[i]['TRXN_NATUR'] === "Lateral Shift Out" || datacon[i]['TRXN_NATUR'] === "Switchout"
//                              || datacon[i]['TRXN_NATUR'] === "Transfer-Out" || datacon[i]['TRXN_NATUR'] === "Transmission Out"
//                               || datacon[i]['TRXN_NATUR'] === "Switch Over Out" || datacon[i]['TRXN_NATUR'] === "LTOP"
//                               || datacon[i]['TRXN_NATUR'] === "LTOF" || datacon[i]['TRXN_NATUR'] === "Partial Switch Out" || 
//                               datacon[i]['TRXN_NATUR'] === "Full Switch Out") {
//                                 datacon[i]['TRXN_NATUR'] = "Switch Out";
//                             }if (datacon[i]['TRXN_NATUR'] === "Lateral Shift In" || datacon[i]['TRXN_NATUR'] === "Switch-In" 
//                             || datacon[i]['TRXN_NATUR'] === "Transfer-In" || datacon[i]['TRXN_NATUR'] === "Switch Over In" 
//                             || datacon[i]['TRXN_NATUR'] === "LTIN" || datacon[i]['TRXN_NATUR'] === "LTIA") {
//                                 datacon[i]['TRXN_NATUR'] = "Switch In";
//                             }if (datacon[i]['TRXN_NATUR'] === "Dividend Reinvest" || 
//                             datacon[i]['TRXN_NATUR'] === "Dividend Paid"
//                              || datacon[i]['TRXN_NATUR'] === "Div. Reinvestment") {
//                                 datacon[i]['TRXN_NATUR'] = "Dividend";
//                             }if (datacon[i]['TRXN_NATUR'] === "Gross Dividend") {
//                                 datacon[i]['TRXN_NATUR'] = "Dividend Payout";
//                             }if (datacon[i]['TRXN_NATUR'] === "Consolidation In") {
//                                 datacon[i]['TRXN_NATUR'] = "Con In";
//                             }if (datacon[i]['TRXN_NATUR'] === "Consolidation Out") {
//                                 datacon[i]['TRXN_NATUR'] = "Con Out";
//                             }if (datacon[i]['TRXN_NATUR'] === "Consolidation Out") {
//                                 datacon[i]['TRXN_NATUR'] = "Con Out";
//                             }if (datacon[i]['TRXN_NATUR'] === "Purchase" || datacon[i]['TRXN_NATUR'] === "NEW" || 
//                                 datacon[i]['TRXN_NATUR'] === "Initial Allotment"
//                             || datacon[i]['TRXN_NATUR'] === "NEWPUR") {
//                                 datacon[i]['TRXN_NATUR'] = "Purchase";
//                             }if(datacon[i]['TRXN_NATUR'] === "Additional Purchase" || datacon[i]['TRXN_NATUR'] === "ADD" ||
//                              datacon[i]['TRXN_NATUR'] === "ADDPUR") {
//                                 datacon[i]['TRXN_NATUR'] = "Add. Purchase";
//                             }if (datacon[i]['PER_STATUS'] === "On Behalf Of Minor" || datacon[i]['PER_STATUS'] === "MINOR" || datacon[i]['PER_STATUS'] === "On Behalf of Minor" )  {
//                                 datacon[i]['PER_STATUS'] = "Minor";   
// 				datacon[i]['PAN'] = "";   
//                             }if (datacon[i]['PER_STATUS'] === "INDIVIDUAL" || datacon[i]['PER_STATUS'] === "Resident Individual") {
//                                datacon[i]['PER_STATUS'] = "Individual";
//                            }if (datacon[i]['PER_STATUS'] === "HINDU UNDIVIDED FAMI") {
//                              datacon[i]['PER_STATUS'] = "HUF";
//                           }
//                         }
//                         resdata.data = datacon.sort((a, b) => new Date(b.TRADDATE.split("-").reverse().join("/")).getTime() - new Date(a.TRADDATE.split("-").reverse().join("/")).getTime())
//                         res.json(resdata)
//                         return resdata;
//                     } else {
//                         resdata = {
//                             status: 400,
//                             message: 'Data not found',
//                         }
//                         res.json(resdata)
//                         return resdata;
//                     }
//                     });
//                 });
//             });
//            }       
//         });
//     }
// } catch (err) {
//     console.log(err)
// }
// })

app.post("/api/gettransactionuserwise", function (req, res) {
    try{
        var member="";var guardpan1=[];var guardpan2=[];
        var arr1=[];var arr2=[];var arr3=[];var alldata=[];var arrFolio=[];var arrName=[];
        let regex = /^([a-zA-Z]){5}([0-9]){4}([a-zA-Z]){1}?$/;
        if(req.body.month ===""){
            resdata = {
                status: 400,
                message: 'Please enter month',
            }
            res.json(resdata);
            return resdata;
        }else if(req.body.year ===""){
            resdata = {
                status: 400,
                message: 'Please enter year',
            }
            res.json(resdata);
            return resdata;
        }else if(req.body.pan ===""){
            resdata = {
                status: 400,
                message: 'Please enter pan',
            }
            res.json(resdata);
            return resdata;
        }else if(!regex.test(req.body.pan)) {
            resdata = {
                status: 400,
                message: 'Please enter valid pan',
            }
            res.json(resdata);
            return resdata;
        }else{
            var lastdate =  new Date(req.body.year, req.body.month, 0).getDate();
            var firstdate = req.body.year+"-"+req.body.month+"-"+"01";
            var seconddate = req.body.year+"-"+req.body.month+"-"+lastdate;
            
             family.find({ adminPan:  {$regex : `^${req.body.pan}.*` , $options: 'i' }  },{_id:0,memberPan:1}, function (err, member) {
                if(member!=""){
                    member  = [...new Set(member.map(({memberPan}) => memberPan.toUpperCase()))];
                    guardpan1.push({GUARD_PAN:req.body.pan.toUpperCase()});
                    guardpan2.push({GUARDPANNO:req.body.pan.toUpperCase()});
                    arr1.push({PAN:req.body.pan.toUpperCase()});
                     arr2.push({PAN1:req.body.pan.toUpperCase()});
                    for(var j=0;j<member.length;j++){     
                    guardpan1.push({GUARD_PAN:member[j]}); 
                    guardpan2.push({GUARDPANNO:member[j]});
                    arr1.push({PAN:member[j]});
                     arr2.push({PAN1:member[j]});
                     }
                    var strPan1 = {$or:guardpan1};
                    var strPan2 = {$or:guardpan2};
                    folioc.find(strPan1).distinct("FOLIOCHK", function (err, member1) {
                      foliok.find(strPan2).distinct("ACNO", function (err, member2) {
                      var alldata = member1.concat(member2);   
                      for(var j=0;j<alldata.length;j++){     
                        arr1.push({FOLIO_NO:alldata[j]});
                         arr2.push({TD_ACNO:alldata[j]});
                         }
                        var strFolio = {$or:arr1};
                         var strFolio1 = {$or:arr2};
        pipeline = [  ///trans_cams
            { $match: { $and: [{TRADDATE: { $gte: new Date(moment(firstdate).format("YYYY-MM-DD")), $lt: new Date(moment(seconddate).format("YYYY-MM-DD")) } }, strFolio  ] } },

            { $group: { _id: { TAX_STATUS:"$TAX_STATUS",TRXNNO:"$TRXNNO",INV_NAME: "$INV_NAME", PAN: "$PAN", TRXN_NATUR: "$TRXN_NATUR", FOLIO_NO: "$FOLIO_NO", SCHEME: "$SCHEME", AMOUNT: "$AMOUNT", TRADDATE: "$TRADDATE" } } },
            { $lookup: { from: 'folio_cams', localField: '_id.FOLIO_NO', foreignField: 'FOLIOCHK', as: 'detail' } },
            { $project: { _id: 0,GUARD_NAME:"$detail.GUARD_NAME",GUARD_PAN:"$detail.GUARD_PAN",PER_STATUS:"$_id.TAX_STATUS",TRXNNO:"$_id.TRXNNO", INVNAME: "$_id.INV_NAME", PAN: "$_id.PAN", TRXN_NATUR: "$_id.TRXN_NATUR", FOLIO_NO: "$_id.FOLIO_NO", SCHEME: "$_id.SCHEME", AMOUNT: "$_id.AMOUNT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRADDATE" } } } },
            { $sort: { TRADDATE: -1 } }
        ]
         pipeline1 = [  ///trans_karvy
            { $match: { $and: [{TD_TRDT: { $gte: new Date(moment(firstdate).format("YYYY-MM-DD")), $lt: new Date(moment(seconddate).format("YYYY-MM-DD")) } }, strFolio1  ] } },

            { $group: { _id: { STATUS:"$STATUS",TD_TRNO:"$TD_TRNO", INVNAME: "$INVNAME", PAN1: "$PAN1", TRDESC: "$TRDESC", TD_ACNO: "$TD_ACNO", FUNDDESC: "$FUNDDESC", TD_AMT: "$TD_AMT", TD_TRDT: "$TD_TRDT" } } },
            { $lookup: { from: 'folio_karvy', localField: '_id.TD_ACNO',foreignField: 'ACNO',as:'detail'} },
            { $project: { _id: 0,GUARD_NAME:"$detail.GUARDIANN0",GUARD_PAN:"$detail.GUARDPANNO",PER_STATUS:"$_id.STATUS", TRXNNO:"$_id.TD_TRNO",INVNAME: "$_id.INVNAME", PAN: "$_id.PAN1", TRXN_NATUR: "$_id.TRDESC", FOLIO_NO: "$_id.TD_ACNO", SCHEME: "$_id.FUNDDESC", AMOUNT: "$_id.TD_AMT", TRADDATE: { $dateToString:{format: "%d-%m-%Y", date: "$_id.TD_TRDT" } }, month: { $month: ('$_id.TD_TRDT') }, year: { $year: ('$_id.TD_TRDT') } } },
            { $sort: { TRADDATE: -1 } }
        ]
       
        transc.aggregate(pipeline, (err, camsdata) => {
            transk.aggregate(pipeline1, (err, karvydata) => {
                    if ( karvydata != 0 || camsdata != 0) {
                        resdata = {
                            status: 200,
                            message: 'Successfull',
                            data: frankdata
                        }
                        var datacon = karvydata.concat(camsdata);
                        datacon = datacon.map(JSON.stringify).reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
                            .filter(function (item, index, arr) { return arr.indexOf(item, index + 1) === -1; }) // check if there is any occurence of the item in whole array
                            .reverse().map(JSON.parse);
                            var newdata1 = datacon.map(item=>{
                                return [JSON.stringify(item),item]
                                 }); // creates array of array
                                 var maparr1 = new Map(newdata1); // create key value pair from array of array
                                 datacon = [...maparr1.values()];//converting back to array from mapobject 

                             

                        for (var i = 0; i < datacon.length; i++) {
                            if (datacon[i]['TRXN_NATUR'] === "Redemption" || datacon[i]['TRXN_NATUR'] === "FUL" || datacon[i]['TRXN_NATUR'] === "SIPR" || 
                        datacon[i]['TRXN_NATUR'] === "Full Redemption" || datacon[i]['TRXN_NATUR'] === "Partial Redemption") {
                            datacon[i]['TRXN_NATUR'] = "RED";
                        }  if (datacon[i]['TRXN_NATUR'].match(/Systematic Investment.*/) || 
                         datacon[i]['TRXN_NATUR'].match(/Systematic - Instalment.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic - To.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic-NSE.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic Physical.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic-Normal.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic (ECS).*/)) {
                            datacon[i]['TRXN_NATUR'] = "SIP";
                        } if (datacon[i]['TRXN_NATUR'] === "Systematic Withdrawal") {
                            datacon[i]['TRXN_NATUR'] = "SWP";
                        }if (Math.sign(datacon[i]['AMOUNT']) === -1) {
                            datacon[i]['TRXN_NATUR'] = "SIPR";
                        } if (datacon[i]['TRXN_NATUR'].match(/Systematic - From.*/) || datacon[i]['TRXN_NATUR'] === "S T P" || datacon[i]['TRXN_NATUR'] === "S T P In") {
                            datacon[i]['TRXN_NATUR'] = "STP";
                        }if (datacon[i]['TRXN_NATUR'] === "Lateral Shift Out" || datacon[i]['TRXN_NATUR'] === "Switchout"
                         || datacon[i]['TRXN_NATUR'] === "Transfer-Out" || datacon[i]['TRXN_NATUR'] === "Transmission Out"
                          || datacon[i]['TRXN_NATUR'] === "Switch Over Out" || datacon[i]['TRXN_NATUR'] === "LTOP"
                          || datacon[i]['TRXN_NATUR'] === "LTOF" || datacon[i]['TRXN_NATUR'] === "Partial Switch Out" || 
                          datacon[i]['TRXN_NATUR'] === "Full Switch Out") {
                            datacon[i]['TRXN_NATUR'] = "Switch Out";
                        }if (datacon[i]['TRXN_NATUR'] === "Lateral Shift In" || datacon[i]['TRXN_NATUR'] === "Switch-In" 
                        || datacon[i]['TRXN_NATUR'] === "Transfer-In" || datacon[i]['TRXN_NATUR'] === "Switch Over In" 
                        || datacon[i]['TRXN_NATUR'] === "LTIN" || datacon[i]['TRXN_NATUR'] === "LTIA") {
                            datacon[i]['TRXN_NATUR'] = "Switch In";
                        }if (datacon[i]['TRXN_NATUR'] === "Dividend Reinvest" || 
                        datacon[i]['TRXN_NATUR'] === "Dividend Paid"
                         || datacon[i]['TRXN_NATUR'] === "Div. Reinvestment") {
                            datacon[i]['TRXN_NATUR'] = "Dividend";
                        }if (datacon[i]['TRXN_NATUR'] === "Gross Dividend") {
                            datacon[i]['TRXN_NATUR'] = "Dividend Payout";
                        }if (datacon[i]['TRXN_NATUR'] === "Consolidation In") {
                            datacon[i]['TRXN_NATUR'] = "Con In";
                        }if (datacon[i]['TRXN_NATUR'] === "Consolidation Out") {
                            datacon[i]['TRXN_NATUR'] = "Con Out";
                        }if (datacon[i]['TRXN_NATUR'] === "Consolidation Out") {
                            datacon[i]['TRXN_NATUR'] = "Con Out";
                        }if (datacon[i]['TRXN_NATUR'] === "Purchase" || datacon[i]['TRXN_NATUR'] === "NEW" || 
                            datacon[i]['TRXN_NATUR'] === "Initial Allotment"
                        || datacon[i]['TRXN_NATUR'] === "NEWPUR") {
                            datacon[i]['TRXN_NATUR'] = "Purchase";
                        }if(datacon[i]['TRXN_NATUR'] === "Additional Purchase" || datacon[i]['TRXN_NATUR'] === "ADD" ||
                         datacon[i]['TRXN_NATUR'] === "ADDPUR") {
                            datacon[i]['TRXN_NATUR'] = "Add. Purchase";
                        }if (datacon[i]['PER_STATUS'] === "On Behalf Of Minor" || datacon[i]['PER_STATUS'] === "MINOR" || datacon[i]['PER_STATUS'] === "On Behalf of Minor" )  {
                            datacon[i]['PER_STATUS'] = "Minor";     
		            datacon[i]['PAN'] = "";    
                        }if (datacon[i]['PER_STATUS'] === "INDIVIDUAL" || datacon[i]['PER_STATUS'] === "Resident Individual") {
                           datacon[i]['PER_STATUS'] = "Individual";
                       }if (datacon[i]['PER_STATUS'] === "HINDU UNDIVIDED FAMI") {
                         datacon[i]['PER_STATUS'] = "HUF";
                      }
                       if (datacon[i]['GUARD_NAME']) {
                        datacon[i]['GUARD_NAME'] = datacon[i]['GUARD_NAME'][0] ;
                    } if (datacon[i]['GUARD_PAN']) {
                        datacon[i]['GUARD_PAN'] = datacon[i]['GUARD_PAN'][0] ;
                    } if (datacon[i]['GUARD_NAME'] === null && datacon[i]['GUARD_PAN'] === null) {
                        datacon[i]['GUARD_NAME'] = "";
                        datacon[i]['GUARD_PAN'] = "";
                      }
                     }

                     resdata.data = datacon.sort((a, b) => new Date(b.TRADDATE.split("-").reverse().join("/")).getTime() - new Date(a.TRADDATE.split("-").reverse().join("/")).getTime());
                     res.json(resdata);
                    return resdata;
                    }else{
                        resdata = {
                            status: 400,
                            message: 'Data not found',
                            
                        }
                        res.json(resdata);
                       return resdata;
                    }
                    
             });
          });
        });
    });
        }else{
            pipeline = [  ///trans_cams
                { $match: { $and: [{TRADDATE: { $gte: new Date(moment(firstdate).format("YYYY-MM-DD")), $lt: new Date(moment(seconddate).format("YYYY-MM-DD")) } }, {PAN1:req.body.pan }  ] } },
    
                { $group: { _id: { TAX_STATUS:"$TAX_STATUS",TRXNNO:"$TRXNNO",INV_NAME: "$INV_NAME", PAN: "$PAN", TRXN_NATUR: "$TRXN_NATUR", FOLIO_NO: "$FOLIO_NO", SCHEME: "$SCHEME", AMOUNT: "$AMOUNT", TRADDATE: "$TRADDATE" } } },
                { $lookup: { from: 'folio_cams', localField: '_id.FOLIO_NO', foreignField: 'FOLIOCHK', as: 'detail' } },
                { $project: { _id: 0,GUARD_NAME:"$detail.GUARD_NAME",GUARD_PAN:"$detail.GUARD_PAN",PER_STATUS:"$_id.TAX_STATUS",TRXNNO:"$_id.TRXNNO", INVNAME: "$_id.INV_NAME", PAN: "$_id.PAN", TRXN_NATUR: "$_id.TRXN_NATUR", FOLIO_NO: "$_id.FOLIO_NO", SCHEME: "$_id.SCHEME", AMOUNT: "$_id.AMOUNT", TRADDATE: { $dateToString: { format: "%d-%m-%Y", date: "$_id.TRADDATE" } } } },
                { $sort: { TRADDATE: -1 } }
            ]
             pipeline1 = [  ///trans_karvy
                { $match: { $and: [{TD_TRDT: { $gte: new Date(moment(firstdate).format("YYYY-MM-DD")), $lt: new Date(moment(seconddate).format("YYYY-MM-DD")) } },{PAN1:req.body.pan } ] } },
    
                { $group: { _id: { STATUS:"$STATUS",TD_TRNO:"$TD_TRNO", INVNAME: "$INVNAME", PAN1: "$PAN1", TRDESC: "$TRDESC", TD_ACNO: "$TD_ACNO", FUNDDESC: "$FUNDDESC", TD_AMT: "$TD_AMT", TD_TRDT: "$TD_TRDT" } } },
                { $lookup: { from: 'folio_karvy', localField: '_id.TD_ACNO',foreignField: 'ACNO',as:'detail'} },
                { $project: { _id: 0,GUARD_NAME:"$detail.GUARDIANN0",GUARD_PAN:"$detail.GUARDPANNO",PER_STATUS:"$_id.STATUS", TRXNNO:"$_id.TD_TRNO",INVNAME: "$_id.INVNAME", PAN: "$_id.PAN1", TRXN_NATUR: "$_id.TRDESC", FOLIO_NO: "$_id.TD_ACNO", SCHEME: "$_id.FUNDDESC", AMOUNT: "$_id.TD_AMT", TRADDATE: { $dateToString:{format: "%d-%m-%Y", date: "$_id.TD_TRDT" } }, month: { $month: ('$_id.TD_TRDT') }, year: { $year: ('$_id.TD_TRDT') } } },
                { $sort: { TRADDATE: -1 } }
            ]
           
             
            transc.aggregate(pipeline, (err, camsdata) => {
                transk.aggregate(pipeline1, (err, karvydata) => {
                      if ( karvydata.length != 0 || camsdata.length != 0) {
                            resdata = {
                                status: 200,
                                message: 'Successfull',
                                data: frankdata
                            }
                        
                        datacon = karvydata.concat(camsdata)
                        datacon = datacon.map(JSON.stringify).reverse() // convert to JSON string the array content, then reverse it (to check from end to begining)
                            .filter(function (item, index, arr) { return arr.indexOf(item, index + 1) === -1; }) // check if there is any occurence of the item in whole array
                            .reverse().map(JSON.parse);

                         
                        for (var i = 0; i < datacon.length; i++) {
                            if (datacon[i]['TRXN_NATUR'] === "Redemption" || datacon[i]['TRXN_NATUR'] === "FUL" || datacon[i]['TRXN_NATUR'] === "SIPR" || 
                            datacon[i]['TRXN_NATUR'] === "Full Redemption" || datacon[i]['TRXN_NATUR'] === "Partial Redemption") {
                                datacon[i]['TRXN_NATUR'] = "RED";
                            }  if (datacon[i]['TRXN_NATUR'].match(/Systematic Investment.*/) || 
                             datacon[i]['TRXN_NATUR'].match(/Systematic - Instalment.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic - To.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic-NSE.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic Physical.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic-Normal.*/) || datacon[i]['TRXN_NATUR'].match(/Systematic (ECS).*/)) {
                                datacon[i]['TRXN_NATUR'] = "SIP";
                            } if (datacon[i]['TRXN_NATUR'] === "Systematic Withdrawal") {
                                datacon[i]['TRXN_NATUR'] = "SWP";
                            }if (Math.sign(datacon[i]['AMOUNT']) === -1) {
                                datacon[i]['TRXN_NATUR'] = "SIPR";
                            } if (datacon[i]['TRXN_NATUR'].match(/Systematic - From.*/) || datacon[i]['TRXN_NATUR'] === "S T P" || datacon[i]['TRXN_NATUR'] === "S T P In") {
                                datacon[i]['TRXN_NATUR'] = "STP";
                            }if (datacon[i]['TRXN_NATUR'] === "Lateral Shift Out" || datacon[i]['TRXN_NATUR'] === "Switchout"
                             || datacon[i]['TRXN_NATUR'] === "Transfer-Out" || datacon[i]['TRXN_NATUR'] === "Transmission Out"
                              || datacon[i]['TRXN_NATUR'] === "Switch Over Out" || datacon[i]['TRXN_NATUR'] === "LTOP"
                              || datacon[i]['TRXN_NATUR'] === "LTOF" || datacon[i]['TRXN_NATUR'] === "Partial Switch Out" || 
                              datacon[i]['TRXN_NATUR'] === "Full Switch Out") {
                                datacon[i]['TRXN_NATUR'] = "Switch Out";
                            }if (datacon[i]['TRXN_NATUR'] === "Lateral Shift In" || datacon[i]['TRXN_NATUR'] === "Switch-In" 
                            || datacon[i]['TRXN_NATUR'] === "Transfer-In" || datacon[i]['TRXN_NATUR'] === "Switch Over In" 
                            || datacon[i]['TRXN_NATUR'] === "LTIN" || datacon[i]['TRXN_NATUR'] === "LTIA") {
                                datacon[i]['TRXN_NATUR'] = "Switch In";
                            }if (datacon[i]['TRXN_NATUR'] === "Dividend Reinvest" || 
                            datacon[i]['TRXN_NATUR'] === "Dividend Paid"
                             || datacon[i]['TRXN_NATUR'] === "Div. Reinvestment") {
                                datacon[i]['TRXN_NATUR'] = "Dividend";
                            }if (datacon[i]['TRXN_NATUR'] === "Gross Dividend") {
                                datacon[i]['TRXN_NATUR'] = "Dividend Payout";
                            }if (datacon[i]['TRXN_NATUR'] === "Consolidation In") {
                                datacon[i]['TRXN_NATUR'] = "Con In";
                            }if (datacon[i]['TRXN_NATUR'] === "Consolidation Out") {
                                datacon[i]['TRXN_NATUR'] = "Con Out";
                            }if (datacon[i]['TRXN_NATUR'] === "Consolidation Out") {
                                datacon[i]['TRXN_NATUR'] = "Con Out";
                            }if (datacon[i]['TRXN_NATUR'] === "Purchase" || datacon[i]['TRXN_NATUR'] === "NEW" || 
                                datacon[i]['TRXN_NATUR'] === "Initial Allotment"
                            || datacon[i]['TRXN_NATUR'] === "NEWPUR") {
                                datacon[i]['TRXN_NATUR'] = "Purchase";
                            }if(datacon[i]['TRXN_NATUR'] === "Additional Purchase" || datacon[i]['TRXN_NATUR'] === "ADD" ||
                             datacon[i]['TRXN_NATUR'] === "ADDPUR") {
                                datacon[i]['TRXN_NATUR'] = "Add. Purchase";
                            }if (datacon[i]['PER_STATUS'] === "On Behalf Of Minor" || datacon[i]['PER_STATUS'] === "MINOR" || datacon[i]['PER_STATUS'] === "On Behalf of Minor" )  {
                                datacon[i]['PER_STATUS'] = "Minor";   
				datacon[i]['PAN'] = "";   
                            }if (datacon[i]['PER_STATUS'] === "INDIVIDUAL" || datacon[i]['PER_STATUS'] === "Resident Individual") {
                               datacon[i]['PER_STATUS'] = "Individual";
                           }if (datacon[i]['PER_STATUS'] === "HINDU UNDIVIDED FAMI") {
                             datacon[i]['PER_STATUS'] = "HUF";
                          } if (datacon[i]['GUARD_NAME']) {
                            datacon[i]['GUARD_NAME'] = datacon[i]['GUARD_NAME'][0] ;
                        } if (datacon[i]['GUARD_PAN']) {
                            datacon[i]['GUARD_PAN'] = datacon[i]['GUARD_PAN'][0] ;
                        } if (datacon[i]['GUARD_NAME'] === null && datacon[i]['GUARD_PAN'] === null) {
                            datacon[i]['GUARD_NAME'] = "";
                            datacon[i]['GUARD_PAN'] = "";
                          }
                        }
                        resdata.data = datacon.sort((a, b) => new Date(b.TRADDATE.split("-").reverse().join("/")).getTime() - new Date(a.TRADDATE.split("-").reverse().join("/")).getTime())
                        res.json(resdata)
                        return resdata;
                    } else {
                        resdata = {
                            status: 400,
                            message: 'Data not found',
                        }
                        res.json(resdata)
                        return resdata;
                    }
                  
                });
            });
           }       
        });
    }
} catch (err) {
    console.log(err)
}
})


   app.post("/api/getschemelist", function (req, res) {
	   try{
        var folio = req.body.folio;;
        const pipeline = [
                {$match : {FOLIO_NO:folio}},
                {$group : {_id : { AMC_CODE:"$AMC_CODE", PRODCODE:"$PRODCODE", code: { $substr: ["$PRODCODE", { $strLenCP: "$AMC_CODE" }, -1] }  }}},
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
            {$match : {TD_ACNO:folio,SCHEMEISIN : {$ne : null}}}, 
            {$group : {_id:{SCHEMEISIN:"$SCHEMEISIN"} }},
            {$lookup: { from: 'products',localField: '_id.SCHEMEISIN',foreignField: 'ISIN',as: 'master' } },
            { $unwind: "$master"},
            {$project:{_id:0,products:"$master" }   },
       ] 
        const pipeline2=[ ///trans_franklin
            {$match : {FOLIO_NO:folio,ISIN : {$ne : null}}}, 
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

//app.listen(port, ()=> { console.log("server started at port ",port)})

var server = app.listen(port, ()=> { console.log("server started at port ",port)});
server.setTimeout(500000);


