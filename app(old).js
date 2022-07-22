const { exec } = require('child_process');
const fs = require('fs');


function convertAvroToJson(offer, hour, min, name) {
    exec(`cd ./input && java -jar avro-tools-1.11.0.jar tojson ../AVROs/${offer}/${hour}/${min}/${name}.avro > 58.json`);

    setTimeout(() =>{
        const file = fs.readFileSync(`./input/58.json`, 'utf-8');
        const commas = file.replace(new RegExp('}"}}', 'g'), '}"}},');
        const removeLast = commas.slice(0, commas.length - 3);
        const jsons = `[${removeLast}]`;

        fs.writeFileSync("./input/58.json", jsons);
    }, 3000);
    
}

// function buildCsv(path, identifyer) {
//     const file = fs.readFileSync(path, 'utf-8');
//     const data = JSON.parse(file);

//     const releventInfoArr = data.map(x => JSON.parse(x.Body.bytes));

//     let outputClaim = 'PartitionKey,RowKey,CreatedOn,Platform,RewardId,Carrier,DeviceCarrier\r\n';
//     let outputGift = 'PartitionKey,RowKey,CreatedOn,Platform,RewardId,Carrier,DeviceCarrier\r\n';
//     let outputRedeem = 'PartitionKey,RowKey,CreatedOn,Platform,RewardId,Carrier,DeviceCarrier\r\n';

//     let claimTouched = false;
//     let giftTouched = false;
//     let redeemTouched = false;

//     releventInfoArr.map(item => {
//         const valueStr = `"${identifyer}","${item.EventId}","${item.CreatedOn}","${item.AppInformation.Platform}","${item.RewardId}","${item.AppInformation.Carrier}","${item.AppInformation.DeviceCarrier}"\r\n`;

//         if(item.Type === "Claim") {
//             outputClaim += valueStr;
//             claimTouched = true;
//         }

//         if(item.Type === "Gift") {
//             outputGift += valueStr;
//             giftTouched = true;
//         }

//         if(item.Type === "Redeem") {
//             outputRedeem += valueStr;
//             redeemTouched = true;
//         }
//     })

//     claimTouched && fs.writeFileSync(`./output/claim/${identifyer}-claim.csv`, outputClaim);
//     redeemTouched && fs.writeFileSync(`./output/redeem/${identifyer}-redeem.csv`, outputRedeem);
//     giftTouched && fs.writeFileSync(`./output/gift/${identifyer}-gift.csv`, outputGift);
// }

function buildOneCsv(path, identifyer) {
    const file = fs.readFileSync(path, 'utf-8');
    const data = JSON.parse(file);

    const releventInfoArr = data.map(x => JSON.parse(x.Body.bytes));

    const parts = identifyer.split("_");
    if(parts[2] === "1" || parts[2] === "2") {
        let head = 'PartitionKey,RowKey,CreatedOn,Platform,RewardId,Carrier,DeviceCarrier\r\n';

        fs.writeFileSync("./output/claim.csv", head);
        fs.writeFileSync("./output/gift.csv", head);
        fs.writeFileSync("./output/redeem.csv", head);
    }

    releventInfoArr.map(item => {
        const valueStr = `"${identifyer}","${item.EventId}","${item.CreatedOn}","${item.AppInformation.Platform}","${item.RewardId}","${item.AppInformation.Carrier}","${item.AppInformation.DeviceCarrier}"\r\n`;

        if(item.Type === "Claim") {
            let file = fs.readFileSync("./output/claim.csv", 'utf-8');
            file += valueStr;
            fs.writeFileSync("./output/claim.csv", file);
        }

        if(item.Type === "Gift") {
            let file = fs.readFileSync("./output/gift.csv", 'utf-8');
            file += valueStr;
            fs.writeFileSync("./output/gift.csv", file);
        }

        if(item.Type === "Redeem") {
            let file = fs.readFileSync("./output/redeem.csv", 'utf-8');
            file += valueStr;
            fs.writeFileSync("./output/redeem.csv", file);
        }
    })
}

var filePath = "./input/58.json";
const basePath = "./input";

// function runSet(hour, minset) {
//     minset.map(x => {
//         setTimeout(() => {
//             run(hour, x);
//         }, 45000)
//     })
// }

function run(offer, hour, min, name) {
    convertAvroToJson(offer, hour, min, name);

    setTimeout(() => {
        const identifyer = `20220412_${hour}_${min[0] === '0' ? min[1] : min}`;
        buildOneCsv(filePath, identifyer);
    }, 3000)
}

function runManually(identifyer) {
    buildOneCsv(filePath, identifyer);
}

const setOdds = ['01','06','11','16','21','26','31','36','41','46','51','56']
const setEvens = [];

//STEPS
//_________________________________________________________________________________________________________________________________________________________
//in run function set the offer (folders 0 -15 in avros directory)
////set hour (folders in offer folder)
////set minute (folders in hour folder)
////set name (name of avro file in minute fulder exclude extention)
//in terminal run node app.js
//import csv to the respective tables
//repeat for earch hour until complete then move to next offer.
//_________________________________________________________________________________________________________________________________________________________

//offer folder, hour folder, minute folder, name of avro file
run('2', '00', '57', '02');

//finished with this ^ need to import 