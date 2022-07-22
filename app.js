const fs = require('fs');
const util = require('util');
// const {
//     exec
// } = require("child_process");

const exec = util.promisify(require('child_process').exec)

async function getAvrosToJson(offer, hoursArr, minArr, name = '58' ) {
    let counter = 1;
    let out = [];
    console.log("Converting Avro files ro JSON.....")
    for (let i = 0; i < hoursArr.length; i++) {
        for (let j = 0; j < minArr.length; j++) {
            await exec(`cd ./input && java -jar avro-tools-1.11.0.jar tojson ../AVROs/${offer}/${hoursArr[i]}/${minArr[j]}/${name}.avro > ${counter}.json`);
            out.push({counter: counter, hour: hoursArr[i], min: minArr[j]});
            counter++;

        }
    }

    return out;
}

function formatJSON(count) {
    let head = 'PartitionKey,RowKey,CreatedOn,Platform,RewardId,Carrier,DeviceCarrier\r\n';

    fs.writeFileSync("./output/claim.csv", head);
    fs.writeFileSync("./output/gift.csv", head);
    fs.writeFileSync("./output/redeem.csv", head);

    for (let i = 0; i < count.length; i++) {
        console.log(`Creating csv files: ${Math.round((i/count.length) * 100)}%`)
        const file = fs.readFileSync(`./input/${count[i].counter}.json`, 'utf-8');
        const commas = file.replace(new RegExp('}"}}', 'g'), '}"}},');
        const removeLast = commas.slice(0, commas.length - 3);
        const jsons = `[${removeLast}]`;
        const parsed = JSON.parse(jsons);
        const releventInfoArr = parsed.map(x => JSON.parse(x.Body.bytes));

        const identifyer = `20220412_${count[i].hour}_${count[i].min[0] === '0' ? count[i].min[1] : count[i].min}`;

        releventInfoArr.map(item => {
            const valueStr = `"${identifyer}","${item.EventId}","${item.CreatedOn}","${item.AppInformation.Platform}","${item.RewardId}","${item.AppInformation.Carrier}","${item.AppInformation.DeviceCarrier}"\r\n`;

            if (item.Type === "Claim") {
                let file = fs.readFileSync("./output/claim.csv", 'utf-8');
                file += valueStr;
                fs.writeFileSync("./output/claim.csv", file);
            }

            if (item.Type === "Gift") {
                let file = fs.readFileSync("./output/gift.csv", 'utf-8');
                file += valueStr;
                fs.writeFileSync("./output/gift.csv", file);
            }

            if (item.Type === "Redeem") {
                let file = fs.readFileSync("./output/redeem.csv", 'utf-8');
                file += valueStr;
                fs.writeFileSync("./output/redeem.csv", file);
            }
        })
        fs.unlinkSync(`./input/${count[i].counter}.json`)
    }
    console.log("Creating csv files : 100%")
}

async function run2(offer, name, hourSet, minSet) {
    let counter = 1;

    const count = await getAvrosToJson(offer, hourSet, minset, name);

    formatJSON(count)
}

/*
    STEPS:
    _________________________________________________________________________________________________________________________________________________________
    prereqs: install Java JDK, npm install
    
    1. download avro files for all offers
    2. put them in the AVROs folder
    3. In run2 func pass in order the offer folder, the name of the avro file with 
    no extention (should be the same for all avros inside of an hour folder) then pass
    the "set" which is a minute array of every 5 minutes either starting on 01 or 02.

    4. in CMD at the directory for this project run node app.js.
    5. in the output folder there are csvs for claim, gift and redeem. Import these to 
    the matching table.

    6. repeat for all offers.

    note: this script was written at 2am quickly to finish a task. IT WILL CRASH!! also if your internet isnt great,
    uploading the csvs will take a substantial ammount of time. 

    if you pass the fullHoursSet it will crash. I have broken it up in to segments that can be done together. you may need to break it up more
    00 - 10
    11 - 14
    15
    16
    17 & 18


*/
const minuteSet1 = ['01', '06', '11', '16', '21', '26', '31', '36', '41', '46', '51', '56'];
const minuteSet2 = ['02','07','12','17','22','27','32','37','42','47','52','57'];

const fullHoursSet = ['00','01','02','03','04','05','06','07','08','09','10','11','12','13','14','15','16','17','18'];
 const hoursArr1 = ['00','01','02','03','04','05','06','07','08','09','10']
 const hoursArr2 = ['11','12','13','14'];
 const hoursArr3 = ['15'];
 const hoursArr4 = ['16'];
 const hoursArr5 = ['17', '18'];

run2('15', '07', hoursArr1, minuteSet1);