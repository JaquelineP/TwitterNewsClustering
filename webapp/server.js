var express = require('express');
var app = express();

// read data and create "json object"
var data  = {};
var fs = require('fs');
// get filenames
var path = '../SparkTwitterClustering/output/merged_clusterInfo'
var filenames = fs.readdirSync(path);
filenames = filenames.filter(function(e){
    // startwith
    return e.lastIndexOf('part-', 0) === 0
});
var lines = []

// for each filename
var batch = [];
var k = -1
var pre = -1
for (index in filenames){
    var filename = path + '/' + filenames[index]
    var array = fs.readFileSync(filename).toString().split(")\n");
    if (array.length == 1) {
        console.log("empty file "+ filenames[index])
        continue;
    }
    for(i in array) {
        var line = array[i];
        // remove first char (
        line = line.substring(1);
        var lineObj = line.split(",")
        if (lineObj.length < 8) continue;
        lines.push(lineObj);
        var time = parseInt(lineObj[8])
        if (! (time in data)) {
            data[time] = []
        }
        data[time].push(lineObj)
    }
}

var dataArray = []
Object.keys(data).forEach(function (key) {
    console.log(key)
    var empty = [pre + 1, 0, -1, 0, 0, '000000000000000000|||||', '', false, key]

    // add empty clusters
    var pre = -1
    var array = []
    data[key].forEach(function(line){
        var clusterId  = parseInt(line[0]);
        while (pre + 1 < clusterId) {
            array.push(empty);
            pre++;
        }
        array.push(line)
        pre++;
    });
    dataArray.push(array);
});

app.set('views', './views');
app.set('view engine', 'jade');

var interesting = [];
for (i = 0; i < dataArray.length; i++) {
    for (y = 0; y < dataArray[i].length; y++) {
        if (dataArray[i][y][7] == 'true')
            if (!(y in interesting)) interesting.push(y);
    }
}

app.get('/', function(req, res) {
    res.render('home', {
        // enter params for view here
        title: "Twitter News Stream",
        clusters: dataArray,
        interesting: interesting
    });
});
  
app.listen(3000);