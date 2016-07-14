var express = require('express');
var ejs = require('ejs')

var app = express();

/* use ejs template engine instead of jade */
app.set('view engine', 'html');
app.engine('html', ejs.renderFile);


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

app.get('/index', function(req, res) {
        var clusters = [
        [
            {
                'id': 1,
                'count': 10,
                'tweet': 'https://twitter.com/nytimes/status/753559763808612352',
                'newsUrl': 'http://www.nytimes.com/2016/07/15/us/politics/house-democrats-campaign.html?smid=tw-nytimes&smtyp=cur&_r=0',
                'silhouette': 1.2,
                'intra': 0.3,
                'inter': 0.1
            },
            {
                'id': 1,
                'count': 15,
                'tweet': 'https://twitter.com/nytimes/status/753559763808612352',
                'newsUrl': 'http://www.nytimes.com/2016/07/15/us/politics/house-democrats-campaign.html?smid=tw-nytimes&smtyp=cur&_r=0',
                'silhouette': 1.2,
                'intra': 0.3,
                'inter': 0.1
            },
            {
                'id': 1,
                'count': 20,
                'tweet': 'https://twitter.com/nytimes/status/753559763808612352',
                'newsUrl': 'http://www.nytimes.com/2016/07/15/us/politics/house-democrats-campaign.html?smid=tw-nytimes&smtyp=cur&_r=0',
                'silhouette': 1.2,
                'intra': 0.3,
                'inter': 0.1
            },

        ],
        [
            {
                'id': 2,
                'count': 40,
                'tweet': 'https://twitter.com/nytimes/status/753559763808612352',
                'newsUrl': 'http://www.nytimes.com/2016/07/15/us/politics/house-democrats-campaign.html?smid=tw-nytimes&smtyp=cur&_r=0',
                'silhouette': 1.2,
                'intra': 0.3,
                'inter': 0.1
            },
            {
                'id': 2,
                'count': 20,
                'tweet': 'https://twitter.com/nytimes/status/753559763808612352', 
                'newsUrl': 'http://www.nytimes.com/2016/07/15/us/politics/house-democrats-campaign.html?smid=tw-nytimes&smtyp=cur&_r=0',
                'silhouette': 1.2,
                'intra': 0.3,
                'inter': 0.1
            },
            {
                'id': 2,
                'count': 20,
                'tweet': 'https://twitter.com/nytimes/status/753559763808612352', 
                'newsUrl': 'http://www.nytimes.com/2016/07/15/us/politics/house-democrats-campaign.html?smid=tw-nytimes&smtyp=cur&_r=0',
                'silhouette': 1.2,
                'intra': 0.3,
                'inter': 0.1
            },
        ]
    ]
    res.render('index.html', {
        // enter params for view here
        title: "Twitter News Stream",
        clusters: clusters,
        interesting: interesting
    });
});

app.get('/', function(req, res) {

    res.render('home', {
        // enter params for view here
        title: "Twitter News Stream",
        clusters: dataArray,
        interesting: interesting
    });
});
  
app.listen(3000);
