var express = require('express');
var ejs = require('ejs')
var path = require('path')
var app = express();
var fs = require('fs');

/* use ejs template engine instead of jade */
app.set('view engine', 'html');
app.engine('html', ejs.renderFile);
app.use(express.static(path.join(__dirname, 'public')));

function readData() {

    // get filenames
    var path = '../SparkTwitterClustering/output/merged_clusterInfo'
    var filenames = fs.readdirSync(path);
    filenames = filenames.filter(function(e){
        // startwith
        return e.lastIndexOf('part-', 0) === 0
    });

    var lines = []
    var times = []
    // for each filename
    for (index in filenames){
        var filename = path + '/' + filenames[index]
        var array = fs.readFileSync(filename).toString().split(")\n");
        if (array.length == 1) {
            console.log("empty file "+ filenames[index])
            continue;
        }
        array.forEach(function(line) {

            // remove first char (
            line = line.substring(1);
            var lineArray = line.split(",")
            if (lineArray.length < 8) return;
            var time = parseInt(lineArray[8])

            var contains = false
            times.forEach(function(ea) {
                if (ea === time) contains = true
            })
            if (!contains) times.push(time)

            var parsedLine = {
                'id': lineArray[0],
                'count': lineArray[1],
                'tweetId' :  lineArray[5],
                'tweet': 'https://twitter.com/nytimes/status/' + lineArray[5],
                'newsUrl': lineArray[6],
                'silhouette': lineArray[2],
                'intra': lineArray[3],
                'inter': lineArray[4],
                'interesting' : lineArray[7] == 'true',
                'batch_time' : time
            }
            lines.push(parsedLine);
        });
    }

    // group lines by clusterid
    function groupBy( array , f ) {
      var groups = {};
      array.forEach( function( o )
      {
        var group = JSON.stringify( f(o) );
        groups[group] = groups[group] || [];
        groups[group].push( o );
      });
      return Object.keys(groups).map( function( group )
      {
        return groups[group];
      })
    }

    var result = groupBy(lines, function(item) {
      return [item.id];
    });

    // filter uninteresting clusters
    result = result.filter(function(cluster) {
        var interesting = false;
        cluster.forEach(function(line){
            interesting = interesting || line.interesting
        });
        return interesting;
    })

    // add empty cluster if deleted
    return result.map(function(cluster) {
        if (cluster.length == times.length) return cluster;

        var existingBatches = cluster.map(function(ea) {return ea.batch_time})

        times.forEach(function(batch) {
            var contains = false
            existingBatches.forEach(function(ea) {
                if (ea == batch) contains = true
            });
            if (contains) return
            var empty = {
                'id': cluster[0].id,
                'count': 0,
                'tweetId' : '',
                'tweet': '',
                'newsUrl': '',
                'silhouette': '',
                'intra': '',
                'inter': '',
                'interesting' : '',
                'batch_time' : batch
            }
            // for this time no batch exists -> add empty batch
            empty.batch_time = batch
            cluster.push(empty)
        })
        return cluster.sort(function(a, b){
            return a['batch_time'] - b['batch_time']
        })
    })
}

app.set('views', './views');
app.set('view engine', 'jade');

var clusters = readData()
app.get('/index', function(req, res) {
    res.render('index.html', {
        // enter params for view here
        title: "Twitter News Stream",
        clusters: clusters,
        interesting: clusters.length
    });
});

app.listen(3000);
