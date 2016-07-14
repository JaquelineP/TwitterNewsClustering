var express = require('express');
var ejs = require('ejs')
var path = require('path')
var app = express();

/* use ejs template engine instead of jade */
app.set('view engine', 'html');
app.engine('html', ejs.renderFile);
app.use(express.static(path.join(__dirname, 'public')));

function readData() {

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
            var parsedLine = {
                'id': lineArray[0],
                'count': lineArray[1],
                'tweet': 'https://twitter.com/nytimes/status/' + lineArray[5],
                'newsUrl': lineArray[6],
                'silhouette': lineArray[2],
                'intra': lineArray[3],
                'inter': lineArray[4],
                'interesting' : lineArray[7] == 'true',
                'batch_time' : lineArray[8]
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
    return result.filter(function(cluster) {
        var interesting = false;
        cluster.forEach(function(line){
            interesting = interesting || line.interesting
        });
        return interesting;
    })
}

app.set('views', './views');
app.set('view engine', 'jade');

app.get('/index', function(req, res) {
    res.render('index.html', {
        // enter params for view here
        title: "Twitter News Stream",
        clusters: readData()
    });
});

app.listen(3000);
