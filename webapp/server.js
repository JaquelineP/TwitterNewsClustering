var express = require('express');
var ejs = require('ejs')
var path = require('path')
var app = express();
var fs = require('fs');

/* use ejs template engine instead of jade */
app.set('view engine', 'html');
app.engine('html', ejs.renderFile);
app.use(express.static(path.join(__dirname, 'public')));

var path = '../SparkTwitterClustering/output/'
var times = []

function save(object) {
    fs.writeFile("test", JSON.stringify(object), function(err) {
        if(err) {
            return console.log(err);
        }

        console.log("The file was saved!");
    });
}

function getFilenames(path) {
    var filenames = fs.readdirSync(path);
    filenames = filenames.filter(function(e){
        // startwith
        return e.lastIndexOf('part-', 0) === 0
    });
    return filenames.map(function(name) {
        return path + '/' + name
    })
}

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

function readData() {

    // get filenames
    var filenames = getFilenames(path + 'merged_clusterInfo')
    var lines = []

    // for each filename
    filenames.forEach(function(filename) {
        var array = fs.readFileSync(filename).toString().split(")\n");
        if (array.length == 1) {
            console.log("empty file "+ filename)
            return;
        }
        array.forEach(function(line) {

            // remove first char (
            line = line.substring(1);
            var lineArray = line.split(",")
            if (lineArray.length < 8) return;
            var time = parseInt(lineArray[8])

            var contains = false
            var timeIndex = -1
            times.forEach(function(ea, index) {
                if (ea === time) {
                    contains = true
                    timeIndex = index
                }
            })
            if (!contains) {
                times.push(time)
                timeIndex = times.length - 1
            }

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
                'batch_time' : timeIndex
            }
            lines.push(parsedLine);
        });
    })

    // group lines by clusterid
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

        times.forEach(function(batch, index) {
            var contains = false
            existingBatches.forEach(function(ea) {
                if (ea == index) contains = true
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
                'batch_time' : index
            }
            // for this time no batch exists -> add empty batch
            empty.batch_time = index
            cluster.push(empty)
        })
        return cluster.sort(function(a, b){
            return a['batch_time'] - b['batch_time']
        })
    })
}

function readTweets() {

    // get filenames
    var filenames = getFilenames(path + 'merged_tweets')
    var lines = []
    var tweetTimes = []

    // for each filename
    filenames.forEach(function(filename) {
        var array = fs.readFileSync(filename).toString().split(")\n");
        if (array.length == 1) {
            console.log("empty file "+ filename)
            return;
        }
        array.forEach(function(line) {

            // remove first char (
            line = line.substring(1);
            var lineArray = line.split(",")
            if (lineArray.length < 4) return;
            var time = parseInt(lineArray[0])

            var contains = false
            var timeIndex = -1
            tweetTimes.forEach(function(ea, index) {
                if (ea === time) {
                    contains = true
                    timeIndex = index
                }
            })
            if (!contains) {
                tweetTimes.push(time)
                timeIndex = tweetTimes.length - 1
            }


            var parsedTweet = {
                'id': lineArray[1],
                'tweetId' :  lineArray[2],
                'batch_time' : timeIndex,
                'text' :  lineArray[3]
            }
            lines.push(parsedTweet);
        });
    })

    return groupBy(lines, function(item) {
      return [item.id];
    });

}


app.set('views', './views');
app.set('view engine', 'jade');

// add cluster table
var clusters = readData()
app.get('/index', function(req, res) {
    res.render('index.html', {
        // enter params for view here
        title: "Twitter News Stream",
        clusters: clusters,
        interesting: clusters.length
    });
});

// add detail pages foreach cluster and time
var clusterIds = clusters.map(function(cluster) {
    return cluster[0].id
})
var tweets = readTweets()
clusterIds.forEach(function(clusterId) {
    times.forEach(function(time, timeIndex) {
        app.get('/' + clusterId + '/' + timeIndex, function(req, res) {
            var c = tweets.filter(function(item) {
                return item[0].id == clusterId
            })[0]
            var t = c.filter(function(item) {
                return item.batch_time == timeIndex
            })
            res.render('clusterInfo.html', {
                tweets: t
            })
        })
    })
})

app.listen(3000);