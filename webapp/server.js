var express = require('express');
 
var app = express();

var data = require('./data.json');
 
app.set('views', './views');
app.set('view engine', 'jade');

var interesting = [];
for (i = 0; i < data.all.length; i++) {
    for (y = 0; y < data.all[0].length; y++) {
        if (data.all[i][y].interesting)
            if (!(y in interesting)) interesting.push(y);
    }
}

app.get('/', function(req, res) {
    res.render('home', {
        // enter params for view here
        title: "Twitter News Stream",
        clusters: data.all,
        interesting: interesting
    });
});
  
app.listen(3000);